from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Set, Callable

import numpy as np
import pandas as pd


class ExprError(ValueError):
    """Raised when an expression is invalid or uses disallowed syntax."""


_ALLOWED_FUNCS: Dict[str, Callable[[Any], Any]] = {
    "abs": np.abs,
}


_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Compare, ast.Call,
    ast.Name, ast.Load, ast.Constant,
    ast.And, ast.Or, ast.Not,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow,
    ast.UAdd, ast.USub,
    ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Eq, ast.NotEq,
)


_ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow)
_ALLOWED_UNARYOPS = (ast.UAdd, ast.USub, ast.Not)
_ALLOWED_BOOLOPS = (ast.And, ast.Or)
_ALLOWED_CMPOPS = (ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Eq, ast.NotEq)


@dataclass(frozen=True)
class CompiledExpr:
    expr: str
    ast_tree: ast.AST
    names: Set[str]


def _collect_names(node: ast.AST) -> Set[str]:
    names: Set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            names.add(n.id)
    return names


def compile_expr(expr: str) -> CompiledExpr:
    # Parse + validate an expression; return AST + referenced column names.
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ExprError(f"Invalid expression syntax: {e}") from e

    _validate_ast(tree)
    names = _collect_names(tree)
    # keep only column names(exclude allowed function names)
    names = {n for n in names if n not in _ALLOWED_FUNCS}

    return CompiledExpr(expr=expr, ast_tree=tree, names=names)


def _validate_ast(node: ast.AST) -> None:
    for n in ast.walk(node):
        if not isinstance(n, _ALLOWED_NODES):
            raise ExprError(f"Disallowed syntax: {type(n).__name__}")

        if isinstance(n, ast.BinOp) and not isinstance(n.op, _ALLOWED_BINOPS):
            raise ExprError(f"Disallowed operator: {type(n.op).__name__}")

        if isinstance(n, ast.UnaryOp) and not isinstance(n.op, _ALLOWED_UNARYOPS):
            raise ExprError(f"Disallowed unary operator: {type(n.op).__name__}")

        if isinstance(n, ast.BoolOp) and not isinstance(n.op, _ALLOWED_BOOLOPS):
            raise ExprError(f"Disallowed boolean operator: {type(n.op).__name__}")

        if isinstance(n, ast.Compare):
            for op in n.ops:
                if not isinstance(op, _ALLOWED_CMPOPS):
                    raise ExprError(f"Disallowed comparison operator: {type(op).__name__}")

        if isinstance(n, ast.Call):
            if not isinstance(n.func, ast.Name):
                raise ExprError("Only simple function calls like abs(x) are allowed.")
            func_name = n.func.id
            if func_name not in _ALLOWED_FUNCS:
                raise ExprError(f"Function '{func_name}' is not allowed. Allowed: {list(_ALLOWED_FUNCS)}")
            if len(n.keywords) != 0:
                raise ExprError("Keyword arguments are not allowed in function calls.")
            if len(n.args) != 1:
                raise ExprError(f"Function '{func_name}' must take exactly 1 argument.")


def eval_expr(compiled: CompiledExpr, df: pd.DataFrame) -> pd.Series:
    # Evaluate expression on df -> boolean mask (Series).
    missing = [c for c in compiled.names if c not in df.columns]
    if missing:
        raise ExprError(f"Expression references missing columns: {missing}")

    env: Dict[str, Any] = {}
    # expose columns
    for c in compiled.names:
        env[c] = df[c]
    # expose allowed functions
    env.update(_ALLOWED_FUNCS)

    result = _eval_node(compiled.ast_tree.body, env)

    # Normalize to boolean Series
    if isinstance(result, (pd.Series, np.ndarray)):
        # comparisons / boolean ops should yield bool; but users might write numeric expr by mistake
        if getattr(result, "dtype", None) is not None and result.dtype != bool:
            raise ExprError("Expression must evaluate to a boolean mask (use comparisons like <, ==, etc.).")
        return pd.Series(result, index=df.index, dtype=bool)

    if isinstance(result, (bool, np.bool_)):
        # broadcast scalar boolean
        return pd.Series([bool(result)] * len(df), index=df.index, dtype=bool)

    raise ExprError("Expression must evaluate to a boolean mask.")


def _eval_node(node: ast.AST, env: Mapping[str, Any]) -> Any:
    # Constants
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float, bool)) or node.value is None:
            return node.value
        raise ExprError(f"Only numeric/bool constants are allowed, got: {type(node.value).__name__}")

    # Variables
    if isinstance(node, ast.Name):
        if node.id not in env:
            raise ExprError(f"Unknown name: {node.id}")
        return env[node.id]

    # Unary ops
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, env)
        if isinstance(node.op, ast.Not):
            return ~operand
        if isinstance(node.op, ast.UAdd):
            return +operand
        if isinstance(node.op, ast.USub):
            return -operand

    # Binary ops
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, env)
        right = _eval_node(node.right, env)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.Pow):
            return left ** right

    # Bool ops
    if isinstance(node, ast.BoolOp):
        values = [_eval_node(v, env) for v in node.values]
        if isinstance(node.op, ast.And):
            out = values[0]
            for v in values[1:]:
                out = out & v
            return out
        if isinstance(node.op, ast.Or):
            out = values[0]
            for v in values[1:]:
                out = out | v
            return out

    # Comparisons (chain supported: a < b < c)
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, env)
        out = None
        for op, comp in zip(node.ops, node.comparators):
            right = _eval_node(comp, env)

            if isinstance(op, ast.Lt):
                cur = left < right
            elif isinstance(op, ast.LtE):
                cur = left <= right
            elif isinstance(op, ast.Gt):
                cur = left > right
            elif isinstance(op, ast.GtE):
                cur = left >= right
            elif isinstance(op, ast.Eq):
                cur = left == right
            elif isinstance(op, ast.NotEq):
                cur = left != right
            else:
                raise ExprError(f"Unsupported comparison operator: {type(op).__name__}")

            out = cur if out is None else (out & cur)
            left = right
        return out

    # Function calls
    if isinstance(node, ast.Call):
        func_name = node.func.id  # validated already
        func = env.get(func_name, None)
        if func is None:
            raise ExprError(f"Unknown function: {func_name}")
        arg = _eval_node(node.args[0], env)
        return func(arg)

    raise ExprError(f"Unsupported expression element: {type(node).__name__}")
