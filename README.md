# atl08kit — A Python Toolkit for ICESat-2 ATL08 Point Data

`atl08kit` is a lightweight Python library for geospatial analysis and
quality control of ICESat-2 ATL08 point data.

This project was developed as part of the **Geospatial Programming** course
and focuses on building a **well-structured, testable, and reusable Python library**
rather than a monolithic application or a collection of scripts.

The toolkit supports:

- Flexible quality control of ATL08 point tables using a safe, expression-based DSL
- Strong-beam selection based on satellite orientation (`sc_orient`)
- An end-to-end processing pipeline accessible via both a Python API and a CLI
- Reproducible geospatial data processing with comprehensive automated tests

---



## Motivation

atl08kit is motivated by the way **ICESat-2 ATL08 data** are commonly used in real geospatial research workflows.

ATL08 is a land and vegetation height product derived from spaceborne LiDAR observations and is widely used for terrain analysis, vegetation studies, and multi-source elevation comparison.
In practice, ATL08 data are almost always processed as point tables and require strict and repeatable quality control.

Typical workflows repeatedly involve:
- Quality filtering based on elevation differences, terrain flags, and cloud indicators
- Strong- and weak-beam selection using satellite orientation information (sc_orient)
- Spatial clipping to areas of interest or masking with raster datasets
- Iterative adjustment of filtering rules across experiments

However, these operations are often implemented as ad-hoc scripts with limited reusability and poor testability.

For this reason, atl08kit does not aim to implement a full ICESat-2 processing chain.
Instead, it focuses on the **most common and error-prone steps** in ATL08 point data analysis and abstracts them into a modular, composable, and testable Python library.
    

---

## Installation

We recommend using a dedicated conda environment:

```bash
conda create -n atl08kit python=3.10 -y
conda activate atl08kit
```
Install the package in editable mode:
```bash
pip install -e .
```

Verify installation:
```bash
atl08kit version
```



---

## Quick Start (Command Line Interface)

This section demonstrates a typical end-to-end workflow using `atl08kit`,
starting from raw ATL08 HDF5 files and producing analysis-ready geospatial outputs.

### Step 1 — Extract point tables from ATL08 HDF5

ATL08 data are distributed as HDF5 files.
The `extract` command converts ATL08 `land_segments` into a flat CSV point table
that can be processed by all other modules in `atl08kit`.

```bash
atl08kit extract \
  --h5 ATL08_20220102113835_01711406_007_01.h5 \
  --out points_raw.csv \
  --summary
```

This command:
- Reads ATL08 land_segments from the input HDF5 file
- Extracts all 1D datasets aligned with latitude and longitude
- Skips non-aligned or multi-dimensional datasets
- Produces a CSV file compatible with downstream processing steps


### Step 2 — Quality filtering using expressions

Points can be filtered using a safe, expression-based DSL.

```bash
atl08kit filter \
  --in points_raw.csv \
  --out points_qc.csv \
  --expr "abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3" \
  --summary
```

This command:
- Evaluates the expression on each row of the point table
- Keeps only points that satisfy the specified conditions
- Writes the filtered table to a new CSV file



### Step3 - Select strong-beam points

```bash
atl08kit beams \
  --in points.csv \
  --out points_strong.csv \
  --h5-folder /path/to/ATL08_h5_files \
  --summary
```

This command:
- Reads ATL08 HDF5 files from the provided folder
- Determines strong beams based on /orbit_info/sc_orient
- Keeps only rows corresponding to strong beams



### Step 4 — Spatial filtering using a vector mask (AOI)

Filtered points can be clipped to an area of interest using a vector polygon.

```bash
atl08kit vector \
  --in points_qc.csv \
  --out points_aoi.csv \
  --mask boundary/2022.shp \
  --mode clip \
  --summary
```

This command:
- Applies a spatial clip using the provided polygon mask
- Keeps only points inside the area of interest

### Step 5 — Export to geospatial vector formats

Point tables can be exported to common GIS formats.

```bash
atl08kit export \
  --in points_aoi.csv \
  --out points_aoi.geojson \
  --summary
```
Supported output formats:
- GeoJSON (.geojson)
- GeoPackage (.gpkg)
- ESRI Shapefile (.shp)

All attribute columns are preserved in the output.

### (Optional) Run a combined processing pipeline

For convenience, the same steps can be combined using the run command.
```bash
atl08kit run \
  --in points_raw.csv \
  --out points_result.csv \
  --h5-folder /path/to/ATL08_h5_files \
  --expr "abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3" \
  --summary
```
  
The pipeline applies:
- Optional strong-beam selection
- Optional expression-based filtering
- A single consolidated output



---
## Core Concepts

atl08kit is designed around a small number of explicit concepts:

- **Point Table**  
  A flat CSV table where each row represents an ATL08 land segment.

- **Quality Control (QC)**  
  Boolean filtering rules applied to point attributes.

- **Beam Semantics**  
  Explicit handling of strong and weak beams based on satellite orientation.

- **Spatial Context**  
  Integration of vector and raster masks to restrict analysis regions.
---



## Command Reference


This section documents the core commands provided by `atl08kit`.
Each command is designed to perform a single, well-defined operation and can be
used independently or composed into a larger workflow.


### `extract` — Convert ATL08 HDF5 to point tables

The `extract` command converts ICESat-2 ATL08 HDF5 files into flat CSV point tables
based on the `land_segments` group.

```bash
atl08kit extract \
  --h5 ATL08_20220102113835_01711406_007_01.h5 \
  --out points_raw.csv \
  --summary
```
#### Behavior
-   Reads all available beams (gt1l–gt3r) by default
-    Traverses the land_segments group of each beam
-    Extracts all 1D datasets aligned with latitude/longitude
-    Automatically skips:
     - Multi-dimensional datasets
     - Datasets whose length does not match the number of land segments
-    Flattens nested dataset paths into column names(e.g. terrain/h_te_best_fit → terrain_h_te_best_fit)

#### Always included columns
-    lon, lat — point coordinates
-    beam — beam identifier (gt1l, gt2r, etc.)
-    source_file — ATL08 file stem for traceability

#### Notes

The output CSV is the standard interchange format used by all downstream
commands in atl08kit.

### `filter` — Expression-based quality control

The filter command applies boolean quality-control rules to a point table
using a safe, expression-based DSL.
```bash
atl08kit filter \
  --in points_raw.csv \
  --out points_qc.csv \
  --expr "abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3" \
  --summary
```

#### Expression language

#### Supported elements include:
-    Arithmetic operators: +  -  *  /  **
-    Comparison operators: <  <=  >  >=  ==  !=
-    Boolean operators: and, or, not
-    Built-in functions: abs(x)

Expressions are parsed using Python’s AST and validated against a strict
whitelist to prevent unsafe execution.

#### Examples
-    abs(dem_h - terrain_h_te_best_fit) <= 3
-    terrain_flg == 0 and cloud_flag_atm < 3
-    abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3

#### Notes
-    All referenced column names must exist in the input table
-    The expression must evaluate to a boolean mask
    
### `beams` — Strong-beam selection

The beams command filters a point table to retain only strong-beam
measurements based on satellite orientation (sc_orient).
```bash
atl08kit beams \
  --in points.csv \
  --out points_strong.csv \
  --h5-folder /path/to/ATL08_h5_files \
  --summary
```
Requirements

The input CSV must contain:
-    beam — beam identifier
-    source_file — ATL08 filename or stem

Behavior
-    Reads sc_orient from each ATL08 HDF5 file
-    Determines which beams are strong for each orbit
-    Keeps only rows corresponding to strong beams

This command makes beam semantics explicit and avoids hard-coded assumptions
in analysis scripts.


### `vector` — Spatial filtering with vector masks

The vector command filters points using polygon masks (AOIs).

```bash
atl08kit vector \
  --in points_qc.csv \
  --out points_aoi.csv \
  --mask boundary/2022.shp \
  --mode clip \
  --summary
```
#### Modes
-    clip — keep points inside the polygon
-    exclude — keep points outside the polygon

Spatial predicates
-    within (default)
-    intersects

#### Monthly AOI support

For time-dependent AOIs (e.g. monthly boundaries), vector supports
automatic mask resolution based on the ATL08 filename:

```bash
atl08kit vector \
  --in points.csv \
  --out points_aoi.csv \
  --monthly-folder boundaries/ \
  --source-file ATL08_20220102113835_01711406_007_01 \
  --ext .shp
```
In this mode, the AOI file is inferred from the year-month encoded in
the ATL08 filename.

### `raster` — Raster-based masking

The raster command filters points by sampling values from a raster dataset.

```bash
atl08kit raster \
  --in points.csv \
  --out points_masked.csv \
  --raster landcover.tif \
  --keep 1 2 3 \
  --summary
```
Capabilities
-    Keep or drop points based on raster values
-    Select raster band
-    Optionally keep or discard nodata samples

This is commonly used for land-cover masks, DEM validity layers,
or classification rasters.

### `export` — Export to geospatial vector formats

The export command converts a CSV point table into standard GIS vector formats

```bash
atl08kit export \
  --in points_aoi.csv \
  --out points_aoi.geojson \
  --summary
  ```
Supported formats
-    GeoJSON (.geojson)
-    GeoPackage (.gpkg)
-    ESRI Shapefile (.shp)

All attribute columns are preserved in the output.
Export requires geopandas.

---

## Advanced Usage — Batch Processing

In practical workflows, ATL08 point tables are often generated and processed
in large numbers (e.g. per orbit, per tile, or per month).

The `batch` command applies the same processing logic to **all CSV files in a directory**
and produces both per-file outputs and an aggregated summary table.

Typical use cases include:
- Applying identical QC rules to multiple ATL08 tiles
- Running consistent experiments across different regions or time periods
- Collecting pass rates and statistics for quality assessment

```bash
atl08kit batch \
  --in-dir points_raw/ \
  --out-dir points_qc/ \
  --expr "abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3" \
  --summary batch_summary.csv
```
This command:
- Iterates over all CSV files in the input directory
- Applies the specified expression-based quality control
- Writes filtered CSV files to the output directory
- Generates a summary CSV reporting input size, output size, and pass rate for each file

## Python API Example

All core functionality in atl08kit is also available programmatically.
```python
import pandas as pd
from atl08kit.filters import filter_by_expr

df = pd.read_csv("points_raw.csv")

filtered, summary = filter_by_expr(
    df,
    "abs(dem_h - terrain_h_te_best_fit) <= 3 and cloud_flag_atm < 3"
)

print(summary)
filtered.to_csv("points_qc.csv", index=False)
```
The Python API mirrors the CLI behavior and is designed for reproducible
research workflows and integration into larger analysis pipelines.

---

## Testing

Automated tests are implemented using pytest.

Run the full test suite:
```bash
pytest
```
The tests include:
-    Unit tests for expression parsing and filtering
-    Unit tests for strong-beam logic
-    CLI tests executed via subprocess
-    Pipeline and batch integration tests

Synthetic test data are generated inside the test suite to ensure
reproducibility and independence from large external datasets.

---
## Project Structure
```bash
src/atl08kit/
  expr.py        # safe expression parsing and validation
  filters.py     # expression-based filtering logic
  beams.py       # strong-beam detection using sc_orient
  raster.py      # raster-based masking utilities
  vector_mask.py # vector-based masking utilities
  pipeline.py    # end-to-end processing pipeline
  cli.py         # command-line interface
  atl08.py       # HDF5 (ATL08) land_segments extraction utilities

tests/
  test_filters.py
  test_beams.py
  test_cli.py
  test_pipeline_cli.py
  ...
```

---
## Module Structure
The following table summarizes the core modules of atl08kit, their responsibilities, main functionalities, and typical usage scenarios.

| Module file | Responsibility | Description |
|------------|----------------|-------------|
| `atl08.py` | ATL08 data reading and parsing | Extracts `land_segments` point data from ATL08 HDF5 files and automatically discovers all one-dimensional datasets aligned with latitude and longitude, producing analysis-ready CSV tables. |
| `io.py` | Generic table I/O | Provides unified utilities for reading and writing CSV point tables, ensuring consistent data handling across modules. |
| `expr.py` | Safe expression DSL | Parses and validates user-defined expressions using Python AST, allowing only a restricted set of operators and functions (e.g. `abs`) to ensure safe and deterministic evaluation. |
| `filters.py` | Expression-based filtering | Applies validated expressions to point tables, generating boolean masks and returning filtered results together with summary statistics. |
| `beams.py` | Strong / weak beam logic | Identifies strong and weak beams based on ATL08 `sc_orient` metadata and filters point tables accordingly. |
| `vector_mask.py` | Vector-based spatial masking | Performs spatial filtering of points using vector polygon masks (AOIs), supporting clipping and exclusion operations. |
| `raster.py` | Raster-based spatial masking | Samples raster values at point locations and keeps or discards points based on raster-based constraints. |
| `pipeline.py` | Processing pipeline orchestration | Combines strong-beam selection and expression-based filtering into a reproducible single-file processing workflow. |
| `batch.py` | Batch processing engine | Applies a unified processing pipeline to multiple CSV files within a directory and produces aggregated summaries. |
| `export.py` | Spatial data export | Converts CSV point tables into standard GIS vector formats such as GeoJSON, GeoPackage, and Shapefile. |
| `cli.py` | Command-line interface | Exposes all core functionalities through a unified and user-facing command-line interface. |
| `water.py` | Temporal helper utilities | Provides lightweight helper functions for extracting year–month (`YYYYMM`) information from ATL08 filenames, supporting monthly vector-mask and batch-processing workflows. |

---

## Test Suite Structure
The tests/ directory contains unit tests, CLI tests, and integration tests
covering all core functionalities of atl08kit.
The test suite is designed to validate both individual modules and complete
end-to-end workflows.

| Test file | Test level | Description |
|----------|------------|-------------|
| `test_atl08_fields.py` | Unit | Tests field discovery from ATL08 HDF5 files, ensuring correct enumeration of available `land_segments` datasets for a given beam. |
| `test_batch_cli.py` | Integration / CLI | Tests the batch-processing command, including directory traversal, parameter forwarding, and summary table generation. |
| `test_beams.py` | Unit | Tests strong-beam identification logic based on `sc_orient`, validating correct beam classification under different satellite orientations. |
| `test_cli.py` | CLI | Tests general CLI behavior, including command registration, argument parsing, and error handling. |
| `test_export.py` | Unit | Tests CSV-to-vector export logic, including `GeoDataFrame` creation and driver selection. |
| `test_export_cli.py` | CLI | End-to-end test of the export command via subprocess, verifying output file creation and non-empty results. |
| `test_extract_cli.py` | CLI | Tests the extract command using a synthetic ATL08 HDF5 fixture, validating aligned-field extraction and skipped multi-dimensional datasets. |
| `test_filters.py` | Unit | Tests expression-based filtering logic, including valid expressions, invalid syntax, missing columns, and boolean mask correctness. |
| `test_io.py` | Unit | Tests low-level I/O utilities for reading and writing CSV tables, including format validation and error handling. |
| `test_pipeline_cli.py` | CLI / Integration | Tests the run command, validating correct orchestration of optional strong-beam filtering and expression-based QC. |
| `test_raster.py` | Unit | Tests raster sampling and masking logic, including value-based keep/drop rules and `nodata` handling. |
| `test_raster_cli.py` | CLI | End-to-end tests for the raster command, ensuring correct interaction between CSV input and raster masks. |
| `test_vector_mask.py` | Unit | Tests vector-based spatial masking logic, including geometric predicates such as `within` and `intersects`. |
| `test_vector_cli.py` | CLI | Tests the vector command for AOI-based spatial filtering using polygon masks. |
| `test_vector_monthly_cli.py` | CLI | Tests monthly AOI resolution logic, where vector masks are automatically selected based on year–month information in ATL08 filenames. |

---




## Notes

This project was developed by a group of up to three students as required by the course.
Version control was managed using Git, and the project emphasizes clarity, modularity,
and testability over feature completeness.