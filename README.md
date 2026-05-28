# tap-geo

`tap-geo` is a Singer tap for Geospatial datasets.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.


## Capabilities

- `catalog`
- `state`
- `discover`
- `activate-version`
- `about`
- `stream-maps`
- `schema-flattening`
- `batch`

## Supported Python Versions

- 3.10
- 3.11
- 3.12
- 3.13
- 3.14

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| files | True | None | List of file configs to parse |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_maps.__else__ | False | None | Currently, only setting this to `__NULL__` is supported. This will remove all other streams. |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| faker_config | False | None | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an additional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False | None | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False | None | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False | None | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |
| batch_config | False | None | Configuration for BATCH message capabilities. |
| batch_config.encoding | False | None | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False | None | Format to use for batch files. |
| batch_config.encoding.compression | False | None | Compression format to use for batch files. |
| batch_config.storage | False | None | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False | None | Root path to use when writing batch files. |
| batch_config.storage.prefix | False | None | Prefix to use when writing batch files. |

A full list of supported settings and capabilities is available by running: `tap-geo --about`

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/celine-eu/tap-geo.git@main
```

## Configuration

### Accepted Config Options

See also [meltano.yml](./meltano.yml) for a working configuration

Provide a list of `files` with those fields

`paths` list of files in glob format, required
`table_name` name of the destination table, default to filename
`primary_keys` list of columns to use as primary keys
`geometry_format` store geospatial information in "wkt" (default) or "geojson"
`bbox` optional bounding box filter `[west, south, east, north]`

#### Example config

```yaml
config:
  files:
    - paths:
        - "data/osm/*.osm"
        - "data/osm/**/*.pbf"
      table_name: osm_data
      primary_keys: ["id"]
      geometry_format: "wkt"

    - paths:
        - "data/shapes/**/*.shp"
      table_name: shapes
      skip_fields: ["temp_field"]
      expose_fields: ["col_name", "col_2"]
      geometry_format: "geojson"

    - paths:
        - "data/buildings.geojson"
      table_name: buildings
      primary_keys: ["building_id"]

    # e.g. use docker compose up to test locally
    - paths:
        - "s3://local-data/buildings.geojson"
      table_name: buildings
      primary_keys: ["building_id"]

    # GeoParquet with bbox filter (e.g. Overture Maps buildings)
    - paths:
        - "s3://overturemaps-us-west-2/release/2026-05-20.0/theme=buildings/type=building/*"
      table_name: overture_buildings
      bbox: [-117.06, 32.58, -117.04, 32.59]
      geometry_format: wkt
```

### Supported Formats

| Format | Extensions | Bbox Behavior |
|:-------|:-----------|:-------------|
| Shapefile | `.shp` | Post-parse spatial filter |
| GeoJSON | `.geojson`, `.json` | Post-parse spatial filter |
| GPX | `.gpx` | Post-parse spatial filter |
| OSM/PBF | `.osm`, `.pbf` | Post-parse spatial filter |
| GeoPackage | `.gpkg` | Post-parse spatial filter |
| GeoParquet | `.parquet`, `.geoparquet` | Predicate pushdown (row-group pruning) |

### Bounding Box Filter

The `bbox` parameter accepts `[west, south, east, north]` coordinates. For GeoParquet files, the filter is applied as predicate pushdown — only row groups intersecting the bbox are downloaded. For all other formats, records are filtered post-parse via geometry intersection.

### S3 Storage

To use an S3-based storage, provide these environment variables:

- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` access key/secret pair
- `S3_ENDPOINT_URL` Custom S3 endpoint such as minio or compatible interface

For public S3 buckets (e.g. Overture Maps), no credentials are needed — anonymous access is used automatically when the environment variables above are not set.

Example:

`S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_ENDPOINT_URL=http://localhost:19000 meltano run tap-geo target-jsonl`


### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-geo` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-geo --version
tap-geo --help
tap-geo --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-geo` CLI interface directly using `uv run`:

```bash
uv run tap-geo --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-geo
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-geo --version

# OR run a test ELT pipeline:
meltano run tap-geo target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
