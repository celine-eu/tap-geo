import os
import pytest
from tap_geo.tap import TapGeo
from tap_geo.streams import GeoStream

BASE = "data"


@pytest.mark.parametrize(
    "filename",
    [
        "stazioni.shp",  # shapefile (requires .dbf/.shx/.prj alongside)
        "test.geojson",
        "test.osm",
    ],
)
def test_geo_stream_schema_and_records(filename):
    """Ensure schema can be built and at least one record is parsed."""
    filepath = os.path.join(BASE, filename)

    # Config must pass a list of paths
    cfg = {"paths": [filepath]}
    tap = TapGeo(config={"files": [cfg]})

    stream = GeoStream(tap, cfg)

    # Schema should be a dict with properties
    schema = stream.schema
    assert "properties" in schema

    # Collect some records
    records = list(stream.get_records(context=None))
    assert isinstance(records, list)
    assert len(records) > 0

    # All records must have geometry + metadata + features
    for rec in records:
        assert "geometry" in rec
        assert "metadata" in rec
        assert "features" in rec


def test_tapgeo_discovers_streams():
    """Ensure TapGeo discovers all configured file paths."""
    files = [
        {"paths": [os.path.join(BASE, "stazioni.shp")]},
        {"paths": [os.path.join(BASE, "test.geojson")]},
        {"paths": [os.path.join(BASE, "test.osm")]},
    ]
    tap = TapGeo(config={"files": files})
    streams = tap.discover_streams()

    # One stream per config entry
    assert len(streams) == len(files)

    for stream in streams:
        assert isinstance(stream, GeoStream)
        # Ensure schema and at least one record per stream
        schema = stream.schema
        assert "properties" in schema
        recs = list(stream.get_records(context=None))
        assert len(recs) > 0
