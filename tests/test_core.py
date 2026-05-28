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
        "test.parquet",
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


class TestBboxFiltering:
    """Tests for bounding box spatial filtering."""

    def test_bbox_parquet_filters_records(self):
        """Bbox on GeoParquet should only return features inside the box."""
        # Inner bbox covers 5 of 10 points in test.parquet
        inner_bbox = [11.10, 46.05, 11.15, 46.09]
        cfg = {"paths": [os.path.join(BASE, "test.parquet")], "bbox": inner_bbox}
        tap = TapGeo(config={"files": [cfg]})
        stream = GeoStream(tap, cfg)
        filtered = list(stream.get_records(context=None))

        cfg_all = {"paths": [os.path.join(BASE, "test.parquet")]}
        tap_all = TapGeo(config={"files": [cfg_all]})
        stream_all = GeoStream(tap_all, cfg_all)
        all_records = list(stream_all.get_records(context=None))

        assert len(all_records) == 10
        assert len(filtered) == 5
        assert len(filtered) < len(all_records)

    def test_bbox_geojson_filters_records(self):
        """Bbox on GeoJSON should post-filter via shapely intersects."""
        # test.geojson has 18 features around lon 11.45-11.49, lat 46.04-46.06
        # Use a bbox that covers roughly half the area
        partial_bbox = [11.45, 46.04, 11.47, 46.06]
        cfg = {"paths": [os.path.join(BASE, "test.geojson")], "bbox": partial_bbox}
        tap = TapGeo(config={"files": [cfg]})
        stream = GeoStream(tap, cfg)
        filtered = list(stream.get_records(context=None))

        cfg_all = {"paths": [os.path.join(BASE, "test.geojson")]}
        tap_all = TapGeo(config={"files": [cfg_all]})
        stream_all = GeoStream(tap_all, cfg_all)
        all_records = list(stream_all.get_records(context=None))

        assert 0 < len(filtered) < len(all_records)

    def test_bbox_no_filter_returns_all(self):
        """Without bbox, all records are returned."""
        cfg = {"paths": [os.path.join(BASE, "test.parquet")]}
        tap = TapGeo(config={"files": [cfg]})
        stream = GeoStream(tap, cfg)
        records = list(stream.get_records(context=None))
        assert len(records) == 10

    def test_bbox_validation_wrong_length(self):
        """Bbox with wrong number of elements should raise ValueError."""
        cfg = {"paths": [os.path.join(BASE, "test.parquet")], "bbox": [1.0, 2.0]}
        with pytest.raises(ValueError, match="4 numbers"):
            TapGeo(config={"files": [cfg]})

    def test_bbox_validation_not_a_list(self):
        """Bbox that is not a list/array should fail config validation."""
        cfg = {"paths": [os.path.join(BASE, "test.parquet")], "bbox": "invalid"}
        with pytest.raises(Exception):
            TapGeo(config={"files": [cfg]})

    def test_parquet_record_structure(self):
        """GeoParquet records should have standard geometry/features/metadata shape."""
        cfg = {
            "paths": [os.path.join(BASE, "test.parquet")],
            "expose_fields": ["name"],
        }
        tap = TapGeo(config={"files": [cfg]})
        stream = GeoStream(tap, cfg)
        records = list(stream.get_records(context=None))

        for rec in records:
            assert "geometry" in rec
            assert "features" in rec
            assert "metadata" in rec
            assert rec["metadata"]["driver"] == "geoparquet"
            # 'name' should be exposed at top level
            assert "name" in rec
            # 'name' should NOT be in features
            assert "name" not in rec["features"]
