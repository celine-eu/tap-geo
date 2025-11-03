"""Geo tap class."""

from __future__ import annotations

from singer_sdk import Tap, typing as th

from tap_geo.streams import GeoStream
from tap_geo.storage import Storage


class TapGeo(Tap):
    """Singer tap for geospatial files."""

    name = "tap-geo"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property("paths", th.ArrayType(th.StringType), required=True),
                    th.Property("table_name", th.StringType),
                    th.Property("skip_fields", th.ArrayType(th.StringType), default=[]),
                    th.Property(
                        "primary_keys", th.ArrayType(th.StringType), default=[]
                    ),
                    th.Property(
                        "geometry_format",
                        th.StringType,
                        default="wkt",
                        description="Geometry format: wkt or geojson",
                    ),
                    th.Property(
                        "expose_fields",
                        th.ArrayType(th.StringType),
                        default=[],
                        description="List of feature properties to expose as top-level columns. "
                        "All other properties will go into `features`.",
                    ),
                )
            ),
            required=True,
            description="List of file configs to parse",
        ),
    ).to_dict()

    def discover_streams(self):
        streams = []
        for file_cfg in self.config["files"]:
            all_paths = []
            for pattern in file_cfg["paths"]:
                st = Storage(pattern)
                all_paths.extend(st.glob())
            cfg = {**file_cfg, "paths": all_paths}
            streams.append(GeoStream(self, cfg))
        return streams


if __name__ == "__main__":
    TapGeo.cli()
