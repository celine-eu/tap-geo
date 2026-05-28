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
                    th.Property(
                        "bbox",
                        th.ArrayType(th.NumberType),
                        default=None,
                        description="Bounding box filter [west, south, east, north].",
                    ),
                    th.Property(
                        "download_workers",
                        th.IntegerType,
                        default=4,
                        description="Parallel download threads for remote parquet files.",
                    ),
                )
            ),
            required=True,
            description="List of file configs to parse",
        ),
    ).to_dict()

    @staticmethod
    def _is_parquet_glob(pattern: str) -> bool:
        """Check if a pattern is a directory/glob that should be kept unexpanded for DuckDB."""
        return pattern.endswith("/*") or pattern.endswith("/**")

    def discover_streams(self):
        streams = []
        for file_cfg in self.config["files"]:
            all_paths = []
            for pattern in file_cfg["paths"]:
                if self._is_parquet_glob(pattern):
                    all_paths.append(pattern)
                else:
                    st = Storage(pattern)
                    all_paths.extend(st.glob())
            cfg = {**file_cfg, "paths": all_paths}
            streams.append(GeoStream(self, cfg))
        return streams


if __name__ == "__main__":
    TapGeo.cli()
