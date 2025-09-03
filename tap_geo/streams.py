"""GeoStream base logic for geospatial file parsing."""

from __future__ import annotations
import typing as t
from pathlib import Path
import json
import fiona
from shapely.geometry import shape, mapping
from shapely.wkt import dumps as to_wkt

from singer_sdk.streams import Stream

from .osm import OSMHandler

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap


def fiona_type_to_jsonschema(fiona_type: str) -> dict:
    """Map Fiona/GDAL field type to JSON schema."""
    ft = fiona_type.lower()
    if ft.startswith("int"):
        return {"type": ["null", "integer"]}
    if ft.startswith(("float", "real", "double", "decimal")):
        return {"type": ["null", "number"]}
    if ft.startswith(("str", "string", "varchar", "c")):
        return {"type": ["null", "string"]}
    if ft.startswith("date") and not ft.startswith("datetime"):
        return {"type": ["null", "string"], "format": "date"}
    if ft.startswith("datetime"):
        return {"type": ["null", "string"], "format": "date-time"}
    if ft.startswith("time"):
        return {"type": ["null", "string"], "format": "time"}
    if ft.startswith(("bool", "boolean", "logical")):
        return {"type": ["null", "boolean"]}
    if ft.startswith(("binary", "blob", "bytes")):
        return {"type": ["null", "string"], "contentEncoding": "base64"}
    if ft.startswith(("json", "object")):
        return {"type": ["null", "object"]}
    return {"type": ["null", "string"]}


class GeoStream(Stream):
    """Stream for geospatial files (SHP, GeoJSON, OSM, GPX, etc.)."""

    def __init__(self, tap: Tap, file_cfg: dict) -> None:
        self.file_cfg = file_cfg
        self.filepath = Path(file_cfg["path"])
        table_name = file_cfg.get("table_name") or self.filepath.stem
        self.primary_keys = file_cfg.get("primary_keys", [])
        self.tap = tap
        super().__init__(tap, name=table_name)

    @property
    def schema(self) -> dict:
        """Build schema from file metadata."""
        suffix = self.filepath.suffix.lower()

        if suffix in (".osm", ".pbf"):
            return {
                "type": "object",
                "properties": {
                    "id": {"type": ["string"]},
                    "type": {"type": ["string"]},  # node, way, relation
                    "geometry": {"type": ["null", "string", "object"]},
                    "tags": {"type": ["null", "string"]},
                    "members": {"type": ["null", "array"]},
                    "metadata": {"type": ["null", "object"]},
                },
            }

        try:
            with fiona.open(self.filepath) as src:
                props = {
                    k: fiona_type_to_jsonschema(v)
                    for k, v in src.schema["properties"].items()
                }
        except Exception as e:
            self.logger.error("Failed to open %s with Fiona: %s", self.filepath, e)
            raise

        props["geometry"] = {"type": ["null", "string", "object"]}
        props["metadata"] = {"type": ["null", "object"]}
        return {"type": "object", "properties": props}

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Route file parsing depending on extension."""
        skip_fields = set(self.tap.config.get("skip_fields", []))
        geom_fmt = self.tap.config.get("geometry_format", "wkt")
        suffix = self.filepath.suffix.lower()

        try:
            if suffix in (".osm", ".pbf"):
                yield from self._parse_osm(geom_fmt)
            else:
                yield from self._parse_with_fiona(skip_fields, geom_fmt)
        except Exception as e:
            self.logger.exception("Failed parsing file %s: %s", self.filepath, e)
            raise

    def _parse_with_fiona(
        self, skip_fields: set[str], geom_fmt: str
    ) -> t.Iterable[dict]:
        """Parse SHP, GeoJSON, GPKG via Fiona."""
        try:
            with fiona.open(self.filepath) as src:
                crs = src.crs_wkt or src.crs
                driver = src.driver
                for i, feat in enumerate(src, start=1):
                    try:
                        props = {
                            k: v
                            for k, v in feat["properties"].items()
                            if k not in skip_fields
                        }
                        geom = None
                        if feat.get("geometry"):
                            if geom_fmt == "wkt":
                                geom = to_wkt(shape(feat["geometry"]))
                            elif geom_fmt == "geojson":
                                geom = mapping(shape(feat["geometry"]))
                        yield {
                            **props,
                            "geometry": geom,
                            "metadata": {
                                "source": str(self.filepath),
                                "driver": driver,
                                "crs": crs,
                            },
                        }
                    except Exception as fe:
                        self.logger.warning(
                            "Failed to parse feature %d in %s: %s",
                            i,
                            self.filepath,
                            fe,
                        )
        except Exception as e:
            self.logger.error("Could not open dataset %s: %s", self.filepath, e)
            raise

    def _parse_osm(self, geom_fmt: str) -> t.Iterable[dict]:
        """Parse OSM XML/PBF using pyosmium."""
        try:
            handler = OSMHandler(geom_fmt)
            handler.apply_file(str(self.filepath))
            for rec in handler.records:
                yield {
                    **rec,
                    "tags": json.dumps(rec.get("tags", {})),  # force to string
                    "metadata": {
                        "source": str(self.filepath),
                    },
                }
        except Exception as e:
            self.logger.error("OSM parsing failed for %s: %s", self.filepath, e)
            raise
