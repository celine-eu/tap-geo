"""GeoStream base logic for geospatial file parsing."""

from __future__ import annotations
import typing as t
from pathlib import Path
import fiona
from shapely.geometry import shape, mapping
from shapely.wkt import dumps as to_wkt

from singer_sdk.streams import Stream

from .osm import OSMHandler

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap


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
                    "type": {"type": ["string"]},
                    "geometry": {"type": ["null", "string", "object"]},
                    "properties": {"type": ["null", "object"]},
                    "members": {"type": ["null", "array"]},
                    "metadata": {"type": ["null", "object"]},
                },
            }

        try:
            with fiona.open(self.filepath) as src:
                pass
        except Exception as e:
            self.logger.error("Failed to open %s with Fiona: %s", self.filepath, e)
            raise

        return {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "geometry": {"type": ["null", "string", "object"]},
                "properties": {"type": ["null", "string", "object"]},
                "metadata": {"type": ["null", "object"]},
            },
        }

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
        """Parse SHP, GeoJSON, GPKG via Fiona, pack props into metadata."""
        try:
            with fiona.open(self.filepath) as src:
                crs = src.crs_wkt or src.crs
                driver = src.driver
                for i, feat in enumerate(src, start=1):
                    try:
                        # everything in properties → metadata
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

                        id_value = props.get("id", props.get("@id", None))

                        # fallback: composite key from configured primary keys

                        if self.file_cfg.get("primary_keys", []):
                            try:
                                id_value = "_".join(
                                    str(props.get(k))
                                    for k in self.file_cfg.get("primary_keys", [])
                                    if props.get(k) is not None
                                )
                            except Exception as e:
                                self.logger.warning(
                                    "Failed to build ID from primary keys: %s", e
                                )
                                id_value = None

                        yield {
                            "id": id_value,
                            "geometry": geom,
                            "properties": props,
                            "metadata": {
                                "source": str(self.filepath),
                                "driver": driver,
                                "crs": crs,
                            },
                        }
                    except Exception as fe:
                        self.logger.warning(
                            "Failed to parse feature %d in %s: %s", i, self.filepath, fe
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
                # move tags & members into metadata
                metadata = {
                    "source": str(self.filepath),
                    "members": rec.pop("members", None),
                }
                yield {
                    "id": rec.get("id") or rec.get("@id"),
                    "type": rec.get("type"),
                    "geometry": rec.get("geometry"),
                    "properties": rec.pop("tags", None),
                    "metadata": metadata,
                }
        except Exception as e:
            self.logger.error("OSM parsing failed for %s: %s", self.filepath, e)
            raise
