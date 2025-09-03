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
        self.expose_fields: list[str] = file_cfg.get("expose_fields", [])

        # ensure pks are also exposed as columns
        for pk in self.primary_keys:
            if pk not in self.expose_fields:
                self.expose_fields.append(pk)

        self.tap = tap
        super().__init__(tap, name=table_name)

    @property
    def schema(self) -> dict:
        """Build schema from file metadata."""

        base_props = {
            "id": {"type": ["null", "string"]},
            "geometry": {"type": ["null", "string", "object"]},
            "features": {"type": ["null", "object"]},
            "metadata": {"type": ["null", "object"]},
        }

        extras = {f: {"type": ["null", "string"]} for f in self.expose_fields}

        suffix = self.filepath.suffix.lower()
        if suffix in (".osm", ".pbf"):
            return {
                "type": "object",
                "properties": {
                    **extras,
                    "id": {"type": ["null", "string"]},
                    "type": {"type": ["null", "string"]},
                    "members": {"type": ["null", "array"]},
                    **base_props,
                },
            }

        try:
            with fiona.open(self.filepath):
                pass
        except Exception as e:
            self.logger.error("Failed to open %s with Fiona: %s", self.filepath, e)
            raise

        return {"type": "object", "properties": {**extras, **base_props}}

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
                        props = {
                            k: v
                            for k, v in feat["properties"].items()
                            if k not in skip_fields
                        }

                        exposed = {
                            k: props.pop(k)
                            for k in list(self.expose_fields)
                            if k in props
                        }

                        geom = None
                        if feat.get("geometry"):
                            if geom_fmt == "wkt":
                                geom = to_wkt(shape(feat["geometry"]))
                            elif geom_fmt == "geojson":
                                geom = mapping(shape(feat["geometry"]))

                        id_value = exposed.get("id", None)
                        if self.file_cfg.get("primary_keys", None):
                            try:
                                id_value = "_".join(
                                    str(exposed.get(k) or props.get(k))
                                    for k in self.file_cfg.get("primary_keys", [])
                                    if (exposed.get(k) or props.get(k)) is not None
                                )
                            except Exception as e:
                                self.logger.warning(
                                    "Failed to build ID from primary keys: %s", e
                                )

                        yield {
                            **exposed,
                            "id": id_value,
                            "geometry": geom,
                            "features": props,
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
                metadata = {
                    "source": str(self.filepath),
                }

                tags = rec.pop("tags", {}) or {}
                exposed = {
                    k: tags.pop(k) for k in list(self.expose_fields) if k in tags
                }

                yield {
                    **exposed,
                    "id": rec.get("id") or rec.get("@id"),
                    "type": rec.get("type"),
                    "members": rec.pop("members", None),
                    "geometry": rec.get("geometry"),
                    "features": tags,
                    "metadata": metadata,
                }
        except Exception as e:
            self.logger.error("OSM parsing failed for %s: %s", self.filepath, e)
            raise
