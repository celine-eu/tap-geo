"""GeoStream base logic for geospatial file parsing."""

from __future__ import annotations
import typing as t
from pathlib import Path

import fiona
from shapely.geometry import shape, mapping
from shapely.wkt import dumps as to_wkt
from singer_sdk.streams import Stream
from singer_sdk import typing as th
import os
from datetime import datetime, timezone
from .osm import OSMHandler

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

__INCREMENTAL_KEY = "__updated_at"


class GeoStream(Stream):
    """Stream for geospatial files (SHP, GeoJSON, OSM, GPX, etc.)."""

    def __init__(self, tap: Tap, file_cfg: dict) -> None:

        self.file_cfg = file_cfg
        # expand config to handle multiple files
        self.filepaths = [Path(p) for p in file_cfg.get("paths", [])]
        if not self.filepaths:
            raise ValueError(
                "GeoStream requires at least one path in file_cfg['paths']."
            )

        table_name = file_cfg.get("table_name") or self.filepaths[0].stem
        super().__init__(tap, name=table_name)

        self.state_partitioning_keys = ["filename"]
        self.replication_key = __INCREMENTAL_KEY
        self.forced_replication_method = "INCREMENTAL"

        self.primary_keys: list[str] = [
            p.lower() for p in file_cfg.get("primary_keys", [])
        ]

        self.core_fields = ["geometry", "features", "metadata"]
        self.expose_fields: list[str] = [
            p.lower()
            for p in file_cfg.get("expose_fields", [])
            if not p.lower() in self.core_fields
        ]

        # ensure PKs are exposed
        for pk in self.primary_keys:
            if pk not in self.expose_fields:
                self.expose_fields.append(pk)

        self.tap = tap

    @property
    def schema(self) -> dict:
        """Build schema once, based on the first file."""
        base_props = {
            "geometry": {"type": ["null", "string", "object"]},
            "features": {"type": ["null", "object"]},
            "metadata": {"type": ["null", "object"]},
        }
        extras = {
            f.lower(): {"type": ["null", "string", "number", "object"]}
            for f in self.expose_fields
        }

        extras[__INCREMENTAL_KEY] = th.Property(
            str(self.replication_key),
            th.DateTimeType(nullable=True),
            description="Replication checkpoint (file mtime or row date)",
        ).to_dict()

        suffix = self.filepaths[0].suffix.lower()
        if suffix in (".osm", ".pbf"):
            return {
                "type": "object",
                "properties": {
                    **extras,
                    "id": {"type": ["null", "string", "number"]},
                    "type": {"type": ["null", "string"]},
                    "members": {"type": ["null", "array"]},
                    **base_props,
                },
            }

        try:
            with fiona.open(self.filepaths[0]):
                pass
        except Exception as e:
            self.logger.error("Failed to open %s with Fiona: %s", self.filepaths[0], e)
            raise

        return {"type": "object", "properties": {**extras, **base_props}}

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Iterate through all files in this stream config."""
        skip_fields = set(self.tap.config.get("skip_fields", []))
        geom_fmt = self.tap.config.get("geometry_format", "wkt")

        for filepath in self.filepaths:

            partition_context = {"filename": os.path.basename(filepath)}
            last_bookmark = self.get_starting_replication_key_value(partition_context)

            bookmark_dt: datetime | None = None
            if last_bookmark:
                bookmark_dt = datetime.fromisoformat(last_bookmark)
                if bookmark_dt.tzinfo is None:
                    bookmark_dt = bookmark_dt.replace(tzinfo=timezone.utc)
                else:
                    bookmark_dt = bookmark_dt.astimezone(timezone.utc)

            mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)

            # skip file entirely if mtime <= bookmark
            if bookmark_dt and mtime <= bookmark_dt:
                self.logger.info(
                    "Skipping %s (mtime=%s <= bookmark=%s)",
                    filepath,
                    mtime,
                    bookmark_dt,
                )
                return []

            suffix = filepath.suffix.lower()
            try:
                if suffix in (".osm", ".pbf"):
                    yield from self._parse_osm(filepath, geom_fmt, mtime)
                else:
                    yield from self._parse_with_fiona(
                        filepath, skip_fields, geom_fmt, mtime
                    )
            except Exception as e:
                self.logger.exception("Failed parsing file %s: %s", filepath, e)
                raise

    def _parse_with_fiona(
        self, filepath: Path, skip_fields: set[str], geom_fmt: str, mtime: datetime
    ) -> t.Iterable[dict]:
        """Parse SHP, GeoJSON, GPKG via Fiona."""
        try:
            with fiona.open(filepath) as src:
                crs = src.crs_wkt or src.crs
                driver = src.driver
                for i, feat in enumerate(src, start=1):
                    try:

                        props = {
                            k: v
                            for k, v in feat["properties"].items()
                            if k not in skip_fields
                        }
                        props_map = {k.lower(): k for k in props}

                        exposed = {
                            k.lower(): props.pop(props_map[k.lower()])
                            for k in list(self.expose_fields)
                            if k.lower() in props_map
                            and k.lower() not in self.core_fields
                        }

                        geom = None
                        if feat.get("geometry"):
                            if geom_fmt == "wkt":
                                geom = to_wkt(shape(feat["geometry"]))
                            elif geom_fmt == "geojson":
                                geom = mapping(shape(feat["geometry"]))
                        yield {
                            **exposed,
                            "geometry": geom,
                            "features": props,
                            "metadata": {
                                "source": str(filepath),
                                "driver": driver,
                                "crs": crs,
                            },
                            __INCREMENTAL_KEY: mtime,
                        }
                    except Exception as fe:
                        self.logger.warning(
                            "Failed to parse feature %d in %s: %s", i, filepath, fe
                        )
        except Exception as e:
            self.logger.error("Could not open dataset %s: %s", filepath, e)
            raise

    def _parse_osm(
        self, filepath: Path, geom_fmt: str, mtime: datetime
    ) -> t.Iterable[dict]:
        """Parse OSM XML/PBF using pyosmium."""
        try:
            handler = OSMHandler(geom_fmt)
            handler.apply_file(str(filepath))
            for rec in handler.records:
                metadata = {"source": str(filepath)}
                tags = rec.pop("tags", {}) or {}
                exposed = {
                    k.lower(): tags.pop(k)
                    for k in list(self.expose_fields)
                    if k in tags
                    and k.lower() not in [*self.core_fields, "id", "type", "members"]
                }
                yield {
                    **exposed,
                    "id": rec.get("id") or rec.get("@id"),
                    "type": rec.get("type"),
                    "members": rec.pop("members", None),
                    "geometry": rec.get("geometry"),
                    "features": tags,
                    "metadata": metadata,
                    __INCREMENTAL_KEY: mtime,
                }
        except Exception as e:
            self.logger.error("OSM parsing failed for %s: %s", filepath, e)
            raise
