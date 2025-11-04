"""GeoStream base logic for geospatial file parsing (SHP, GeoJSON, GPX, OSM/PBF) with storage abstraction."""

from __future__ import annotations
import typing as t
import tempfile
import os
import json
from datetime import datetime, timezone
from pathlib import Path

import shapely.geometry
from shapely.wkt import dumps as to_wkt
import shapefile  # pyshp
import gpxpy

from singer_sdk.streams import Stream
from singer_sdk import typing as th

from .storage import Storage, FileInfo
from .osm import OSMHandler
from contextlib import contextmanager

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

SDC_INCREMENTAL_KEY = "_sdc_last_modified"
SDC_FILENAME = "_sdc_filename"


class GeoStream(Stream):
    """Stream for geospatial files (SHP, GeoJSON, GPX, OSM/PBF) supporting fsspec storage."""

    def __init__(self, tap: Tap, file_cfg: dict) -> None:
        self.file_cfg = file_cfg
        self.path_patterns = file_cfg.get("paths", [])
        if not self.path_patterns:
            raise ValueError(
                "GeoStream requires at least one path in file_cfg['paths']."
            )

        self.table_name = file_cfg.get("table_name") or Path(self.path_patterns[0]).stem
        super().__init__(tap, name=self.table_name)

        self.state_partitioning_keys = [SDC_FILENAME]
        self.replication_key = SDC_INCREMENTAL_KEY
        self.forced_replication_method = "INCREMENTAL"

        self.primary_keys: list[str] = [
            p.lower() for p in file_cfg.get("primary_keys", [])
        ]

        self.core_fields = ["geometry", "features", "metadata"]
        self.expose_fields: list[str] = [
            p.lower()
            for p in file_cfg.get("expose_fields", [])
            if p.lower() not in self.core_fields
        ]
        for pk in self.primary_keys:
            if pk not in self.expose_fields:
                self.expose_fields.append(pk)

        self.tap = tap
        self.storages = [Storage(pat) for pat in self.path_patterns]

    # -------------------------------------------------------------------------
    # Utility: staged local file for remote handling
    # -------------------------------------------------------------------------
    @contextmanager
    def _staged_local_file(self, st: Storage, path: str):
        """Yield a local filesystem path; downloads to temp dir if remote."""
        if os.path.exists(path):
            yield path
            return

        suffix = Path(path).suffix.lower()
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / Path(path).name

            if suffix == ".shp":
                base = os.path.splitext(path)[0]
                for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                    candidate = base + ext
                    try:
                        with (
                            st.open(candidate, "rb") as fh,
                            open(Path(tmpdir) / Path(candidate).name, "wb") as out,
                        ):
                            out.write(fh.read())
                    except Exception:
                        continue
            else:
                with st.open(path, "rb") as fh, open(local_path, "wb") as out:
                    out.write(fh.read())

            yield str(local_path)

    # -------------------------------------------------------------------------
    # Schema building (aligned with parsing)
    # -------------------------------------------------------------------------
    @property
    def schema(self) -> dict:
        """Infer schema from the first available file by introspection."""
        test_path = None
        storage = None
        for st in self.storages:
            files = st.glob()
            if files:
                test_path = files[0]
                storage = st
                break
        if not test_path or not storage:
            raise FileNotFoundError("No files found for GeoStream schema detection")

        suffix = Path(test_path).suffix.lower()

        parser_map = {
            ".shp": self._peek_shapefile,
            ".geojson": self._peek_geojson,
            ".json": self._peek_geojson,
            ".gpx": self._peek_gpx,
            ".osm": self._peek_osm,
            ".pbf": self._peek_osm,
        }

        parser = parser_map.get(suffix)
        if not parser:
            raise ValueError(f"Unsupported file type for schema: {suffix}")

        first_record = next(parser(storage, test_path), None)
        if not first_record:
            raise ValueError(f"No records found for schema inference: {test_path}")

        properties: list[th.Property] = []

        for k, v in first_record.items():
            if k in (SDC_INCREMENTAL_KEY, SDC_FILENAME):
                continue

            tpe: t.Any
            # --- Explicit type detection order (list first)
            if isinstance(v, list):
                # Infer element type if possible
                elem_type: t.Any | None = None
                for elem in v:
                    if elem is None:
                        continue
                    if isinstance(elem, (int, float)):
                        elem_type = th.NumberType(nullable=True)
                    elif isinstance(elem, str):
                        elem_type = th.StringType(nullable=True)
                    elif isinstance(elem, dict):
                        elem_type = th.ObjectType(
                            additional_properties=True, nullable=True
                        )
                    else:
                        elem_type = th.CustomType(
                            {"type": ["null", "string", "object", "number"]}
                        )
                    break
                if elem_type is None:
                    elem_type = th.CustomType(
                        {"type": ["null", "string", "object", "number"]}
                    )
                tpe = th.ArrayType(elem_type)

            elif isinstance(v, (int, float)):
                tpe = th.NumberType(nullable=True)

            elif isinstance(v, str):
                tpe = th.StringType(nullable=True)

            elif isinstance(v, dict):
                tpe = th.ObjectType(additional_properties=True, nullable=True)

            else:
                # Generic fallback type: allow arrays too, to prevent schema rejection
                tpe = th.CustomType(
                    {"type": ["null", "string", "object", "number", "array"]}
                )

            properties.append(th.Property(k, tpe))

        # Always include incremental + filename keys
        properties.extend(
            [
                th.Property(SDC_INCREMENTAL_KEY, th.DateTimeType(nullable=True)),
                th.Property(SDC_FILENAME, th.StringType(nullable=True)),
            ]
        )
        return th.PropertiesList(*properties).to_dict()

    # -------------------------------------------------------------------------
    # Record iteration
    # -------------------------------------------------------------------------
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Iterate through all files in configured storages."""
        skip_fields = set(self.tap.config.get("skip_fields", []))
        geom_fmt = self.tap.config.get("geometry_format", "wkt")

        for st in self.storages:
            for path in st.glob():
                info: FileInfo = st.describe(path)

                partition_context = {SDC_FILENAME: os.path.basename(info.path)}
                last_bookmark = self.get_starting_replication_key_value(
                    partition_context
                )
                bookmark_dt = None
                if last_bookmark:
                    bookmark_dt = datetime.fromisoformat(last_bookmark)
                    if bookmark_dt.tzinfo is None:
                        bookmark_dt = bookmark_dt.replace(tzinfo=timezone.utc)

                if bookmark_dt and info.mtime <= bookmark_dt:
                    self.logger.info(
                        "Skipping %s (mtime=%s <= bookmark=%s)",
                        info.path,
                        info.mtime,
                        bookmark_dt,
                    )
                    continue

                suffix = Path(info.path).suffix.lower()
                try:
                    if suffix == ".shp":
                        yield from self._parse_shapefile(
                            st, info.path, skip_fields, geom_fmt, info.mtime
                        )
                    elif suffix in (".geojson", ".json"):
                        yield from self._parse_geojson(
                            st, info.path, skip_fields, geom_fmt, info.mtime
                        )
                    elif suffix == ".gpx":
                        yield from self._parse_gpx(st, info.path, geom_fmt, info.mtime)
                    elif suffix in (".osm", ".pbf"):
                        yield from self._parse_osm(st, info.path, geom_fmt, info.mtime)
                    else:
                        self.logger.warning(
                            "Skipping unsupported file suffix %s", suffix
                        )
                        continue

                    self._increment_stream_state(
                        {SDC_INCREMENTAL_KEY: info.mtime.isoformat()},
                        context=partition_context,
                    )
                except Exception as e:
                    self.logger.exception("Failed parsing file %s: %s", info.path, e)
                    raise

    # -------------------------------------------------------------------------
    # Parsers
    # -------------------------------------------------------------------------
    def _parse_shapefile(self, st, path, skip_fields, geom_fmt, mtime):
        with self._staged_local_file(st, path) as local:
            reader = shapefile.Reader(local)
            fields = reader.fields[1:]
            field_names = [f[0].lower() for f in fields]

            for sr in reader.iterShapeRecords():
                geom = shapely.geometry.shape(sr.shape.__geo_interface__)
                geom_out = to_wkt(geom) if geom_fmt == "wkt" else geom.__geo_interface__
                props = {
                    field_names[i]: sr.record[i]
                    for i in range(len(field_names))
                    if field_names[i] not in skip_fields
                }
                exposed = {
                    k: props.pop(k) for k in list(self.expose_fields) if k in props
                }
                yield {
                    **exposed,
                    "geometry": geom_out,
                    "features": props,
                    "metadata": {"source": path, "driver": "shapefile"},
                    SDC_INCREMENTAL_KEY: mtime,
                    SDC_FILENAME: os.path.basename(path),
                }

    def _peek_shapefile(self, st, path):
        yield from self._parse_shapefile(
            st, path, set(), "wkt", datetime.now(timezone.utc)
        )

    def _parse_geojson(self, st, path, skip_fields, geom_fmt, mtime):
        with self._staged_local_file(st, path) as local:
            with open(local, "r", encoding="utf-8") as jf:
                gj = json.load(jf)

            features = gj.get("features") if "features" in gj else [gj]
            for feat in features:
                geom_obj = shapely.geometry.shape(feat["geometry"])
                geom_out = to_wkt(geom_obj) if geom_fmt == "wkt" else feat["geometry"]
                props = {
                    k.lower(): v
                    for k, v in (feat.get("properties") or {}).items()
                    if k.lower() not in skip_fields
                }
                exposed = {
                    k: props.pop(k) for k in list(self.expose_fields) if k in props
                }
                yield {
                    **exposed,
                    "geometry": geom_out,
                    "features": props,
                    "metadata": {"source": path, "driver": "geojson"},
                    SDC_INCREMENTAL_KEY: mtime,
                    SDC_FILENAME: os.path.basename(path),
                }

    def _peek_geojson(self, st, path):
        yield from self._parse_geojson(
            st, path, set(), "wkt", datetime.now(timezone.utc)
        )

    def _parse_gpx(self, st, path, geom_fmt, mtime):
        with self._staged_local_file(st, path) as local:
            with open(local, "r", encoding="utf-8") as gf:
                gpx = gpxpy.parse(gf)

            for wp in gpx.waypoints:
                geom_obj = shapely.geometry.Point(wp.longitude, wp.latitude)
                geom_out = (
                    to_wkt(geom_obj)
                    if geom_fmt == "wkt"
                    else geom_obj.__geo_interface__
                )
                yield {
                    "geometry": geom_out,
                    "features": {
                        "name": wp.name,
                        "elevation": wp.elevation,
                        "time": wp.time.isoformat() if wp.time else None,
                    },
                    "metadata": {"source": path, "driver": "gpx_waypoint"},
                    SDC_INCREMENTAL_KEY: mtime,
                    SDC_FILENAME: os.path.basename(path),
                }

            for track in gpx.tracks:
                for segment in track.segments:
                    coords = [(pt.longitude, pt.latitude) for pt in segment.points]
                    geom_obj = shapely.geometry.LineString(coords)
                    geom_out = (
                        to_wkt(geom_obj)
                        if geom_fmt == "wkt"
                        else geom_obj.__geo_interface__
                    )
                    yield {
                        "geometry": geom_out,
                        "features": {
                            "name": track.name,
                            "segment_index": getattr(segment, "index", None),
                            "elevations": [pt.elevation for pt in segment.points],
                        },
                        "metadata": {"source": path, "driver": "gpx_track"},
                        SDC_INCREMENTAL_KEY: mtime,
                        SDC_FILENAME: os.path.basename(path),
                    }

    def _peek_gpx(self, st, path):
        yield from self._parse_gpx(st, path, "wkt", datetime.now(timezone.utc))

    def _parse_osm(self, st, path, geom_fmt, mtime):
        with self._staged_local_file(st, path) as local:
            handler = OSMHandler(geom_fmt)
            handler.apply_file(local)
            for rec in handler.records:
                metadata = {"source": path}
                tags = rec.pop("tags", {}) or {}
                exposed = {
                    k.lower(): tags.pop(k)
                    for k in self.expose_fields
                    if k in tags
                    and k.lower() not in [*self.core_fields, "id", "type", "members"]
                }
                yield {
                    **exposed,
                    "id": rec.get("id"),
                    "type": rec.get("type"),
                    "members": rec.pop("members", None),
                    "geometry": rec.get("geometry"),
                    "features": tags,
                    "metadata": metadata,
                    SDC_INCREMENTAL_KEY: mtime,
                    SDC_FILENAME: os.path.basename(path),
                }

    def _peek_osm(self, st, path):
        yield from self._parse_osm(st, path, "wkt", datetime.now(timezone.utc))
