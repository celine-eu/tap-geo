"""GeoStream base logic for geospatial file parsing (SHP, GeoJSON, GPX, OSM/PBF, GPKG) with storage abstraction."""

from __future__ import annotations
import typing as t
import functools
import hashlib
import os
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely.geometry
from shapely.geometry import box as shapely_box
from shapely.wkt import dumps as to_wkt, loads as from_wkt
from shapely.wkb import loads as from_wkb
import shapefile  # pyshp
import gpxpy

from singer_sdk.streams import Stream
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk import typing as th

from .storage import Storage, FileInfo
from .osm import OSMHandler
from contextlib import contextmanager

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

SDC_INCREMENTAL_KEY = "_sdc_last_modified"
SDC_FILENAME = "_sdc_filename"


def _to_python_native(value: t.Any) -> t.Any:  # noqa: PLR0911
    """Convert numpy/pandas types to JSON-serializable Python native types."""
    if value is None:
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if not np.isnan(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, (list, dict, str, int, float, bool)):
        return value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


class GeoStream(Stream):
    """Stream for geospatial files (SHP, GeoJSON, GPX, OSM/PBF, GPKG) supporting fsspec storage."""

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

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

        bbox_raw = file_cfg.get("bbox")
        if bbox_raw is not None:
            if not isinstance(bbox_raw, (list, tuple)) or len(bbox_raw) != 4:  # noqa: PLR2004
                msg = f"bbox must be [west, south, east, north] (4 numbers), got: {bbox_raw}"
                raise ValueError(msg)
            self.bbox: tuple[float, float, float, float] | None = (
                float(bbox_raw[0]), float(bbox_raw[1]),
                float(bbox_raw[2]), float(bbox_raw[3]),
            )
        else:
            self.bbox = None

        self.tap = tap
        self.storages = [Storage(pat) for pat in self.path_patterns]

    # -------------------------------------------------------------------------
    # Shared cache
    # -------------------------------------------------------------------------
    _CACHE_MAX_AGE_DAYS = 7

    @property
    def _cache_dir(self) -> Path:
        d = Path("cache")
        d.mkdir(exist_ok=True)
        return d

    def _evict_stale_cache(self, keep: set[str] | None = None) -> None:
        """Remove cache entries older than _CACHE_MAX_AGE_DAYS, preserving `keep`."""
        import time

        keep = keep or set()
        cutoff = time.time() - (self._CACHE_MAX_AGE_DAYS * 86400)
        removed = 0
        freed = 0
        for f in self._cache_dir.iterdir():
            if str(f) in keep or not f.is_file():
                continue
            try:
                stat = f.stat()
                if stat.st_mtime < cutoff:
                    freed += stat.st_size
                    f.unlink()
                    removed += 1
            except OSError:
                continue
        if removed:
            self.logger.info(
                "Cache cleanup: removed %d stale file(s), freed %.1f MB",
                removed, freed / 1_048_576,
            )

    # -------------------------------------------------------------------------
    # Utility: staged local file for remote handling
    # -------------------------------------------------------------------------
    @contextmanager
    def _staged_local_file(self, st: Storage, path: str):
        """Yield a local filesystem path; caches remote files in ./cache."""
        if os.path.exists(path):
            yield path
            return

        suffix = Path(path).suffix.lower()
        cache = self._cache_dir

        if suffix == ".shp":
            base = os.path.splitext(path)[0]
            local_path = cache / Path(path).name
            for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                candidate = base + ext
                dest = cache / Path(candidate).name
                if dest.exists():
                    continue
                try:
                    with (
                        st.open(candidate, "rb") as fh,
                        open(dest, "wb") as out,
                    ):
                        out.write(fh.read())
                except Exception:
                    continue
        else:
            local_path = cache / Path(path).name
            if not local_path.exists():
                with st.open(path, "rb") as fh, open(local_path, "wb") as out:
                    out.write(fh.read())

        local_path.touch()
        yield str(local_path)

    # -------------------------------------------------------------------------
    # Schema building (aligned with parsing)
    # -------------------------------------------------------------------------
    @functools.cached_property
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
            ".gpkg": self._peek_gpkg,
            ".parquet": self._peek_parquet,
            ".geoparquet": self._peek_parquet,
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
                    # bool must be checked before int/float: bool is a subclass of int
                    if isinstance(elem, bool):
                        elem_type = th.BooleanType(nullable=True)
                    elif isinstance(elem, (int, float)):
                        elem_type = th.NumberType(nullable=True)
                    elif isinstance(elem, str):
                        elem_type = th.StringType(nullable=True)
                    elif isinstance(elem, dict):
                        elem_type = th.ObjectType(
                            additional_properties=True, nullable=True
                        )
                    else:
                        elem_type = th.CustomType(
                            {"type": ["null", "string", "object", "number", "boolean"]}
                        )
                    break
                if elem_type is None:
                    elem_type = th.CustomType(
                        {"type": ["null", "string", "object", "number", "boolean"]}
                    )
                tpe = th.ArrayType(elem_type)

            # bool must be checked before int/float: bool is a subclass of int
            elif isinstance(v, bool):
                tpe = th.BooleanType(nullable=True)

            elif isinstance(v, (int, float)):
                tpe = th.NumberType(nullable=True)

            elif isinstance(v, str):
                tpe = th.StringType(nullable=True)

            elif isinstance(v, dict):
                tpe = th.ObjectType(additional_properties=True, nullable=True)

            else:
                # Generic fallback type: allow arrays too, to prevent schema rejection
                tpe = th.CustomType(
                    {"type": ["null", "string", "object", "number", "array", "boolean"]}
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
    @staticmethod
    def _is_parquet_pattern(pattern: str) -> bool:
        """Check if a glob pattern targets parquet files."""
        clean = pattern.rstrip("*").rstrip("/")
        return clean.endswith((".parquet", ".geoparquet")) or pattern.endswith("/*")

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Iterate through all files in configured storages."""
        skip_fields = set(self.file_cfg.get("skip_fields", []))
        geom_fmt = self.file_cfg.get("geometry_format", "wkt")

        parquet_dirs: list[str] = []
        other_patterns: list[str] = []

        for pattern in self.path_patterns:
            clean = pattern.rstrip("*").rstrip("/")
            if pattern.endswith((".parquet", ".geoparquet")):
                parquet_dirs.append(clean)
            elif pattern.endswith("/*"):
                st = Storage(pattern)
                sample = st.fs.ls(clean, detail=False)
                is_pq = any(
                    s.endswith((".parquet", ".geoparquet")) for s in sample[:5]
                )
                if is_pq:
                    parquet_dirs.append(clean)
                else:
                    other_patterns.append(pattern)
            else:
                other_patterns.append(pattern)

        if parquet_dirs:
            bbox_str = ",".join(str(x) for x in self.bbox) if self.bbox else ""
            fingerprint = hashlib.sha256(
                f"{sorted(parquet_dirs)}|{bbox_str}".encode()
            ).hexdigest()[:16]

            partition_ctx = {SDC_FILENAME: f"pq_{fingerprint}"}
            last_bookmark = self.get_starting_replication_key_value(partition_ctx)
            if last_bookmark:
                self.logger.info(
                    "Parquet dataset already synced (bookmark=%s, fingerprint=%s). "
                    "Change paths or bbox to trigger re-sync.",
                    last_bookmark, fingerprint,
                )
            else:
                self.logger.info(
                    "Found %d parquet source(s), querying as dataset "
                    "(fingerprint=%s)",
                    len(parquet_dirs), fingerprint,
                )
                record_count = 0
                for record in self._parse_parquet_dataset(
                    parquet_dirs, skip_fields, geom_fmt,
                ):
                    record_count += 1
                    yield record

                self._increment_stream_state(
                    {SDC_INCREMENTAL_KEY: datetime.now(timezone.utc).isoformat()},
                    context=partition_ctx,
                )
                self.logger.info(
                    "Parquet sync complete: %d records, state saved (fingerprint=%s)",
                    record_count, fingerprint,
                )

        other_files: list[tuple[Storage, str]] = []
        for pattern in other_patterns:
            st = Storage(pattern)
            for path in st.glob():
                other_files.append((st, path))

        if other_files:
            self.logger.info("Found %d non-parquet file(s)", len(other_files))

        file_count = len(other_files)
        for file_index, (st, path) in enumerate(other_files):
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
            file_label = os.path.basename(info.path)
            file_idx = file_index + 1
            self.logger.info(
                "Processing file %d/%d: %s", file_idx, file_count, file_label,
            )
            try:
                records: t.Iterable[dict] | None = None
                if suffix == ".shp":
                    records = self._parse_shapefile(
                        st, info.path, skip_fields, geom_fmt, info.mtime
                    )
                elif suffix in (".geojson", ".json"):
                    records = self._parse_geojson(
                        st, info.path, skip_fields, geom_fmt, info.mtime
                    )
                elif suffix == ".gpx":
                    records = self._parse_gpx(
                        st, info.path, geom_fmt, info.mtime
                    )
                elif suffix in (".osm", ".pbf"):
                    records = self._parse_osm(
                        st, info.path, geom_fmt, info.mtime
                    )
                elif suffix == ".gpkg":
                    records = self._parse_gpkg(
                        st, info.path, skip_fields, geom_fmt, info.mtime
                    )
                else:
                    self.logger.warning(
                        "Skipping unsupported file suffix %s", suffix
                    )
                    continue

                records = self._apply_bbox_filter(records, geom_fmt)

                record_count = 0
                for record in records:
                    record_count += 1
                    yield record

                self.logger.info(
                    "File %d/%d: %d records emitted from %s",
                    file_idx, file_count, record_count, file_label,
                )

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

    # -------------------------------------------------------------------------
    # GeoPackage (.gpkg)
    # -------------------------------------------------------------------------
    @staticmethod
    def _gpkg_wkb_to_geom(blob: bytes) -> shapely.geometry.base.BaseGeometry | None:
        """Parse a GPKG geometry blob (GPKG header + WKB) into a Shapely geometry."""
        if not blob or len(blob) < 8:
            return None
        # Magic bytes check
        if blob[0:2] != b"GP":
            return None
        flags = blob[3]
        little_endian = bool(flags & 0x01)
        envelope_type = (flags >> 1) & 0x07
        is_empty = bool((flags >> 4) & 0x01)
        if is_empty:
            return None
        # Envelope sizes: 0=none, 1=XY(32B), 2=XYZ(48B), 3=XYM(48B), 4=XYZM(64B)
        envelope_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
        envelope_bytes = envelope_sizes.get(envelope_type, 0)
        wkb_offset = 8 + envelope_bytes
        wkb = blob[wkb_offset:]
        return from_wkb(wkb)

    def _parse_gpkg(self, st, path, skip_fields, geom_fmt, mtime):
        with self._staged_local_file(st, path) as local:
            conn = sqlite3.connect(local)
            conn.row_factory = sqlite3.Row
            try:
                layers = conn.execute(
                    "SELECT table_name, column_name FROM gpkg_geometry_columns"
                ).fetchall()
                for layer_name, geom_col in layers:
                    cols = conn.execute(
                        f"PRAGMA table_info({layer_name})"  # noqa: S608
                    ).fetchall()
                    prop_cols = [
                        c["name"]
                        for c in cols
                        if c["name"] != geom_col
                    ]
                    for row in conn.execute(
                        f"SELECT {geom_col}, {', '.join(prop_cols)} FROM {layer_name}"  # noqa: S608
                    ):
                        geom_blob = row[0]
                        geom_obj = self._gpkg_wkb_to_geom(geom_blob)
                        if geom_obj is None:
                            continue
                        geom_out = (
                            to_wkt(geom_obj)
                            if geom_fmt == "wkt"
                            else geom_obj.__geo_interface__
                        )
                        props = {
                            c.lower(): row[c]
                            for c in prop_cols
                            if c.lower() not in skip_fields
                        }
                        exposed = {
                            k: props.pop(k)
                            for k in list(self.expose_fields)
                            if k in props
                        }
                        yield {
                            **exposed,
                            "geometry": geom_out,
                            "features": props,
                            "metadata": {
                                "source": path,
                                "driver": "gpkg",
                                "layer": layer_name,
                            },
                            SDC_INCREMENTAL_KEY: mtime,
                            SDC_FILENAME: os.path.basename(path),
                        }
            finally:
                conn.close()

    def _peek_gpkg(self, st, path):
        yield from self._parse_gpkg(
            st, path, set(), "wkt", datetime.now(timezone.utc)
        )

    # -------------------------------------------------------------------------
    # GeoParquet (.parquet / .geoparquet)
    # -------------------------------------------------------------------------
    @staticmethod
    def _pyarrow_filesystem(path: str) -> t.Any:
        """Build a pyarrow filesystem for the given path."""
        if not path.startswith("s3://"):
            return None
        import pyarrow.fs as pafs

        key = os.getenv("S3_ACCESS_KEY_ID")
        secret = os.getenv("S3_SECRET_ACCESS_KEY")
        if key and secret:
            endpoint = os.getenv("S3_ENDPOINT_URL")
            return pafs.S3FileSystem(
                access_key=key,
                secret_key=secret,
                endpoint_override=endpoint or "",
            )
        return pafs.S3FileSystem(anonymous=True, region="us-west-2")

    def _duckdb_connection(self, base_dirs: list[str]):
        """Create a DuckDB connection configured for the given paths."""
        import duckdb

        conn = duckdb.connect()
        conn.execute("SET enable_object_cache = true")

        is_remote = any(d.startswith("s3://") for d in base_dirs)
        if is_remote:
            conn.execute("INSTALL httpfs; LOAD httpfs")
            key = os.getenv("S3_ACCESS_KEY_ID")
            secret = os.getenv("S3_SECRET_ACCESS_KEY")
            if key and secret:
                conn.execute("SET s3_access_key_id = ?", [key])
                conn.execute("SET s3_secret_access_key = ?", [secret])
                endpoint = os.getenv("S3_ENDPOINT_URL")
                if endpoint:
                    from urllib.parse import urlparse as _urlparse

                    parsed = _urlparse(endpoint)
                    conn.execute("SET s3_endpoint = ?", [parsed.netloc])
                    if parsed.scheme == "http":
                        conn.execute("SET s3_use_ssl = false")
            else:
                conn.execute("""
                    CREATE SECRET (
                        TYPE S3,
                        PROVIDER CONFIG,
                        REGION 'us-west-2'
                    )
                """)

        return conn

    # ---- remote parquet helpers: footer scan + download ----

    def _list_remote_parquet(self, base_dirs: list[str]) -> tuple[list[str], t.Any]:
        """List parquet files in remote dirs, return (paths, pyarrow_fs)."""
        import pyarrow.fs as pafs

        filesystem = self._pyarrow_filesystem(base_dirs[0])
        all_files: list[str] = []

        for d in base_dirs:
            stripped = d[len("s3://"):] if d.startswith("s3://") else d
            stripped = stripped.rstrip("*").rstrip("/")

            if stripped.endswith((".parquet", ".geoparquet")):
                all_files.append(stripped)
            else:
                infos = filesystem.get_file_info(
                    pafs.FileSelector(stripped, recursive=False),
                )
                all_files.extend(
                    fi.path
                    for fi in infos
                    if fi.type == pafs.FileType.File
                    and fi.path.endswith((".parquet", ".geoparquet"))
                )

        return all_files, filesystem

    def _find_bbox_matching_files(
        self,
        all_files: list[str],
        filesystem: t.Any,
    ) -> list[str]:
        """Scan parquet footers in parallel; return files with row groups overlapping the bbox."""
        import pyarrow.parquet as pq
        from concurrent.futures import ThreadPoolExecutor, as_completed

        west, south, east, north = self.bbox  # type: ignore[misc]

        def _check(fpath: str) -> str | None:
            try:
                pf = pq.ParquetFile(fpath, filesystem=filesystem)
                meta = pf.metadata
                if meta.num_row_groups == 0:
                    return None

                col_map: dict[str, int] = {}
                rg0 = meta.row_group(0)
                for ci in range(rg0.num_columns):
                    p = rg0.column(ci).path_in_schema
                    if p in ("bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"):
                        col_map[p] = ci

                if len(col_map) < 4:  # noqa: PLR2004
                    return fpath

                for rg_idx in range(meta.num_row_groups):
                    rg = meta.row_group(rg_idx)
                    s_xmin = rg.column(col_map["bbox.xmin"]).statistics
                    s_xmax = rg.column(col_map["bbox.xmax"]).statistics
                    s_ymin = rg.column(col_map["bbox.ymin"]).statistics
                    s_ymax = rg.column(col_map["bbox.ymax"]).statistics

                    if s_xmin and s_xmin.has_min_max and s_xmin.max < west:
                        continue
                    if s_xmax and s_xmax.has_min_max and s_xmax.min > east:
                        continue
                    if s_ymin and s_ymin.has_min_max and s_ymin.max < south:
                        continue
                    if s_ymax and s_ymax.has_min_max and s_ymax.min > north:
                        continue
                    return fpath

                return None
            except Exception:
                return fpath

        matching: list[str] = []
        total = len(all_files)
        done = 0
        with ThreadPoolExecutor(max_workers=64) as pool:
            futs = {pool.submit(_check, f): f for f in all_files}
            for fut in as_completed(futs):
                done += 1
                result = fut.result()
                if result is not None:
                    matching.append(result)
                if done % 100 == 0 or done == total:
                    self.logger.info(
                        "Footer scan: %d/%d checked, %d matched so far",
                        done, total, len(matching),
                    )

        return sorted(matching)

    def _download_parquet_files(
        self,
        remote_paths: list[str],
        filesystem: t.Any,
    ) -> list[str]:
        """Download parquet files with cache. Skip files already on disk."""
        from concurrent.futures import ThreadPoolExecutor

        cache_dir = self._cache_dir
        workers = self.file_cfg.get("download_workers", 4)
        total = len(remote_paths)
        downloaded = 0
        cached = 0
        dl_bytes = 0

        def _dl(rpath: str) -> str:
            nonlocal downloaded, cached, dl_bytes
            fname = os.path.basename(rpath)
            lpath = cache_dir / fname

            remote_info = filesystem.get_file_info(rpath)
            if lpath.exists() and lpath.stat().st_size == remote_info.size:
                lpath.touch()
                cached += 1
                self.logger.info(
                    "Cache hit (%d+%d)/%d: %s (%.1f MB)",
                    cached, downloaded, total, fname,
                    lpath.stat().st_size / 1_048_576,
                )
                return str(lpath)

            file_bytes = 0
            with filesystem.open_input_stream(rpath) as inp, open(lpath, "wb") as out:
                while True:
                    chunk = inp.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    file_bytes += len(chunk)
            downloaded += 1
            dl_bytes += file_bytes
            self.logger.info(
                "Downloaded (%d+%d)/%d: %s (%.1f MB)",
                cached, downloaded, total, fname,
                file_bytes / 1_048_576,
            )
            return str(lpath)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(_dl, remote_paths))

        self.logger.info(
            "Files ready: %d cached, %d downloaded (%.1f MB new)",
            cached, downloaded, dl_bytes / 1_048_576,
        )

        self._evict_stale_cache(keep=set(results))
        return results

    # ---- DuckDB query (local or remote) ----

    def _duckdb_scan_parquet(
        self,
        file_paths: list[str],
        skip_fields: set[str],
        geom_fmt: str,
    ) -> t.Iterable[dict]:
        """Run a DuckDB scan over parquet files and yield records."""
        import shapely

        conn = self._duckdb_connection(file_paths)

        if len(file_paths) == 1:
            src_sql = f"read_parquet('{file_paths[0]}')"
        else:
            src_list = ", ".join(f"'{f}'" for f in file_paths)
            src_sql = f"read_parquet([{src_list}])"

        geom_col = "geometry"
        try:
            schema_rows = conn.execute(
                f"DESCRIBE SELECT * FROM {src_sql} LIMIT 0",
            ).fetchall()
            all_cols = [row[0] for row in schema_rows]
            needed = [c for c in all_cols if c.lower() not in skip_fields]
            if geom_col not in needed:
                needed.insert(0, geom_col)
        except Exception:
            needed = ["*"]

        col_sql = ", ".join(f'"{c}"' for c in needed) if needed != ["*"] else "*"

        where_parts: list[str] = []
        params: list[t.Any] = []
        if self.bbox is not None:
            west, south, east, north = self.bbox
            where_parts.append(
                "bbox.xmin >= ? AND bbox.xmax <= ? "
                "AND bbox.ymin >= ? AND bbox.ymax <= ?",
            )
            params.extend([west, east, south, north])

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        query = f"SELECT {col_sql} FROM {src_sql} {where_sql}"

        self.logger.info("DuckDB scanning %d column(s)", len(needed) if needed != ["*"] else -1)

        result = conn.execute(query, params)
        reader = result.to_arrow_reader(4096)

        mtime = datetime.now(timezone.utc)
        record_count = 0
        batch_count = 0

        for batch in reader:
            tbl = batch.to_pydict()
            num_rows = len(next(iter(tbl.values())))
            if num_rows == 0:
                continue

            geom_wkbs = tbl[geom_col]
            geom_array = shapely.from_wkb(geom_wkbs)
            valid = ~(shapely.is_missing(geom_array) | shapely.is_empty(geom_array))

            if geom_fmt == "wkt":
                geom_strings = shapely.to_wkt(geom_array)
            else:
                geom_strings = shapely.to_geojson(geom_array)

            for i in range(num_rows):
                if not valid[i]:
                    continue

                geom_out = (
                    geom_strings[i]
                    if geom_fmt == "wkt"
                    else json.loads(geom_strings[i])
                )

                props = {
                    k.lower(): _to_python_native(tbl[k][i])
                    for k in tbl
                    if k != geom_col and k.lower() not in skip_fields
                }
                exposed = {
                    k: props.pop(k)
                    for k in list(self.expose_fields)
                    if k in props
                }

                record_count += 1
                yield {
                    **exposed,
                    "geometry": geom_out,
                    "features": props,
                    "metadata": {"source": "parquet_dataset", "driver": "geoparquet"},
                    SDC_INCREMENTAL_KEY: mtime,
                    SDC_FILENAME: "dataset",
                }

            batch_count += 1

        self.logger.info("Parquet dataset: %d records emitted in %d batches", record_count, batch_count)

    # ---- orchestrator ----

    def _parse_parquet_dataset(
        self,
        base_dirs: list[str],
        skip_fields: set[str],
        geom_fmt: str,
    ) -> t.Iterable[dict]:
        """Parse GeoParquet. For remote sources with bbox, downloads matching files first."""
        is_remote = any(d.startswith("s3://") for d in base_dirs)

        if is_remote and self.bbox:
            all_files, filesystem = self._list_remote_parquet(base_dirs)
            self.logger.info(
                "Scanning %d parquet footers for bbox overlap...", len(all_files),
            )

            matching = self._find_bbox_matching_files(all_files, filesystem)
            if not matching:
                self.logger.info("No parquet files match the bbox filter")
                return

            self.logger.info(
                "Bbox matched %d of %d file(s), downloading...",
                len(matching), len(all_files),
            )

            local_paths = self._download_parquet_files(matching, filesystem)
            self.logger.info(
                "Querying %d local file(s) from %s",
                len(local_paths), self._cache_dir,
            )
            yield from self._duckdb_scan_parquet(
                local_paths, skip_fields, geom_fmt,
            )
        elif is_remote:
            # No bbox — direct remote scan
            sources = []
            for d in base_dirs:
                clean = d.rstrip("*").rstrip("/")
                sources.append(f"{clean}/*.parquet")
            sources = list(dict.fromkeys(sources))
            yield from self._duckdb_scan_parquet(sources, skip_fields, geom_fmt)
        else:
            # Local files
            yield from self._duckdb_scan_parquet(base_dirs, skip_fields, geom_fmt)

    def _peek_parquet(self, st, path):
        """Read first record via pyarrow (no bbox) for schema inference."""
        import pyarrow.parquet as pq

        filesystem = self._pyarrow_filesystem(path)
        pa_path = path[len("s3://"):] if path.startswith("s3://") else path
        pf = pq.ParquetFile(pa_path, filesystem=filesystem)

        geo_meta = json.loads(pf.schema_arrow.metadata.get(b"geo", b"{}"))
        geom_col = geo_meta.get("primary_column", "geometry")

        table = pf.read_row_group(0)
        if table.num_rows == 0:
            return

        row = table.to_pydict()
        first = {col: vals[0] for col, vals in row.items()}

        geom_wkb = first.pop(geom_col, None)
        if geom_wkb is None:
            return
        geom_obj = from_wkb(geom_wkb)
        if geom_obj.is_empty:
            return

        props = {k.lower(): _to_python_native(v) for k, v in first.items()}
        exposed = {k: props.pop(k) for k in list(self.expose_fields) if k in props}

        yield {
            **exposed,
            "geometry": to_wkt(geom_obj),
            "features": props,
            "metadata": {"source": path, "driver": "geoparquet"},
            SDC_INCREMENTAL_KEY: datetime.now(timezone.utc),
            SDC_FILENAME: os.path.basename(path),
        }

    # -------------------------------------------------------------------------
    # Bbox filtering (post-parse, for non-parquet formats)
    # -------------------------------------------------------------------------
    def _apply_bbox_filter(
        self, records: t.Iterable[dict], geom_fmt: str,
    ) -> t.Iterable[dict]:
        """Filter records by bounding box intersection."""
        if self.bbox is None:
            yield from records
            return

        bbox_geom = shapely_box(
            self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3],
        )

        for record in records:
            geom_value = record.get("geometry")
            if geom_value is None:
                continue

            if isinstance(geom_value, str):
                record_geom = from_wkt(geom_value)
            elif isinstance(geom_value, dict):
                record_geom = shapely.geometry.shape(geom_value)
            else:
                yield record
                continue

            if record_geom.intersects(bbox_geom):
                yield record
