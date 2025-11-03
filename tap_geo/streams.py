"""GeoStream base logic for geospatial file parsing, with storage abstraction and shapefile support."""

from __future__ import annotations
import typing as t
import tempfile
import os
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

import fiona
from shapely.geometry import shape, mapping
from shapely.wkt import dumps as to_wkt
from singer_sdk.streams import Stream
from singer_sdk import typing as th

from .osm import OSMHandler
from .storage import Storage, FileInfo

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

SDC_INCREMENTAL_KEY = "_sdc_last_modified"
SDC_FILENAME = "_sdc_filename"


class GeoStream(Stream):
    """Stream for geospatial files (SHP, GeoJSON, OSM, GPX, etc.) supporting fsspec storage."""

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

    # ------------------------------
    # Utility: staged file provider
    # ------------------------------
    @contextmanager
    def staged_local_file(self, st: Storage, path: str) -> t.Generator[str, None, None]:
        """
        Yield a local path usable by Fiona, downloading remote files if needed.
        - For local files → yields directly.
        - For remote shapefiles → downloads .shp + .shx + .dbf + .prj + .cpg.
        - For remote single files → downloads one file.
        """
        if os.path.exists(path):  # Local filesystem, no need to copy
            yield path
            return

        suffix = Path(path).suffix.lower()
        with tempfile.TemporaryDirectory() as tmpdir:
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
                yield str(Path(tmpdir) / Path(path).name)
            else:
                local_path = Path(tmpdir) / Path(path).name
                with st.open(path, "rb") as fh, open(local_path, "wb") as out:
                    out.write(fh.read())
                yield str(local_path)

    # ------------------------------
    # Schema generation
    # ------------------------------
    @property
    def schema(self) -> dict:
        """Build schema once, based on the first accessible file."""
        test_path = None
        for st in self.storages:
            files = st.glob()
            if files:
                test_path = files[0]
                storage = st
                break
        if not test_path:
            raise FileNotFoundError("No files found for GeoStream schema detection")

        suffix = Path(test_path).suffix.lower()

        base_props = [
            th.Property(
                "geometry", th.CustomType({"type": ["null", "string", "object"]})
            ),
            th.Property(
                "features", th.ObjectType(additional_properties=True, nullable=True)
            ),
            th.Property(
                "metadata", th.ObjectType(additional_properties=True, nullable=True)
            ),
            th.Property(SDC_INCREMENTAL_KEY, th.DateTimeType(nullable=True)),
            th.Property(SDC_FILENAME, th.StringType(nullable=True)),
        ]
        extras = [
            th.Property(
                f, th.CustomType({"type": ["null", "string", "number", "object"]})
            )
            for f in self.expose_fields
        ]

        if suffix in (".osm", ".pbf"):
            osm_props = [
                th.Property(
                    "id", th.CustomType({"type": ["null", "string", "number"]})
                ),
                th.Property("type", th.StringType(nullable=True)),
                th.Property(
                    "members", th.ArrayType(th.ObjectType(additional_properties=True))
                ),
            ]
            return th.PropertiesList(*extras, *osm_props, *base_props).to_dict()

        # Try opening via Fiona (using local or staged copy)
        with self.staged_local_file(storage, test_path) as local_file:
            with fiona.open(local_file):
                pass

        return th.PropertiesList(*extras, *base_props).to_dict()

    # ------------------------------
    # Record iteration
    # ------------------------------
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
                    if suffix in (".osm", ".pbf"):
                        yield from self._parse_osm(st, info.path, geom_fmt, info.mtime)
                    else:
                        yield from self._parse_with_fiona(
                            st, info.path, skip_fields, geom_fmt, info.mtime
                        )

                    self._increment_stream_state(
                        {SDC_INCREMENTAL_KEY: info.mtime.isoformat()},
                        context=partition_context,
                    )
                except Exception as e:
                    self.logger.exception("Failed parsing file %s: %s", info.path, e)
                    raise

    # ------------------------------
    # Parsing logic
    # ------------------------------
    def _parse_with_fiona(
        self,
        st: Storage,
        path: str,
        skip_fields: set[str],
        geom_fmt: str,
        mtime: datetime,
    ) -> t.Iterable[dict]:
        """Parse SHP, GeoJSON, GPKG via Fiona, staging remote datasets if needed."""
        with self.staged_local_file(st, path) as local_file:
            with fiona.open(local_file) as src:
                crs = src.crs_wkt or src.crs
                driver = src.driver
                for feat in src:
                    props = {
                        k: v
                        for k, v in feat["properties"].items()
                        if k not in skip_fields
                    }
                    props_map = {k.lower(): k for k in props}
                    exposed = {
                        k.lower(): props.pop(props_map[k.lower()])
                        for k in self.expose_fields
                        if k.lower() in props_map and k.lower() not in self.core_fields
                    }

                    geom = None
                    if feat.get("geometry"):
                        geom = (
                            to_wkt(shape(feat["geometry"]))
                            if geom_fmt == "wkt"
                            else mapping(shape(feat["geometry"]))
                        )

                    yield {
                        **exposed,
                        "geometry": geom,
                        "features": props,
                        "metadata": {"source": path, "driver": driver, "crs": crs},
                        SDC_INCREMENTAL_KEY: mtime,
                        SDC_FILENAME: os.path.basename(path),
                    }

    def _parse_osm(
        self,
        st: Storage,
        path: str,
        geom_fmt: str,
        mtime: datetime,
    ) -> t.Iterable[dict]:
        """Parse OSM XML or PBF via pyosmium (temp file if remote)."""
        with self.staged_local_file(st, path) as local_file:
            handler = OSMHandler(geom_fmt)
            handler.apply_file(local_file)
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
