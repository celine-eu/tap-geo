import osmium
from shapely.geometry import Point, LineString
from shapely.wkt import dumps as to_wkt
from shapely.geometry import mapping


class OSMHandler(osmium.SimpleHandler):
    """OSM parser using pyosmium."""

    def __init__(self, geom_fmt="wkt"):
        super().__init__()
        self.records = []
        self.geom_fmt = geom_fmt

    def node(self, n):
        geom = Point(n.location.lon, n.location.lat)
        geom_out = to_wkt(geom) if self.geom_fmt == "wkt" else mapping(geom)
        self.records.append(
            {
                "id": str(n.id),
                "type": "node",
                "geometry": geom_out,
                "tags": dict(n.tags),
                "metadata": {
                    "version": n.version,
                    "timestamp": str(n.timestamp),
                    "user": n.user,
                    "uid": n.uid,
                },
            }
        )

    def way(self, w):
        coords = [(n.lon, n.lat) for n in w.nodes if n.location.valid()]
        geom = LineString(coords) if coords else None
        if geom:
            geom_out = to_wkt(geom) if self.geom_fmt == "wkt" else mapping(geom)
        else:
            geom_out = None
        self.records.append(
            {
                "id": str(w.id),
                "type": "way",
                "geometry": geom_out,
                "tags": dict(w.tags),
            }
        )

    def relation(self, r):
        # Relations can be complex; we’ll just collect members + tags
        members = [{"type": m.type, "ref": m.ref, "role": m.role} for m in r.members]
        self.records.append(
            {
                "id": str(r.id),
                "type": "relation",
                "geometry": None,  # could be constructed with osmium.geom, if desired
                "tags": dict(r.tags),
                "members": members,
            }
        )
