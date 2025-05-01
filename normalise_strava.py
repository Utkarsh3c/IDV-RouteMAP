import os
import json
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime
import gpxpy
from fitparse import FitFile

RAW_DIR     = "raw"
OUT_INDEX   = "activity_index.json"
OUT_GEOJSON = "segments.geojson"

# ── UTILITY: Normalize sport values ─────────────────────────────────────────────
def normalize_sport(raw_sport):
    s = (raw_sport or "").strip().lower()
    if s in ("cycling", "bike", "biking"):   return "Biking"
    if s in ("running",):                       return "Running"
    return raw_sport.title() if raw_sport else "Unknown"

# ── PARSE TCX ───────────────────────────────────────────────────────────────────
def parse_tcx(path):
    ns = {"t": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
    tree = ET.parse(path)
    root = tree.getroot()

    # Sport metadata
    raw_sport = root.find(".//t:Activity", ns).get("Sport", "Unknown")
    meta = {"activityId": os.path.basename(path), "sport": normalize_sport(raw_sport)}

    laps = root.findall(".//t:Lap", ns)
    total_time = sum(float(l.get("TotalTimeSeconds","0")) for l in laps)
    distance   = sum(float(l.get("DistanceMeters","0"))     for l in laps)
    calories   = sum(int(l.findtext("t:Calories","0",ns))    for l in laps)

    avg_hrs = [int(l.find(".//t:AverageHeartRateBpm/t:Value",ns).text)
               for l in laps if l.find(".//t:AverageHeartRateBpm/t:Value",ns) is not None]
    max_hrs = [int(l.find(".//t:MaximumHeartRateBpm/t:Value",ns).text)
               for l in laps if l.find(".//t:MaximumHeartRateBpm/t:Value",ns) is not None]
    avg_hr = sum(avg_hrs)/len(avg_hrs) if avg_hrs else None
    max_hr = max(max_hrs) if max_hrs else None

    pts, prev_elev, elev_gain = [], None, 0.0
    for tp in root.findall(".//t:Trackpoint", ns):
        t = tp.find("t:Time", ns)
        pos = tp.find("t:Position", ns)
        ele = tp.findtext("t:AltitudeMeters", namespaces=ns)
        if pos is None or t is None: continue
        lat = float(pos.find("t:LatitudeDegrees",ns).text)
        lon = float(pos.find("t:LongitudeDegrees",ns).text)
        pts.append([lon, lat])
        if ele is not None:
            e = float(ele)
            if prev_elev is not None and e > prev_elev:
                elev_gain += e - prev_elev
            prev_elev = e

    avg_pace_s = (total_time/(distance/1000)) if distance else None
    meta.update({
        "start_time":      laps[0].get("StartTime"),
        "duration_s":      total_time,
        "distance_m":      distance,
        "calories":        calories,
        "avg_hr":          round(avg_hr,1) if avg_hr else None,
        "max_hr":          max_hr,
        "elevation_gain_m":round(elev_gain,1),
        "avg_pace_s":      round(avg_pace_s,1) if avg_pace_s else None
    })
    return meta, pts

# ── PARSE GPX ───────────────────────────────────────────────────────────────────
def parse_gpx(path):
    raw_bytes = open(path, "rb").read()
    text = raw_bytes.decode("utf-8", errors="replace")
    gpx = gpxpy.parse(text)

    pts = []
    for tr in gpx.tracks:
        for seg in tr.segments:
            for p in seg.points:
                pts.append([p.longitude, p.latitude])

    start = gpx.tracks[0].segments[0].points[0].time
    distance = gpx.length_2d()
    raw_sport = "Running" if "run" in path.lower() else "Biking"
    meta = {
        "activityId":      os.path.basename(path),
        "sport":           normalize_sport(raw_sport),
        "start_time":      start.isoformat(),
        "duration_s":      None,
        "distance_m":      distance,
        "calories":        None,
        "avg_hr":          None,
        "max_hr":          None,
        "elevation_gain_m":None,
        "avg_pace_s":      None
    }
    return meta, pts

# ── PARSE FIT / FIT.GZ ──────────────────────────────────────────────────────────
def parse_fit(path, compressed=False):
    fobj = gzip.open(path, "rb") if compressed else open(path, "rb")
    fit = FitFile(fobj)
    pts = []
    meta = {k: None for k in [
        "activityId","sport","start_time","duration_s","distance_m",
        "calories","avg_hr","max_hr","elevation_gain_m","avg_pace_s","cadence"
    ]}
    meta["activityId"] = os.path.basename(path)

    hr_samples, prev_elev, elev_gain = [], None, 0.0
    for msg in fit.get_messages("record"):
        vals = msg.get_values()
        lat, lon = vals.get("position_lat"), vals.get("position_long")
        if lat and lon:
            pts.append([lon*(180/(2**31)), lat*(180/(2**31))])
        ts = vals.get("timestamp")
        if ts and not meta["start_time"]:
            meta["start_time"] = ts.isoformat()
        if vals.get("distance"):
            meta["distance_m"] = vals.get("distance")
        if vals.get("heart_rate"):
            hr_samples.append(vals.get("heart_rate"))
        ele = vals.get("enhanced_altitude") or vals.get("altitude")
        if ele is not None:
            if prev_elev is not None and ele>prev_elev:
                elev_gain += ele-prev_elev
            prev_elev = ele
        if vals.get("cadence"):
            meta["cadence"] = vals.get("cadence")

    for msg in fit.get_messages("session"):
        v = msg.get_values()
        if v.get("sport") and not meta["sport"]:
            meta["sport"] = normalize_sport(v.get("sport"))
        if v.get("total_elapsed_time") and not meta["duration_s"]:
            meta["duration_s"] = v.get("total_elapsed_time")
        if v.get("total_calories") and not meta["calories"]:
            meta["calories"] = v.get("total_calories")

    if hr_samples:
        meta["avg_hr"] = round(sum(hr_samples)/len(hr_samples),1)
        meta["max_hr"] = max(hr_samples)
    if elev_gain:
        meta["elevation_gain_m"] = round(elev_gain,1)
    if meta.get("distance_m") and meta.get("duration_s"):
        meta["avg_pace_s"] = round(meta["duration_s"]/ (meta["distance_m"]/1000),1)

    fobj.close()
    return meta, pts

# ── LOAD JSON METADATA ─────────────────────────────────────────────────────────
def load_json_meta(path):
    data = json.load(open(path))
    meta = {**data}
    # ensure sport normalized
    meta["sport"] = normalize_sport(meta.get("sport"))
    # ensure all fields exist
    for k in ["duration_s","distance_m","calories",
              "avg_hr","max_hr","elevation_gain_m",
              "avg_pace_s","cadence"]:
        meta.setdefault(k, None)
    return meta, None

# ── MAIN PROCESS ────────────────────────────────────────────────────────────────
features = []
index    = []
for fname in os.listdir(RAW_DIR):
    lower = fname.lower()
    path  = os.path.join(RAW_DIR, fname)
    try:
        if   lower.endswith(".tcx"):      m, pts = parse_tcx(path)
        elif lower.endswith(".gpx"):      m, pts = parse_gpx(path)
        elif lower.endswith(".fit"):      m, pts = parse_fit(path, compressed=False)
        elif lower.endswith(".fit.gz"):   m, pts = parse_fit(path, compressed=True)
        elif lower.endswith(".json"):     m, pts = load_json_meta(path)
        else: continue
        index.append(m)
        if pts:
            features.append({
                "type": "Feature",
                "geometry": {"type":"LineString","coordinates":pts},
                "properties": m
            })
    except Exception as e:
        print(f"Failed to parse {fname}: {e}")
with open(OUT_INDEX,   "w") as f: json.dump(index, f, indent=2)
with open(OUT_GEOJSON, "w") as f:
    json.dump({"type":"FeatureCollection","features":features}, f, indent=2)
print(f"✅ Wrote {len(index)} metadata entries and {len(features)} geo features.")
