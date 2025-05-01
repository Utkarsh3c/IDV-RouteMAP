#!/usr/bin/env python3
"""
preprocess_tcx.py

Reads raw .tcx files from "raw/" folder, extracts GPS track and rich metadata,
and writes:

  geojson/            – individual .geojson per activity
  metadata/           – individual .json per activity
  segments.geojson    – merged FeatureCollection of all activities
  activity_index.json – merged list of metadata for all activities

Usage:
  python preprocess_tcx.py
"""

import os
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from shapely.geometry import LineString, mapping
import numpy as np

def parse_tcx(file_path):
    ns = {'tcx': 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2'}
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Sport from Activity tag
    activity = root.find('.//tcx:Activity', ns)
    sport = activity.attrib.get('Sport', 'Other') if activity is not None else 'Other'

    coords, hrs, alts, cads = [], [], [], []
    total_dist = total_sec = total_cal = 0.0
    start_time = None

    laps = root.findall('.//tcx:Lap', ns)
    for lap in laps:
        if start_time is None:
            start_time = lap.attrib.get('StartTime')
        total_dist += float(lap.findtext('tcx:DistanceMeters', '0', ns))
        total_sec  += float(lap.findtext('tcx:TotalTimeSeconds', '0', ns))
        total_cal  += float(lap.findtext('tcx:Calories', '0', ns))

        for tp in lap.findall('.//tcx:Trackpoint', ns):
            lat = tp.findtext('tcx:Position/tcx:LatitudeDegrees', None, ns)
            lon = tp.findtext('tcx:Position/tcx:LongitudeDegrees', None, ns)
            if lat and lon:
                coords.append((float(lon), float(lat)))
                hr = tp.findtext('tcx:HeartRateBpm/tcx:Value', None, ns)
                if hr:  hrs.append(int(hr))
                alt = tp.findtext('tcx:AltitudeMeters', None, ns)
                if alt: alts.append(float(alt))
                cad = tp.findtext('tcx:Cadence', None, ns)
                if cad: cads.append(int(cad))

    avg_hr = int(np.mean(hrs)) if hrs else None
    max_hr = int(np.max(hrs)) if hrs else None
    elevation_gain = float(np.sum(np.diff(alts)[np.diff(alts) > 0])) if len(alts) > 1 else None
    avg_cadence = int(np.mean(cads)) if cads else None
    avg_pace_s = (total_sec / (total_dist / 1000)) if total_dist > 0 else None

    return {
        'activityId': file_path.stem,
        'start_time': start_time,
        'sport': sport,
        'distance_m': total_dist,
        'duration_s': total_sec,
        'avg_hr': avg_hr,
        'max_hr': max_hr,
        'avg_pace_s': avg_pace_s,
        'elevation_gain_m': elevation_gain,
        'cadence': avg_cadence,
        'calories': total_cal,
        'coordinates': coords
    }

def main():
    raw_dir = Path('raw')
    geojson_dir = Path('geojson')
    metadata_dir = Path('metadata')

    geojson_dir.mkdir(exist_ok=True)
    metadata_dir.mkdir(exist_ok=True)

    segments = []
    index = []

    for tcx_file in raw_dir.glob('*.tcx'):
        data = parse_tcx(tcx_file)
        if len(data['coordinates']) < 2:
            continue

        # Create GeoJSON feature
        feat = {
            'type': 'Feature',
            'geometry': mapping(LineString(data['coordinates'])),
            'properties': {'activityId': data['activityId']}
        }

        # Write individual geojson
        with open(geojson_dir / f"{data['activityId']}.geojson", 'w') as f:
            json.dump(feat, f)

        # Prepare and write metadata
        meta = {k: v for k, v in data.items() if k != 'coordinates'}
        with open(metadata_dir / f"{data['activityId']}.json", 'w') as f:
            json.dump(meta, f)

        segments.append(feat)
        index.append(meta)

    # Write merged outputs
    with open('segments.geojson', 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': segments}, f)
    with open('activity_index.json', 'w') as f:
        json.dump(index, f)

    print(f"Processed {len(segments)} activities.")
    print("Outputs written to segments.geojson and activity_index.json")

if __name__ == '__main__':
    main()
