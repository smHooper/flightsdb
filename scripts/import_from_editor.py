import os
import sys
import json
import warnings
import pandas as pd
import numpy as np
import geopandas as gpd
from sqlalchemy import create_engine

import import_track
import db_utils

pd.set_option('display.max_columns', None)


def main(geojson_path, track_info_json, config_json, ignore_duplicates=False):
    # def main(geojson_str, track_info_str, config_json):
    with open(config_json) as j:
        params = json.load(j)

    # duplicate points get created when a segment is split and the only thing distinguishing 
    #   them is the mapID, a value created by the track editing app, so sort by both timestamp
    #   and mapID
    gdf = gpd.read_file(geojson_path)\
        .sort_values(['ak_datetime', 'mapID']) 
    with open(track_info_json) as j:
        track_info = json.load(j)

    connection_info = params['db_credentials']['tracks']
    engine = create_engine('postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'
                           .format(**connection_info))
    flight_columns = db_utils.get_db_columns('flights', engine)

    # assign constants (that will eventually wind up in the fights table) to gdf
    for k, v in track_info.items():
        if k in flight_columns.values and k not in gdf:
            gdf[k] = v
    gdf['registration'] = track_info['registration']
    gdf['agol_global_id'] = track_info['globalid']

    gdf.drop(columns=['departure_datetime', 'id'], inplace=True)
    for time_column in gdf.columns[gdf.columns.str.endswith('time')]:
        gdf[time_column] = pd.to_datetime(gdf[time_column])

    if 'nps_mission_code' in gdf:
        gdf.nps_mission_code = gdf.nps_mission_code.replace('', np.nan)

    # get flight ID again since the track segments could have been edited
    import_params = params['import_params'] if 'import_params' in params else {}
    seg_time_diff = import_params['seg_time_diff'] if 'seg_time_diff' in import_params else 15
    gdf = import_track.get_flight_id(gdf, seg_time_diff)

    import_track.import_data(engine=engine, data=gdf, path=track_info['name'], ignore_duplicate_flights=ignore_duplicates, called_from_editor=True, **import_params)


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
