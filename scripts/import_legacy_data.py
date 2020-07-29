import pyodbc
import traceback

from import_track import *

SEG_TIME_DIFF = 15
MIN_POINT_DISTANCE = 200

def main(access_db_path, connection_txt, ssl_cert):

    conn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                          r'DBQ=%s' % (access_db_path))
    cursor = conn.cursor()
    table_names = [t.table_name for t in cursor.tables() if t.table_type == 'TABLE' and t.table_name.lower().startswith('gps')]
    cursor.close()
    del cursor

    track_points = pd.concat([pd.read_sql(f'SELECT * FROM {t}', conn) for t in table_names], sort=False)
    conn.close()

    # column names in access db are the same as what import_track expects except for capitalization
    track_points = track_points.rename(columns={c: c.lower() for c in track_points.columns})\
        .rename(columns={'utc_time': 'utc_datetime', 'ak_time': 'ak_datetime'})\
        .drop(columns='id')

    track_points = gpd.GeoDataFrame(
        track_points,
        geometry=[shapely_Point(x, y, z) for x, y, z in zip(track_points.longitude, track_points.latitude, track_points.altitude_ft)]
    )

    track_points['operator_code'] = 'NPS'

    for registration, gdf in track_points.groupby('registration'):
        gdf.sort_values('ak_datetime', inplace=True)

        gdf['diff_m'] = calc_distance_to_last_pt(gdf)
        gdf['diff_seconds'] = gdf.utc_datetime.diff().dt.seconds
        gdf.loc[gdf.diff_seconds.isnull() | (gdf.diff_seconds == 0) | (gdf.diff_seconds > SEG_TIME_DIFF * 60), 'diff_seconds'] = -1
        gdf = get_flight_id(gdf, SEG_TIME_DIFF) \
            .drop(gdf.index[((gdf.diff_m < MIN_POINT_DISTANCE) & (gdf.utc_datetime.diff().dt.seconds == 0))]) \
            .dropna(subset=['ak_datetime']) \
            .sort_values(by=['ak_datetime'])

        land_times = gdf.groupby('segment_id').ak_datetime.max().to_frame().rename(columns={'ak_datetime': 'landing_datetime'}).reset_index()
        gdf = gdf.merge(land_times, on='segment_id')
        gdf['duration_hrs'] = (gdf.landing_datetime - gdf.departure_datetime).dt.seconds / 3600.0

        gdf['submission_method'] = 'legacy'
        gdf['submission_time'] = datetime.now()
        gdf['source_file'] = access_db_path
        try:
            import_data(connection_txt, data=gdf, submission_method='legacy', operator_code='NPS', ssl_cert_path=ssl_cert, ignore_duplicate_flights=True)
        except:
            print(f'\n\nFailed for {registration}. Error: {traceback.format_exc()}')

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
