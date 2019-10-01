"""
Query a PostGIS database and either write the results to a file or return in memory as GeoDataFrame.

Usage:
    query_tracks.py <connection_txt> <start_date> <end_date> [--table=<str>] [--start_time=<str>] [--end_time=<str>] [--bbox=<str>] [--mask_file=<str>] [--mask_buffer_distance=<str>] [--clip_output] [--output_path=<str>] [--aircraft_info] [--sql_criteria=<str>]

Examples:

Required parameters:
    connection_txt      Path of a text file containing information to connect to the DB. Each line
                        in the text file must be in the form 'variable_name; variable_value.'
                        Required variables: username, password, ip_address, port, db_name.
    start_date          Earliest date of interest to return data from
    end_date            Latest date of interest to return data from

Options:
    -h, --help                          Show this screen.
    -t, --table=<str>                   Table to query geometries from
    -s, --start_time=<str>              Earliest time of day on a 24-hour clock to return data from
    -e, --end_time=<str>                Latest time of day on a 24-hour clock to return data from
    -b, --bbox=<str>                    Bounding box coordinates to query records within in the format (xmin, ymin, xmax, ymax)
    -m, --mask_file=<str>               f
    -d, --mask_buffer_distance=<str>    df,.
    -c, --clip_output                   s
    -o, --output_path=<str>             s
    -a, --aircraft_info                 s
    -q, --sql_criteria=<str>            s
"""

import sys
import os
import warnings
import subprocess
import docopt
import pandas as pd
import geopandas as gpd

import db_utils
from import_track import get_cl_args

SUPPORTED_FILE_EXTENSIONS = ['.geojson', '.json', '.shp', '.csv', '.gpx']

def validate_bounding_box(bbox):
    error_msg = "bbox coordinates must be in the format 'xmin,ymin,xmax,ymax' (in WGS84) but {reason}. bbox given: %s" % bbox

    bounds = [float(c) for c in bbox.split(',')]
    if len(bounds) != 4:
        raise ValueError(error_msg.format(reason="%d bounding coordinates found" % len(bounds)))
    xmin, ymin, xmax, ymax = bounds

    if xmin >= xmax:
        raise ValueError(error_msg.format(reason="xmin was greater than or equal to xmax"))
    if ymin >= ymax:
        raise ValueError(error_msg.format(reason="ymin was greater than or equal to ymax"))
    if xmin < -90 or ymin < -180 or xmax > 90 or ymax > 180:
        raise ValueError(error_msg.format(reason="x coordinates must be between -90 and 90 and y coordinates must be "
                                                 "between -180 and 180"))
    if xmin < 129 or ymin < 51 or xmax > 169 or ymax > 72:
        warnings.warn('The bounding box given is outside of mainland Alaska')


def query_tracks(connection_txt, start_date, end_date, table='flight_points', start_time='00:00', end_time='23:59', bbox=None, mask_file=None, mask_buffer_distance=None, clip_output=False, output_path=None, aircraft_info=False, sql_criteria=''):

    if output_path:
        _, extension = os.path.splitext(output_path)
        if extension not in SUPPORTED_FILE_EXTENSIONS:
            raise ValueError('Unsupported file type: {extension}. File extension must be either {type_str}'
                             .format(extension=extension,
                                     type_str='%s, or %s' % (', '.join(SUPPORTED_FILE_EXTENSIONS[:-1]), SUPPORTED_FILE_EXTENSIONS[-1])
                                     )
                             )
    # If a mask file is given, get the Well-Known Text representation to feed to the query
    if mask_file:
        # Check if the file exists
        if not os.path.isfile(mask_file):
            raise ValueError('mask_file does not exist or is not a file: %s' % mask_file)
        # Make a multi-feature geometry from
        mask = gpd.read_file(mask_file).to_crs(epsg=4326)
        mask['dissolve_field'] = 1
        if mask_buffer_distance:
            ak_albers_mask = mask.to_crs(epsg=3338)
            mask.geometry = ak_albers_mask.buffer(mask_buffer_distance).to_crs(epsg=4326)
        elif not (mask.geom_type == 'Polygon').all():
            raise ValueError("If specifying a mask_file, all features must either have a Polygon geometry type or "
                             "you must specify a mask_buffer_distance with either a Point or Line mask_file")
        mask_wkt = mask.dissolve(by='dissolve_field').squeeze()['geometry'].wkt

        if bbox:
            warnings.warn('You specified both a mask_file and a bbox, but only the mask_file will be used to filter results')

    engine = db_utils.connect_db(connection_txt)
    with engine.connect() as conn, conn.begin():
        query_columns = pd.Series(
            ['flights.' + c for c in db_utils.get_db_columns('flights', engine)
             if c not in ['source_file', 'time_submitted']] + \
            ['{table}.{column}'.format(table=table, column=c) for c in db_utils.get_db_columns(table, engine)
             if c not in ['flight_id', 'id']])
        if aircraft_info:
            query_columns = query_columns.append(
                ['aircraft_info.' + c for c in db_utils.get_db_columns('aircraft_info', engine)])
        if mask_file and clip_output:
            query_columns.replace({'%s.geom' % table: "ST_Intersection(geom, ST_GeomFromText('%s', 4326)) AS geom" % mask_wkt}, inplace=True)
        '''# split datetime columns into date and time because fiona (underlying GeoPandas) freaks out about datetimes
        datetime_columns = []
        for c in query_columns:
            if c.endswith('datetime'):
                datetime_columns.append(c)
                query_columns = query_columns.append(
                    pd.Series(['%s::date AS %s' % (c, c.replace('datetime', 'date').split('.')[-1]),
                               '%s::time AS %s' % (c, c.replace('datetime', 'time').split('.')[-1])]
                                                               ))
        query_columns = query_columns.loc[~query_columns.isin(datetime_columns)].sort_values()'''

    # Compose the SQL
    if start_time and end_time:
        time_clause = "departure_datetime::time >= '{start_time}' AND landing_datetime::time <= '{end_time}'".format(
            start_time=start_time, end_time=end_time)
    else:
        time_clause = ''
    sql = "SELECT {columns} FROM {table} " \
          "INNER JOIN flights ON flights.id = {table}.flight_id " \
          "{aircraft_join}" \
          "WHERE " \
          "   {date_field}::date BETWEEN {start_date} AND {end_date} AND " \
          "   {time_clause}" \
          "   {bbox_criteria}" \
          "   {spatial_filter}" \
          "   {other_criteria}"\
        .format(columns=', '.join(query_columns),
                table=table,
                aircraft_join="INNER JOIN aircraft_info ON aircraft_info.registration = flights.registration" if aircraft_info else '',
                date_field="ak_datetime" if table == 'flight_points' else "departure_datetime",
                time_clause=time_clause if start_time and end_time else '',
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                bbox_criteria=" AND ST_Intersects(ST_MakeEnvelope(%s, 4326), geom)" % bbox if bbox else "",
                spatial_filter=" AND ST_Intersects(geom, ST_GeomFromText('%s', 4326))" % mask_wkt if mask_file and not clip_output else '',
                other_criteria=" AND " + sql_criteria if sql_criteria else ''
                )
    with engine.connect() as conn, conn.begin():
        data = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom')

    # Clipping will return null geometries if other SQL criteria would have returned additional features, so remove those empty geometries
    data = data.loc[~data.geometry.is_empty]

    if output_path:
        datetime_columns = data.columns[data.dtypes == 'datetime64[ns]']
        for c in datetime_columns:
            data[c] = data[c].astype(str)
        data.to_file(output_path)

if __name__ == '__main__':

    args = get_cl_args(__doc__)
    sys.exit(query_tracks(**args))

