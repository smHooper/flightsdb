"""
Query a PostGIS database and either write the results to a file or return in memory as GeoDataFrame.

Usage:
    query_tracks.py <connection_txt> <start_date> <end_date> [--table=<str>] [--start_time=<str>] [--end_time=<str>] [--bbox=<str>] [--mask_file=<str>] [--mask_buffer_distance=<int>] [--clip_output] [--output_path=<str>] [--aircraft_info] [--sql_criteria=<str>]

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
    -b, --bbox=<str>                    Bounding box coordinates to query records within in the format
                                        (xmin, ymin, xmax, ymax)
    -m, --mask_file=<str>               Path to a vector file (Point, Line, or Polygon) to spatially filter query
                                        results. File extension must be either .geojson, .json, .shp, .csv,  or .gpx.
                                        If you give a Point or Line vector file, you must also specify a
                                        mask_buffer_distance.
    -d, --mask_buffer_distance=<int>    Integer distance in meters (as measured in Alaska Albers Equal Area Conic
                                        projection) to buffer around all features in mask_file.
    -c, --clip_output                   Option to specify that the result should be the intersection of mask_file and
                                        the result of the non-spatial query criteria. If this option is not given, all
                                        features that touch mask_file will be returned, but they will not be clipped
                                        to its shape
    -o, --output_path=<str>             Path to write the result to.
    -a, --aircraft_info                 Option to return information about the aircraft (manufacturer, model,
                                        engine model, aircraft type, etc.) append to each row of the query result
    -q, --sql_criteria=<str>            Additional SQL criteria to append to a WHERE statement (e.g.,
                                        'flights.id IN (104, 105, 106)' to limit results to records with those
                                        flight IDs)
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

FIONA_DRIVERS = {'.geojson': 'GeoJSON',
                 '.json': 'GeoJSON',
                 '.shp': 'ESRI Shapefile',
                 '.csv': 'CSV',
                 '.gpx': 'GPX'}

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
    if (xmin < -90) or (ymin < -180) or (xmax > 90) or (ymax > 180):
        raise ValueError(error_msg.format(reason="x coordinates must be between -90 and 90 and y coordinates must be "
                                                 "between -180 and 180"))
    if (xmin < -169) or (ymin < 51) or (xmax > -129) or (ymax > 72):
        warnings.warn('The bounding box given is outside of mainland Alaska')


def get_mask_wkt(mask_gdf, buffer_distance=None):
    '''
    Get a Well-Known Text string from a
    :param mask_gdf: GeoDataframe from the vector mask file
    :param buffer_distance: distance in meters to buffer around the mask
    :return:
    '''
    mask_gdf['dissolve_field'] = 1
    if buffer_distance:
        ak_albers_mask = mask_gdf.to_crs(epsg=3338)
        mask_gdf.geometry = ak_albers_mask.buffer(buffer_distance).to_crs(epsg=4326)
    elif not (mask_gdf.geom_type == 'Polygon').all():
        raise ValueError("If specifying a mask_file, all features must either have a Polygon geometry type or "
                         "you must specify a mask_buffer_distance with either a Point or Line mask_file")
    mask_wkt = mask_gdf.dissolve(by='dissolve_field').squeeze()['geometry'].wkt

    return mask_wkt


def query_tracks(start_date, end_date, connection_txt=None, engine=None, table='flight_points', start_time='00:00', end_time='23:59', bbox=None, mask=None, mask_buffer_distance=None, clip_output=False, aircraft_info=False, sql_criteria=''):

    if not engine:
        if not connection_txt:
            raise ValueError('You must either specify an SQLAalchemy Engine or connection_txt to connect to the database')
        engine = db_utils.connect_db(connection_txt)

    with engine.connect() as conn, conn.begin():
        query_columns = pd.Series(
            ['flights.' + c for c in db_utils.get_db_columns('flights', engine)
             if c not in ['source_file', 'time_submitted']] + \
            [f'{table}.{c}' for c in db_utils.get_db_columns(table, engine)
             if c not in ['flight_id', 'id']])
        if aircraft_info:
            query_columns = query_columns.append(
                ['aircraft_info.' + c for c in db_utils.get_db_columns('aircraft_info', engine)])

    mask_specified = isinstance(mask, gpd.geodataframe.GeoDataFrame)
    if mask_specified:
        # Make sure the mask is in WGS84 (same as database features)
        if not mask.crs['init'] == 'epsg:4326':
            mask = mask.to_crs(epsg='4326')

        mask_wkt = get_mask_wkt(mask, mask_buffer_distance)
        if clip_output:
            query_columns.replace(
                {f'{table}.geom': "ST_Intersection(geom, ST_GeomFromText('%s', 4326)) AS geom" % mask_wkt},
                inplace=True)
            if table == 'flight_points':
                warnings.warn("You specified clip_output=True, but you're querying the flight_points table. This will "
                              "take much longer and yield the same result as specifying a mask with clip_output=False "
                              "(the default)")
        if bbox:
            warnings.warn('You specified both a mask_file and a bbox, but only the mask_file will be used to filter results')
    elif clip_output:
        warnings.warn('clip_output was set to True, but you did not specify a mask_file to spatially filter query results.')

    # Compose the SQL
    if start_time and end_time:
        # If the user is getting points, select points by their timestamp
        if table == 'flight_points':
            time_clause = f"flight_points.ak_datetime::time >= '{start_time}' AND " \
                          f"flight_points.ak_datetime::time <= '{end_time}'"
        # Otherwise, the user is getting lines, so the only timestamps are the departure and landing times
        else:
            time_clause = f"departure_datetime::time >= '{start_time}' AND landing_datetime::time <= '{end_time}'"
    else:
        time_clause = ''

    sql = '''SELECT {columns} FROM {table}
          INNER JOIN flights ON flights.id = {table}.flight_id 
          {aircraft_join}
          WHERE 
             {date_field}::date BETWEEN '{start_date}' AND '{end_date}' AND 
             {time_clause}
             {bbox_criteria}
             {spatial_filter}
             {other_criteria}'''\
        .format(columns=', '.join(query_columns),
                table=table,
                aircraft_join="INNER JOIN aircraft_info ON aircraft_info.registration = flights.registration" if aircraft_info else '',
                date_field="ak_datetime" if table == 'flight_points' else "departure_datetime",
                time_clause=time_clause if start_time and end_time else '',
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                bbox_criteria=f" AND ST_Intersects(ST_MakeEnvelope({bbox}, 4326), geom)" if bbox else "",
                spatial_filter=f" AND ST_Intersects(geom, ST_GeomFromText('{mask_wkt}', 4326))" if mask_specified and not clip_output else '',
                other_criteria=" AND " + sql_criteria if sql_criteria else ''
                )

    with engine.connect() as conn, conn.begin():
        data = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom')

    # Clipping will return null geometries if other SQL criteria would have returned additional features, so remove those empty geometries
    data = data.loc[~data.geometry.is_empty]

    return data


def main(connection_txt, start_date, end_date, table='flight_points', start_time='00:00', end_time='23:59', bbox=None, mask_file=None, mask_buffer_distance=None, clip_output=False, output_path=None, aircraft_info=False, sql_criteria=''):

    if output_path:
        _, path_extension = os.path.splitext(output_path)
        if path_extension not in FIONA_DRIVERS:
            supported_ext = sorted(FIONA_DRIVERS.keys())
            raise ValueError('Unsupported output file type: {extension}. File extension must be either {type_str}'
                             .format(extension=path_extension,
                                     type_str='%s, or %s' % (', '.join(supported_ext[:-1]), supported_ext[-1])
                                     )
                             )
    # If a mask file is given, get the Well-Known Text representation to feed to the query
    mask = None
    if mask_file:
        # Check if the file exists
        if not os.path.isfile(mask_file):
            raise ValueError('mask_file does not exist or is not a file: %s' % mask_file)

        # Check that the file can be read
        _, path_extension = os.path.splitext(mask_file)
        if path_extension not in FIONA_DRIVERS:
            supported_ext = sorted(FIONA_DRIVERS.keys())
            raise ValueError('Unsupported mask_file type: {extension}. File extension must be either {type_str}'
                             .format(extension=path_extension,
                                     type_str='%s, or %s' % (', '.join(supported_ext[:-1]), supported_ext[-1])
                                     )
                             )
        # Make a multi-feature geometry from the mask_file
        mask = gpd.read_file(mask_file).to_crs(epsg=4326)


    engine = db_utils.connect_db(connection_txt)
    data = query_tracks(start_date, end_date, engine=engine, table=table, start_time=start_time, end_time=end_time,
                        bbox=bbox, mask=mask, mask_buffer_distance=mask_buffer_distance, clip_output=clip_output,
                        aircraft_info=aircraft_info, sql_criteria=sql_criteria)

    if output_path:
        datetime_columns = data.columns[data.dtypes == 'datetime64[ns]']
        # convert all datetime cols to str because fiona (underlying GeoPandas) freaks out about datetimes
        for c in datetime_columns:
            data[c] = data[c].astype(str)
        data.to_file(output_path, driver=FIONA_DRIVERS[path_extension])

    return data


if __name__ == '__main__':

    args = get_cl_args(__doc__)
    sys.exit(main(**args))

