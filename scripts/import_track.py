"""
Import a flight track into a spatial database. Accepted file types are GPX, Garmin GDB, and CSV (that come from either GSAT, Spyder tracks, AFF, or Temsco files).

Usage:
    import_track.py <connection_txt> <track_path>  [--seg_time_diff=<int>] [--min_point_distance=<int>] [--registration=<str>] [--submission_method=<str>] [--operator_code=<str>] [--aircraft_type=<str>] [--email_credentials_txt=<str>] [--log_file=<str>]
    import_track.py <connection_txt> --show_operators

Examples:
    python import_track.py connection_info.txt "T:\ResMgmt\Users\sam_h\proj\overflights\sample_data\2019.03.08_N709M_16N_4_edited.gpx" -r N709M -o NPS


Required parameters:
    connection_txt      Path of a text file containing information to connect to the DB. Each line
                        in the text file must be in the form 'variable_name; variable_value.'
                        Required variables: username, password, ip_address, port, db_name.
    track_path          Path of the track file to import.

Options:
    -h, --help                      Show this screen.
    --seg_time_diff=<int>           Minimum time in minutes between two points in a track file indicating the start of
                                    a new track segment [default: 15]
    -d, --min_point_distance=<int>  Minimum distance in meters between consecutive track points to determine unique
                                    vertices. Any points that are less than this distance from and have the same 
                                    timestamp as the preceeding point will be removed. [default: 200]
    -r, --registration=<str>        Tail (N-) number of the aircraft
    -o, --operator_code=<str>       Three digit code for the operator of the aircraft. All administrative flights
                                    should submitted with the code NPS
    -m, --submission_method=<str>   Method used for submission. This parameter should not be given when manually
                                    importing tracks. It's purpose is to distinguish manual vs. automated submissions.
    -t, --aircraft_type=<str>       The model name of the aircraft
    --email_credentials_txt=<str>   Path of a text file containing email username and password for sending
    -s, --show_operators            Print all available operator names and codes to the console
"""



import sys, os
import re
import pytz
import math
import pyproj
import subprocess
import smtplib
import docopt
import numpy as np
import pandas as pd
import gdal # this import is unused, but for some reason geopandas (shapely, actually) won't load unless gdal is imported first
import geopandas as gpd
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import LineString as shapely_LineString, Point as shapely_Point

import db_utils
import process_emails


CSV_INPUT_COLUMNS = {'aff': ['Registration', 'Longitude', 'Latitude', 'Speed (kts)',	'Heading (True)', 'Altitude (FT MSL)', 'Fix', 'PDOP', 'HDOP', 'posnAcquiredUTC', 'posnAcquiredUTC -8', 'usageType', 'source', 'Latency (Sec)'],
                    'gsat': ['Asset', 'IMEI/Unit #/Device ID', 'Device', 'Positions', 'Events', 'Messages', 'Alerts'],
                    'spy': ['Registration', 'DateTime(UTC)', 'DateTime(Local)', 'Latitude', 'Latitude(degrees)', 'Latitude(minutes)', 'Latitude(seconds)', 'Latitude(decimal)', 'Longitude', 'Longitude(degrees)', 'Longitude(minutes)', 'Longitude(seconds)', 'Longitude(decimal)', 'Altitude(Feet)', 'Speed(knots)', 'Bearing', 'PointType', 'Description'],
                    'tms': ['Serial No.', ' UTC', ' Latitude', ' HemNS', ' Longititude', ' HemEW', ' Knots', ' Heading', ' Altitude (m)', ' HDOP', ' New Conn', ' Entered', ' Event']
               }

CSV_OUTPUT_COLUMNS = {'aff': {'Registration':       'registration',
                              'Longitude':          'longitude',
                              'Latitude':           'latitude',
                              'Speed (kts)':        'knots',
                              'Heading (True)':     'heading',
                              'Altitude (FT MSL)':  'altitude_ft',
                              'posnAcquiredUTC':    'utc_datetime',
                              'posnAcquiredUTC -8': 'ak_datetime',
                              'posnAcquiredUTC (8)':'ak_datetime',
                              'posnAcquiredUTC 0':  'ak_datetime',
                              'DateTime Local':     'ak_datetime'
                              },
                      'gsat': { },
                      'spy': {'Registration':       'registration',
                              'DateTime(UTC)':      'utc_datetime',
                              'DateTime(Local)':    'ak_datetime',
                              'Latitude(decimal)':  'latitude',
                              'Longitude(decimal)': 'longitude',
                              'Altitude(Feet)':     'altitude_ft',
                              'Bearing':            'heading'
                              },
                      'tms': {'UTC':                'utc_datetime',
                              'Latitude':           'latitude',
                              'Longititude':        'longitude',
                              'Longitude (DDMM.MMMM)': 'longitude',
                              'Longititude (DDMM.MMMM)': 'longitude',
                              'Knots':              'knots',
                              'Heading':            'heading'
                              }
                       }
ERROR_EMAIL_ADDRESSES = ['samuel_hooper@nps.gov']


def calc_bearing(lat1, lon1, lat2, lon2):
    '''
    Calculate bearing from two lat/lon coordinates. Logic from https://gist.github.com/jeromer/2005586

    :return: integer compass bearing (between 0-360°)
    '''
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)

    longitude_diff = math.radians(lon2 - lon1)

    x = math.sin(longitude_diff) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - \
        (math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(longitude_diff))

    initial_bearing = math.degrees(math.atan2(x, y))

    # math.atan2 returns values from -180° to + 180°, so convert to 0-360
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def calc_distance_to_last_pt(gdf):

    in_proj = pyproj.Proj(init='epsg:4326')
    out_proj = pyproj.Proj(init='epsg:3338') # Alaska Albers Equal Area, which is pretty good at preserving distances
    gdf['x_albers'], gdf['y_albers'] = pyproj.transform(in_proj, out_proj, gdf.longitude.values, gdf.latitude.values)
    distance = (gdf.x_albers.diff()**2 + gdf.y_albers.diff()**2)**0.5 # distance between 2 points

    return distance


def read_gpx(path, seg_time_diff=15):

    gdf = gpd.read_file(path, layer='track_points')#, geometry='geometry')

    # Convert to datetime. It's almost in the right format to be read automatically except there's a T instead of a
    #   space between the date and the time
    gdf['utc_datetime'] = pd.to_datetime(gdf.time)

    # Make points 3D
    gdf['altitude_ft'] = (gdf.ele * 3.2808399).astype(int)
    gdf.geometry = gdf.apply(lambda row: shapely_Point(row.geometry.x, row.geometry.y, row.altitude_ft), axis=1)

    gdf['longitude'] = gdf.geometry.x
    gdf['latitude'] = gdf.geometry.y

    # Calculate speed and bearing because GPX files don't have it
    gdf.sort_values(by='utc_datetime', inplace=True)
    gdf['diff_m'] = calc_distance_to_last_pt(gdf)
    gdf['diff_seconds'] = gdf.utc_datetime.diff().dt.seconds
    gdf.loc[gdf.diff_seconds.isnull() | (gdf.diff_seconds == 0) | (gdf.diff_seconds > seg_time_diff * 60), 'diff_seconds'] = -1
    gdf['m_per_sec'] = gdf.diff_m / gdf.diff_seconds
    gdf['knots'] = (gdf.m_per_sec * 1.94384).fillna(-1).round().astype(int)# 1m/s == 1.94384 knots
    gdf.loc[(gdf.knots < 0) | gdf.knots.isnull(), 'knots'] = 0

    gdf['previous_lat'] = gdf.shift().latitude
    gdf['previous_lon'] = gdf.shift().longitude
    gdf['heading'] = gdf.apply(lambda row:
                                    calc_bearing(*row[['previous_lat', 'previous_lon', 'latitude', 'longitude']]),
                               axis=1)\
        .fillna(-1).round().astype(int)

    return gdf


def read_gdb(path, seg_time_diff=None):
    '''
    Convert GDB to GPX, then just use read_gpx() function
    '''

    stderr_path = os.path.join(os.path.dirname(path), 'stderr.txt')
    out_path = path.replace('.gdb', '.gpx')
    error_message, result_code = '', None  # initialize in case there's an error before assigning within *with* block
    with open(stderr_path, 'wb+') as stderr:
        result_code = subprocess.call('gpsbabel -t -i gdb,via=1 -f "{in_path}" -o gpx -F "{out_path}"'
                                      .format(in_path=path, out_path=out_path),
                                      shell=True, stderr=stderr)
        stderr.seek(0)
        error_message = stderr.read()

    # Clean up the text file that contained the error message (if there was one)
    if os.path.isfile(stderr_path):
        os.remove(stderr_path)

    # If the error message isn't blank, the conversion failed
    if len(error_message) and result_code != 0:
        raise IOError(error_message)

    return read_gpx(out_path)


def format_aff(path):

    df = pd.read_csv(path, encoding='ISO-8859-1')
    df.rename(columns=CSV_OUTPUT_COLUMNS['aff'], inplace=True)

    df.utc_datetime = pd.to_datetime(df.utc_datetime, errors='coerce')

    return df


def dms_to_dd(degrees, minutes, seconds, direction):
    ''' Convert coordinate in degrees minutes seconds to decimal degrees'''
    decimal_degrees = float(degrees) + float(minutes)/60 + float(seconds)/(3600)

    if direction in ['S', 'W']:
        decimal_degrees *= -1

    return decimal_degrees


def parse_gsat_coordinates(coordinates):
    '''GSAT coordinates are in the format 62°37'21.9600"N 150°44'42.0000"W in a single Lat/Lng field'''

    # Make lists of [degress, min, sec, direction] for both lat and lon
    lat_dms, lon_dms = [re.split('[°\'"]+', c) for c in coordinates.split()]

    return dms_to_dd(*lat_dms), dms_to_dd(*lon_dms)


def format_gsat(path):

    # Try to get the registration number. It's stored in the third row, even though the metadata header is the first row
    try:
        registration = 'N' + pd.read_csv(path, encoding='ISO-8859-1').loc[2, 'Asset']
    except:
        registration = ''

    df = pd.read_csv(path, encoding='ISO-8859-1', skiprows=5)

    # If this didn't fail, add a registration column
    if registration:
        df['registration'] = registration

    # Convert coordinates to separate decimal degree lat and lon fields
    df['latitude'], df['longitude'] = list(zip(*df['Lat/Lng'].apply(parse_gsat_coordinates)))

    df['knots'] = df['Speed'].str.split(' ').str[0].astype(float) # Replace "knots" in speed field
    df['heading'] = df['Heading'].str.replace('°', '').astype(int) # Replace degree symbol in heading
    df['altitude_ft'] = df['Altitude'].str.split(' ').str[0].astype(float).round().astype(int) # Replace "ft" and convert to int
    df['utc_datetime'] = pd.to_datetime(df['Date'], errors='coerce')

    return df


def format_spy(path):

    df = pd.read_csv(path, encoding='ISO-8859-1')

    if 'Speed(mph)' in df:
        df['knots'] = df['Speed(mph)'] / 1.151
    elif 'Speed(knots)' in df:
        df['knots'] = df['Speed(knots)']
    else:
        raise KeyError('"Speed" column for file %s not found. Expected either "Speed(mph)" or "Speed(knots)" but columns in this file are:\n\t-%s' % (path, '\n\t-'.join(df.columns.sort_values)))

    df.rename(columns=CSV_OUTPUT_COLUMNS['spy'], inplace=True)
    expected_lat_fields = [k for k, v in CSV_OUTPUT_COLUMNS.items() if v == 'latitude']
    expected_lon_fields = [k for k, v in CSV_OUTPUT_COLUMNS.items() if v == 'longitude']
    try:
        df[['latitude', 'longitude']] = df[['latitude', 'longitude']].astype(float)
    except KeyError as e:
        raise KeyError('No latitude and/or longitude fields found. Expected one of {lat_fields} for latitude fields and'
                       ' {lon_fields for longitude fields. Input fields were:\n\t-{columns}'
                       .format(lat_fields=expected_lat_fields, lon_fields=expected_lon_fields, columns='\n\t-'.join(df.columns))
                       )

    df.altitude_ft = df.altitude_ft.astype(int)
    df.utc_datetime = pd.to_datetime(df.utc_datetime, errors='coerce')

    return df


def format_tms(path):

    df = pd.read_csv(path, encoding='ISO-8859-1')

    # some of the time TMS columns names have spaces on either end
    df.columns = df.columns.str.strip()
    df.rename(columns=CSV_OUTPUT_COLUMNS['tms'], inplace=True)

    # Lat and lon are (annoyingly) in the format DDMM.MMMM without any separator between degrees and minutes, so attempt
    #   to parse them
    try:
        coefficient = df['HemNS'].apply(lambda x: 1 if x == 'N' else -1)
        df['latitude'] = (df['latitude'].astype(str).str[:2].astype(float) + df['latitude'].astype(str).str[2:].astype(float)/60) * coefficient
    except Exception as e:
        raise AttributeError('Could not parse latitude field for %s because of %s' % (path, e))
    try:
        coefficient = df['HemEW'].apply(lambda x: -1 if x == 'W' else 1)
        df['longitude'] = (df['longitude'].astype(str).str[:3].astype(float) + df['longitude'].astype(str).str[3:].astype(float)/60) * coefficient
    except Exception as e:
        raise AttributeError('Could not parse longitude field for %s because of %s' % (path, e))

    df['altitude_ft'] = (df['Altitude (m)'] * 3.2808399).astype(int)
    df['utc_datetime'] = pd.to_datetime(df['utc_datetime'], errors='coerce')

    return df


def read_csv(path, seg_time_diff=None):
    """
    Read and format a CSV of track data. CSVs can from from 4 different sources, so figure out which source it comes
    from and format accordingly

    :param path: path to track CSV
    :return: GeoDataframe of points
    """

    df = pd.read_csv(path, encoding='ISO-8859-1')#ISO encoding handles symbols like '°'

    # Figure out which file type it is (aff, gsat, spy, or tms) by selecting the file type that most closely matches
    #   the expected columns per type
    named_columns = df.columns[~df.columns.str.startswith('Unnamed')].str.strip()
    column_match_scores = {file_type: len(named_columns[named_columns.isin(pd.Series(columns).str.strip())]) / float(len(named_columns)) for
                           file_type, columns in CSV_INPUT_COLUMNS.items()}
    best_match = pd.Series(column_match_scores).idxmax() #index is file_type

    CSV_FUNCTIONS = {'aff': format_aff,
                     'gsat': format_gsat,
                     'spy': format_spy,
                     'tms': format_tms
                     }
    if not best_match in CSV_FUNCTIONS:
        sorted_types = sorted(CSV_FUNCTIONS.keys())
        raise IOError('The data source could not be interpreted from the column names. Only %s, and %s currently accepted.' % (sorted_types[:-1], sorted_types[-1]))
    df = CSV_FUNCTIONS[best_match](path)
    df.heading = df.heading.round().astype(int)
    df.knots = df.knots.round().astype(int)

    # Make the dataframe into a geodataframe of points
    geometry = df.apply(lambda row: shapely_Point(row.longitude, row.latitude, row.altitude_ft), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf['diff_m'] = calc_distance_to_last_pt(gdf)

    return gdf


def get_flight_id(gdf, seg_time_diff):
    '''
    Assign a unique ID to each line segment. IDs use the registation (i.e. N-number) and timestamp like
    <registration>_yyyymmddhhMM. All columns are assigned in place.
    '''
    # Calculate the difference in time between rows
    time_diff = gdf.ak_datetime.diff() / np.timedelta64(1, 'm')#.groupby(gdf['date_str']).diff() # don't groupby day in case there's an overnight flight
    # Assign a sequential integer to each different flight segment for the file. Only the start of a new flight will
    #   have a difference in time with it's previous row of more than seg_time_diff (in theory), so these rows will
    #   evaluate to True. cumsum() treats these as a 1 and False values as 0, so all rows of the same segment will have
    #   the same ID
    gdf['segment_id'] = (time_diff >= seg_time_diff).cumsum()
    #
    departure_times = gdf.groupby('segment_id').ak_datetime.min().to_frame().rename(columns={'ak_datetime': 'departure_datetime'})
    #timestamps = gdf.groupby('segment_id').ak_datetime.first().dt.floor('%smin' % seg_time_diff).dt.strftime('%Y%m%d%H%M').to_frame().rename(columns={'ak_datetime': 'departure_datetime'})
    gdf = gdf.merge(departure_times, on='segment_id')
    gdf['timestamp_str'] = gdf.departure_datetime.dt.floor('%smin' % seg_time_diff).dt.strftime('%Y%m%d%H%M')
    gdf['flight_id'] = gdf.registration + '_' + gdf.timestamp_str#gdf.date_str + '_' + segment_id.astype(str)

    return gdf


READ_FUNCTIONS = {'.gpx': read_gpx,
                  '.gdb': read_gdb,
                  '.csv': read_csv
                  }

def import_track(connection_txt, path, seg_time_diff=15, min_point_distance=200, registration='', submission_method='manual', operator_code=None, aircraft_type=None, silent=False):

    _, extension = os.path.splitext(path)

    if not extension.lower() in READ_FUNCTIONS:
        sorted_extensions = [c.replace('.', '').upper() for c in sorted(READ_FUNCTIONS.keys())]
        raise IOError('Unexpected file type found: %s. Only %s, and %s currently accepted.' %
                      (extension.replace('.', '').upper(), ', '.join(sorted_extensions[:-1]), sorted_extensions[-1]))
    # apply function from dict based on file extension
    gdf = READ_FUNCTIONS[extension.lower()](path)

    # Calculate local (AK) time
    timezone = pytz.timezone('US/Alaska')
    gdf['ak_datetime'] = gdf.utc_datetime + gdf.utc_datetime.apply(timezone.utcoffset)

    # Get unique flight_ids per line segment in place
    if not 'registration' in gdf.columns:
        gdf['registration'] = registration
    gdf = get_flight_id(gdf, seg_time_diff)\
        .drop(gdf.index[(gdf.diff_m < min_point_distance) & (gdf.utc_datetime.diff().dt.seconds == 0)])\
        .dropna(subset=['ak_datetime'])\
        .sort_values(by=['ak_datetime'])

    # Get metadata-y columns
    gdf['time_submitted'] = datetime.now()
    gdf['submission_method'] = submission_method
    if operator_code:
        gdf['operator_code'] = operator_code
    if aircraft_type:
        gdf['aircraft_type'] = aircraft_type

    engine = db_utils.connect_db(connection_txt)

    # get columns from DB tables
    flight_columns = db_utils.get_db_columns('flights', engine)
    point_columns = db_utils.get_db_columns('flight_points', engine)
    line_columns = db_utils.get_db_columns('flight_lines', engine)

    # separate flights, points, and lines
    flights = gdf[[c for c in flight_columns if c in gdf]].drop_duplicates()
    flights['submitted_by'] = os.getlogin()
    if not len(flights):
        raise ValueError('No flight segments found in this file.')

    points = gdf.copy()
    points['geom'] = gdf.geometry.apply(lambda g: WKTElement(g.wkt, srid=4326))
    points.drop(columns=points.columns[~points.columns.isin(point_columns)], inplace=True)

    line_geom = gdf.groupby('flight_id').geometry.apply(lambda g: shapely_LineString(g.to_list()))
    lines = gpd.GeoDataFrame(flights.set_index('flight_id'), geometry=line_geom)
    lines['geom'] = lines.geometry.apply(lambda g: WKTElement(g.wkt, srid=4326))
    lines['flight_id'] = lines.index
    lines.drop(columns=lines.columns[~lines.columns.isin(line_columns)], inplace=True)
    lines.index.name = None

    with engine.connect() as conn, conn.begin():
        # Insert only new flights because if there were already tracks for these flights that were already processed,
        #   the flights are already in the DB
        existing_flight_ids = pd.read_sql("SELECT flight_id FROM flights", conn).squeeze(axis=1)
        new_flights = flights.loc[~flights.flight_id.isin(existing_flight_ids)]
        new_flights.to_sql('flights', conn, if_exists='append', index=False)

        # Warn the user if any of the flights already exist
        n_flights = len(flights)
        n_new_flights = len(new_flights)
        if n_new_flights == 0:
            raise ValueError('No new flight segments were inserted from this file because they all already exist in'
                             ' the database.')
        if n_flights != n_new_flights:
            raise UserWarning('For the file {path}, the following {existing} of {total} flight segments already exist:'
                              '\n\t- {ids}'
                              .format(path=path,
                                      existing=n_flights-n_new_flights,
                                      total=n_flights,
                                      ids='\n\t-'.join(flights.loc[flights.flight_id.isin(existing_flight_ids)])
                                      )
                              )

        # Get the numeric IDs of the flights that were just inserted and insert the points and lines matching those
        #   flight IDs that were just inserted
        flight_ids = pd.read_sql("SELECT id, flight_id FROM flights WHERE flight_id IN ('%s')"
                                 % "', '".join(flights.flight_id),
                                 conn)
        points = points.merge(flight_ids, on='flight_id')
        points.loc[~points.flight_id.isin(existing_flight_ids)]\
            .drop('flight_id', axis=1) \
            .rename(columns={'id': 'flight_id'})\
            .to_sql('flight_points',
                    conn,
                    if_exists='append',
                    index=False,
                    dtype={'geom': Geometry('POINT Z', srid=4326)})
        lines = lines.merge(flight_ids, on='flight_id')
        lines.loc[~lines.flight_id.isin(existing_flight_ids)]\
            .drop('flight_id', axis=1) \
            .rename(columns={'id': 'flight_id'})\
            .to_sql('flight_lines',
                    conn,
                    if_exists='append',
                    index=False,
                    dtype={'geom': Geometry('LineStringZ', srid=4326)})

    if not silent:
        sys.stdout.write('%s flight tracks imported:\n\t-%s' % (n_new_flights, '\n\t-'.join(flight_ids.flight_id)))
        sys.stdout.flush()


def get_cl_args(doc):
    """
    Get command line arguments as a dictionary
    :return: dictionary of arguments
    """
    # Any args that don't have a default value and weren't specified will be None
    cl_args = {k: v for k, v in docopt.docopt(doc).items() if v is not None}

    # get rid of extra characters from doc string and 'help' entry
    args = {re.sub('[<>-]*', '', k): v for k, v in cl_args.items() if k != '--help' and k != '-h'}

    # convert numeric values
    for k, v in args.items():
        if type(v) == bool or v == None:
            continue
        elif re.fullmatch('\d*', v):
            args[k] = int(v)
        elif re.fullmatch('\d*\.\d*', v):
            args[k] = float(v)

    return args


def print_operator_codes(connection_txt):
    """Helper function to display operator codes in the console for the user"""

    engine = db_utils.connect_db(connection_txt)
    operator_codes = db_utils.get_lookup_table(engine, 'operators', index_col='name', value_col='code')
    operator_code_str = '\n\t-'.join(sorted(['%s: %s' % operator for operator in operator_codes.items()]))

    print('Operator code options:\n\t-%s' % operator_code_str)


def main(connection_txt, track_path, seg_time_diff=15, min_point_distance=500, registration='', submission_method='manual', operator_code=None, aircraft_type=None, email_credentials_txt=None, log_file=None):

    sys.stdout.write("Log file for %s: %s\n" % (__file__, datetime.now().strftime('%H:%M:%S %m/%d/%Y')))
    sys.stdout.write('Command: python %s\n\n' % subprocess.list2cmdline(sys.argv))
    sys.stdout.flush()

    seg_time_diff = int(seg_time_diff)
    min_point_distance = int(min_point_distance)

    if email_credentials_txt:
        sender, password = process_emails.get_email_credentials(email_credentials_txt)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.ehlo()
        server.login(sender, password)

    try:
        import_track(connection_txt, track_path, seg_time_diff, min_point_distance, registration, submission_method, operator_code,
                      aircraft_type)
    except Exception as e:
        if email_credentials_txt:
            message_body = '''There was a problem with the attached file: %s'''
            subject = 'Error occurred while processing %s' % os.path.basename(track_path)
            process_emails.send_email(message_body, subject, sender, ERROR_EMAIL_ADDRESSES, server, attachments=[track_path, log_file])
            server.close()

        # Still raise the error so it's logged
        raise e


if __name__ == '__main__':

    args = get_cl_args(__doc__)
    
    if args['show_operators']:
        sys.exit(print_operator_codes(args['connection_txt']))
    else:
        del args['show_operators']
        sys.exit(main(**args))

