"""
Import a flight track into a spatial database. Accepted file types are GPX, Garmin GDB, and CSV (that come from either GSAT, Spyder tracks, AFF, or Temsco files).

Usage:
    import_track.py <connection_txt> <track_path>  [--seg_time_diff=<int>] [--min_point_distance=<int>] [--registration=<str>] [--submission_method=<str>] [--ssl_cert_path=<str>] [--operator_code=<str>] [--aircraft_type=<str>] [--force_import] [--email_credentials_txt=<str>] [--log_file=<str>]
    import_track.py <connection_txt> --show_operators

Examples:
    python import_track.py connection_info.txt "T:/ResMgmt/Users/sam_h/proj/overflights/sample_data/2019.03.08_N709M_16N_4_edited.gpx" -r N709M -o NPS
    python import_track.py connection_info.txt "T:/ResMgmt/Users/sam_h/proj/overflights/sample_data/2019.03.08_N709M_16N_4_edited.gpx" --registration N709M --operator NPS

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
                                    timestamp as the preceding point will be removed. [default: 200]
    -r, --registration=<str>        Tail (N-) number of the aircraft
    -o, --operator_code=<str>       Three digit code for the operator of the aircraft. All administrative flights
                                    should submitted with the code NPS
    -c, --ssl_cert_path=<str>       Path to an SSL .crt or .pem file for sending an HTTP request to registry.faa.gov to
                                    retrieve info about the aircraft
    -m, --submission_method=<str>   Method used for submission. This parameter should not be given when manually
                                    importing tracks. It's purpose is to distinguish manual vs. automated submissions.
    -t, --aircraft_type=<str>       The model name of the aircraft
    -f, --force_import              If specified, import all data even if there are matching flight segments
                                    in the database already
    --email_credentials_txt=<str>   Path of a text file containing email username and password for sending
    -s, --show_operators            Print all available operator names and codes to the console
"""

import sys, os
import re
import pytz
import math
import random
import string
import pyproj
import shutil
import warnings
import subprocess
import smtplib
import chardet.universaldetector
import docopt
import requests
import bs4
import numpy as np
import pandas as pd
#import gdal # this import is unused, but for some reason geopandas (shapely, actually) won't load unless gdal is imported first
import geopandas as gpd
from datetime import datetime, timedelta
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import LineString as shapely_LineString, Point as shapely_Point

import db_utils
import update_aircraft_info as ainfo
import process_emails
import kml_parser
from utils import get_cl_args


# Patterns of column names for different csv sources
CSV_INPUT_COLUMNS = [
    ['aff', ['Registration', 'Longitude', 'Latitude', 'Speed (kts)', 'Heading (True)', 'Altitude (FT MSL)', 'Fix', 'PDOP', 'HDOP', 'posnAcquiredUTC', 'posnAcquiredUTC -8', 'usageType', 'source', 'Latency (Sec)']],
    ['gsat', ['Asset', 'IMEI/Unit #/Device ID', 'Device', 'Positions', 'Events', 'Messages', 'Alerts']],
    ['gsat', ['Events', 'Date', 'Address', 'Lat/Lng', 'Speed', 'Heading', 'Altitude', 'Via']],
    ['spy', ['Aircraft', 'Registration', 'Track', 'Point', 'DateTime(UTC)', 'DateTime(Local)', 'Latitude', 'Latitude(degrees)', 'Latitude(minutes)', 'Latitude(seconds)', 'Latitude(decimal)', 'Longitude', 'Longitude(degrees)', 'Longitude(minutes)', 'Longitude(seconds)', 'Longitude(decimal)', 'Altitude(Feet)', 'Altitude(ft)', 'Speed(knots)', 'Bearing', 'PointType', 'Description']],
    ['tms', ['Serial No.', 'UTC', 'Latitude', 'HemNS', 'Longititude', 'HemEW', 'Knots', 'Heading', 'Altitude (m)', 'HDOP', 'New Conn', 'Entered', 'Event', 'ESN', 'Latitude (DDMM.MMMM)', 'Longititude (DDMM.MMMM)',
       'Heading (True)', 'Server Time (PDT)']],
    ['foreflight', ['Pilot', 'Tail Number', 'Derived Origin', 'Start Latitude', 'Start Longitude', 'Derived Destination', 'End Latitude', 'End Longitude', 'Start Time', 'End Time', 'Total Duration', 'Total Distance', 'Initial Attitude Source', 'Device Model', 'Device Model Detailed', 'iOS Version', 'Battery Level', 'Battery State', 'GPS Source', 'Maximum Vertical Error', 'Minimum Vertical Error', 'Average Vertical Error', 'Maximum Horizontal Error', 'Minimum Horizontal Error', 'Average Horizontal Error', 'Imported From', 'Route Waypoints']]
]

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
                              'Aircraft':           'registration',
                              'DateTime(UTC)':      'utc_datetime',
                              'DateTime(Local)':    'ak_datetime',
                              'Latitude(decimal)':  'latitude',
                              'Longitude(decimal)': 'longitude',
                              'Altitude(Feet)':     'altitude_ft',
                              'Altitude(ft)':       'altitude_ft',
                              'Bearing':            'heading'
                              },
                      'tms': {'UTC':                'utc_datetime',
                              'Latitude':           'latitude',
                              'Longititude':        'longitude',
                              'Longitude (DDMM.MMMM)': 'longitude',
                              'Longititude (DDMM.MMMM)': 'longitude',
                              'Latitude (DDMM.MMMM)': 'latitude',
                              'Knots':              'knots',
                              'Heading':            'heading',
                              'Heading (True)':     'heading'
                              }
                       }
ERROR_EMAIL_ADDRESSES = ['samuel_hooper@nps.gov']

# Columns to use to verify that the file was read correctly
VALIDATION_COLUMNS = pd.Series(['geometry', 'utc_datetime', 'altitude_ft', 'longitude', 'latitude', 'x_albers', 'y_albers', 'diff_m', 'diff_seconds', 'm_per_sec', 'knots', 'previous_lat', 'previous_lon', 'heading'])

ARCHIVE_DIR = r'\\inpdenards\overflights\imported_files\tracks'

REGISTRATION_REGEX = r'(?i)N\d{1,5}[A-Z]{0,2}'

FEET_PER_METER = 3.2808399
M_PER_S_TO_KNOTS = 1.94384

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

    #in_proj = pyproj.Proj('epsg:4326')
    #out_proj = pyproj.Proj('epsg:3338') # Alaska Albers Equal Area, which is pretty good at preserving distances
    transformer = pyproj.Transformer.from_crs('epsg:4326', 'epsg:3338')
    # for some reason you specify .transform() with y, x but it returns x, y
    gdf['x_albers'], gdf['y_albers'] = transformer.transform(gdf.latitude.values, gdf.longitude.values)#pyproj.transform(in_proj, out_proj, gdf.longitude.values, gdf.latitude.values)
    distance = (gdf.x_albers.diff()**2 + gdf.y_albers.diff()**2)**0.5 # distance between 2 points

    return distance


def read_gpx(path, seg_time_diff=15):

    gdf = gpd.read_file(path, layer='track_points')#, geometry='geometry')

    # Convert to datetime. It's almost in the right format to be read automatically except there's a T instead of a
    #   space between the date and the time
    gdf['utc_datetime'] = pd.to_datetime(gdf.time)
    gdf = gdf.loc[~gdf.utc_datetime.isna()] # drop any points without a time. some garmin GPX files do this

    # Make points 3D
    gdf['altitude_ft'] = (gdf.ele * FEET_PER_METER).astype(int)
    gdf.geometry = gdf.apply(lambda row: shapely_Point(row.geometry.x, row.geometry.y, row.altitude_ft), axis=1)

    gdf['longitude'] = gdf.geometry.x
    gdf['latitude'] = gdf.geometry.y

    # Calculate speed and bearing because GPX files don't have it
    gdf.sort_values(by='utc_datetime', inplace=True)
    gdf['diff_m'] = calc_distance_to_last_pt(gdf)
    gdf['diff_seconds'] = gdf.utc_datetime.diff().dt.seconds
    gdf.loc[gdf.diff_seconds.isnull() | (gdf.diff_seconds == 0) | (gdf.diff_seconds > (seg_time_diff * 60)), 'diff_seconds'] = -1
    gdf['m_per_sec'] = gdf.diff_m / gdf.diff_seconds
    gdf['knots'] = (gdf.m_per_sec * M_PER_S_TO_KNOTS).fillna(-1).round().astype(int)# 1m/s == 1.94384 knots
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


def parse_web_sentinel_xml(parser, seg_time_diff=15):

    points = []
    for placemark in parser.find_all('Placemark'):
        if placemark.find('styleUrl').text == '#waypt':
            if not placemark.description:
                raise ValueError
            content = {
                k.strip(): v.strip() for k, v in
                [
                    [j for j in i.split(':', 1)]
                    for i in placemark.description.text.split('\n')
                    if ':' in i
                ]
            }
            coordinate_el = placemark.find('coordinates')
            if not coordinate_el:
                raise ValueError
            coordinates = [c.strip() for c in coordinate_el.text.split(',')]
            content['latitude'] = float(coordinates[1])
            content['longitude'] = float(coordinates[0])
            if len(coordinates) > 2:
                content['altitude_ft'] = float(coordinates[2])
            elif 'Feet' in content:
                content['altitude_ft'] = float(content['Feet'])
            else:
                raise RuntimeError('No altitude value for point in KML file:\n%s' % placemark.prettify())

            if 'Knots' in content:
                content['knots'] = float(content['Knots'])
            content['utc_datetime'] = pd.to_datetime(content['UTC'])

            points.append(content)

    df = pd.DataFrame(points).sort_values('utc_datetime')
    geometry = df.apply(lambda row: shapely_Point(row.longitude, row.latitude, row.altitude_ft), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf['diff_m'] = calc_distance_to_last_pt(gdf)
    gdf['diff_seconds'] = gdf.utc_datetime.diff().dt.seconds
    gdf.loc[gdf.diff_seconds.isnull() | (gdf.diff_seconds == 0) | (gdf.diff_seconds > (seg_time_diff * 60)), 'diff_seconds'] = -1
    gdf['m_per_sec'] = gdf.diff_m / gdf.diff_seconds
    if 'knots' not in gdf:
        gdf['knots'] = (gdf.m_per_sec * M_PER_S_TO_KNOTS).fillna(-1).round().astype(int)# 1m/s == 1.94384 knots
    gdf.loc[(gdf.knots < 0) | gdf.knots.isnull(), 'knots'] = 0

    gdf['previous_lat'] = gdf.shift().latitude
    gdf['previous_lon'] = gdf.shift().longitude
    gdf['heading'] = gdf.apply(lambda row:
                                    calc_bearing(*row[['previous_lat', 'previous_lon', 'latitude', 'longitude']]),
                               axis=1)\
        .fillna(-1).round().astype(int)

    return gdf


def parse_inreach_xml(parser, seg_time_diff=15):

    points = []
    for placemark in parser.find_all('Placemark'):
        if placemark.find('kml:Point'):
            points.append({d['name']: d.find('value').text for d in placemark.find_all('kml:Data') if d.find('value')})

    df = pd.DataFrame(points)

    df['ak_datetime'] = pd.to_datetime(df['Time'])
    df['utc_datetime'] = pd.to_datetime(df['Time UTC'])
    df = df.sort_values('ak_datetime') \
        .loc[df.Event.str.lower().str.contains('tracking')]\
        .rename(columns={c: c.lower().replace(' ', '_') for c in df.columns})


    # Search for registration because with an InReach, it seems particularly likely that multiple aircraft might be
    #   included in the same file
    for c in df.columns:
        registration = df[c].astype(str).str.extract(f'({REGISTRATION_REGEX})').squeeze().str.upper().fillna(False)
        if registration.all():
            df['registration'] = registration
            break

    if not (df.spatialrefsystem.dropna() == 'WGS84').all():
        raise RuntimeError('Not all coordinates in WGS84')

    df.longitude = df.longitude.astype(float)
    df.latitude = df.latitude.astype(float)
    df['altitude_ft'] = df.elevation.str.extract(r'(\d*\.\d*)').astype(float) * FEET_PER_METER

    geometry = df.apply(lambda row: shapely_Point(row.longitude, row.latitude, row.altitude_ft), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf['diff_m'] = calc_distance_to_last_pt(gdf)
    gdf['diff_seconds'] = gdf.utc_datetime.diff().dt.seconds
    gdf.loc[gdf.diff_seconds.isnull() | (gdf.diff_seconds == 0) | (gdf.diff_seconds > (seg_time_diff * 60)), 'diff_seconds'] = -1
    gdf['m_per_sec'] = gdf.diff_m / gdf.diff_seconds
    if 'knots' not in gdf:
        gdf['knots'] = (gdf.m_per_sec * M_PER_S_TO_KNOTS).fillna(-1).round().astype(int)# 1m/s == 1.94384 knots
    gdf.loc[(gdf.knots < 0) | gdf.knots.isnull(), 'knots'] = 0

    gdf['previous_lat'] = gdf.shift().latitude
    gdf['previous_lon'] = gdf.shift().longitude
    gdf['heading'] = gdf.apply(lambda row:
                                    calc_bearing(*row[['previous_lat', 'previous_lon', 'latitude', 'longitude']]),
                               axis=1)\
        .fillna(-1).round().astype(int)

    return gdf


def read_kml(path, seg_time_diff=15):
    '''
    KMLs come in two accepted variants, Foreflight and Web Sentinel. If the file is of the Foreflight variant, convert
    KML to GPX, then just use read_gpx() function. Otherwise, parse the file and convert directly to a GeoDataframe.
    KML formats are:
        1. Foreflight:
            <Placemark>
                <styleUrl>#trackStyle</styleUrl>
                <gx:Track>
                    <when>2019-11-17T18:45:59.015Z</when>
                    <gx:coord>-147.85804607913613 64.80618080143059 126.2699673929132</gx:coord>
                    ...
                </gx:Track>
            </Placemark>

        2. Web Sentinel:
            <Placemark>
                <styleUrl>#waypt</styleUrl>
                <description>
                    UTC: Wed Jun 10 18:37:00 PDT 2020
                    Lat: 63.73285333 N
                    Lon: -148.91178833 E
                    Knots: 0
                    Track: 356
                    Feet: 1738.845147
                </description>
                <Point>
                    <altitudeMode>absolute</altitudeMode>
                    <coordinates>-148.91178833,63.73285333,530.0</coordinates>
                </Point>
            </Placemark>
            <Placemark>
            ...
    '''

    with open(path, encoding='utf-8') as f:
        soup = bs4.BeautifulSoup(f, 'xml')

    if soup.find('name', text='www.websentinel.net'):
        try:
            return parse_web_sentinel_xml(soup, seg_time_diff)
        except Exception as e:
            raise RuntimeError('Could not parse Web Sentinel KML file %s: %s' % (path, e))
    elif soup.find('kml:Data', attrs={'name': 'IMEI'}):
        try:
            return parse_inreach_xml(soup, seg_time_diff)
        except RuntimeError as e:
            raise RuntimeError('Could not process %s because %s' % (path, e))
        except Exception as e:
            raise RuntimeError('Could not parse InReach KML file %s: %s' % (path, e))
    elif soup.find('gx:Track'):
        try:
            parser = kml_parser.ForeflightKMLParser()
            gpx_path = parser.to_gpx(path, path.replace('.kml', '_from_kml.gpx'))
        except Exception as e:
            raise RuntimeError('Could not parse KML file %s from Foreflight format: %s' % (path, e))
        return read_gpx(gpx_path, seg_time_diff)
    
    else:
        raise RuntimeError('Could not understand KML format of file %s' % path)


def format_aff(path, encoding='ISO-8859-1', **kwargs):

    df = pd.read_csv(path, encoding=encoding)
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


def format_gsat(path, encoding='ISO-8859-1', skip_rows=0):

    # Try to get the registration number. It's stored in the third row, even though the metadata header is the first row
    try:
        registration = 'N' + pd.read_csv(path, encoding=encoding).loc[2, 'Asset']
    except:
        registration = ''

    df = pd.read_csv(path, encoding=encoding, skiprows=skip_rows)
    for i in range(1, 6):
        if 'lat/lng' in df.columns.str.lower():
            break
        df = pd.read_csv(path, encoding=encoding, skiprows=i)
    df.dropna(subset=['Lat/Lng'], inplace=True)

    #df = pd.read_csv(path, encoding='ISO-8859-1', skiprows=5)
    # If this didn't fail, add a registration column
    if registration:
        df['registration'] = registration

    # Convert coordinates to separate decimal degree lat and lon fields
    df['latitude'], df['longitude'] = list(zip(*df['Lat/Lng'].apply(parse_gsat_coordinates)))

    # Remove "knots" in speed field
    df['knots'] = df['Speed'].astype(str).str.split(' ').str[0].astype(float)

    # Replace degree symbol in heading. For some GSAT files, it's not there in which case pandas reads the column as
    #   as floats. So first,
    #       convert to string,
    #       then try to remove the degree symbol,
    #       then convert back to float because the decimal will cause .astype(int) to throw an error,
    #       then to int
    df['heading'] = df['Heading']\
        .astype(str)\
        .str.replace('°', '')\
        .astype(float)\
        .astype(int)

    df['altitude_ft'] = df['Altitude'].str.split(' ').str[0].astype(float).round().astype(int) # Replace "ft" and convert to int

    # GSAT datetimes are in local time, so calculate UTC so that
    df['ak_datetime'] = pd.to_datetime(df['Date'], errors='coerce')
    timezone = pytz.timezone('US/Alaska')
    df['utc_datetime'] = df.ak_datetime - df.ak_datetime.apply(timezone.utcoffset)

    return df


def format_spy(path, encoding='ISO-8859-1', **kwargs):

    df = pd.read_csv(path, encoding=encoding)

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
    df.ak_datetime = pd.to_datetime(df.ak_datetime, errors='coerce')

    return df


def format_tms(path, encoding='ISO-8859-1', **kwargs):

    df = pd.read_csv(path, encoding=encoding)

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

    df['altitude_ft'] = (df['Altitude (m)'] * FEET_PER_METER).astype(int)
    df['utc_datetime'] = pd.to_datetime(df['utc_datetime'], errors='coerce')

    return df


def format_foreflight_csv(path, encoding='ISO-8859-1', **kwargs):

    try:
        registration = pd.read_csv(path, encoding=encoding, nrows=5).loc[0, 'Tail Number']
    except:
        registration = ''
    df = pd.read_csv(path, encoding=encoding, skiprows=2)
    df['registration'] = registration

    # Timestamp are in local time. The rest of the read functions all return UTC time, so calcualte that, even though
    #   the local time will just be calculated later
    epoch_datetime = datetime(1970, 1, 1)
    df['utc_datetime'] = pd.to_datetime([epoch_datetime + timedelta(seconds=round(ts/1000)) for ts in df['Timestamp']])

    # Convert altitude meters to feet
    df['altitude_ft'] = (df['Altitude'] * FEET_PER_METER).astype(int)

    # Rename other columns that don't need to be recalculated
    df.rename(columns={'Longitude': 'longitude',
                       'Latitude': 'latitude',
                       'Course': 'heading',
                       'Speed': 'knots'},
              inplace=True)

    return df


def read_excel(path):
    """
    Wrapper for read_csv() (after converting Excel file to CSV)
    :param path: Excel track
    :return: GeoDataFrame of the track file
    """
    df = pd.read_excel(path)
    _, ext = os.path.splitext(path)
    csv_path = path.replace(ext, '.csv')
    df.to_csv(csv_path, index=False)

    return read_csv(csv_path)


def get_csv_type(path, encoding):
    """
    Helper function to try to determine the CSV source
    :param path:
    :param encoding:
    :return:
    """
    column_match_scores = pd.Series([0])
    skip_rows = 0
    best_match = None
    while skip_rows < 10:
        df = pd.read_csv(path, encoding=encoding, nrows=2, skiprows=skip_rows)
        df.columns = df.columns.str.strip()
        # Figure out which file type it is (aff, gsat, spy, or tms) by selecting the file type that most closely matches
        #   the expected columns per type
        named_columns = df.columns[~df.columns.str.startswith('Unnamed')].str.strip()

        if len(named_columns):
            # This row doesn't have any recognized column names so skip it

            column_match_scores = pd.Series({
                file_type: len(named_columns[named_columns.isin(pd.Series(columns).str.strip())]) / float(len(named_columns))
                for file_type, columns in CSV_INPUT_COLUMNS
            })

            if column_match_scores.any():
                best_match = column_match_scores.idxmax() #index is file_type
                break

        skip_rows += 1

    return best_match, skip_rows


def read_csv(path, seg_time_diff=None):
    """
    Read and format a CSV of track data. CSVs can come from 4 different sources, so figure out which source it comes
    from and format accordingly

    :param path: path to track CSV
    :return: GeoDataframe of points
    """
    # Try to determine the file's encoding
    detector = chardet.universaldetector.UniversalDetector()
    encoding = ''
    with open(path, 'rb') as f:
        for line in f.readlines():
            detector.feed(line)
            if detector.done:
                encoding = detector.result['encoding']
                break
    
    detector.close()
    # Get the encoding even if the detector doesn't have high confidence
    encoding = detector.result['encoding']
        #raise RuntimeError('Could not determine encoding of file ' + path)
    

    CSV_FUNCTIONS = {'aff': format_aff,
                     'gsat': format_gsat,
                     'spy': format_spy,
                     'tms': format_tms,
                     'foreflight': format_foreflight_csv
                     }

    best_match, skip_rows = get_csv_type(path, encoding)
    
    if not best_match in CSV_FUNCTIONS:
        sorted_types = sorted(CSV_FUNCTIONS.keys())
        raise IOError(
            'The data source could not be interpreted from the column names. ' +
            ('Only %s, and %s currently accepted.' % (', '.join(sorted_types[:-1]), sorted_types[-1]))
        )

    df = CSV_FUNCTIONS[best_match](path, encoding, skip_rows=skip_rows)
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
    #   evaluate to True. Also start a new segment where the is_new_segment column is True (this column is potentially 
    #   altered in the web app when a track is split, but if it doesn't exist in the data, create it). cumsum() treats 
    #   these as a 1 and False values as 0, so all rows of the same segment will have the same ID.
    if 'is_new_segment' not in gdf:
        gdf['is_new_segment'] = False
    gdf['segment_id'] = ((time_diff >= seg_time_diff) | gdf.is_new_segment).cumsum()
    
    departure_times = gdf.groupby('segment_id').ak_datetime.min().to_frame().rename(columns={'ak_datetime': 'departure_datetime'})
    gdf = gdf.merge(departure_times, on='segment_id')
    gdf['timestamp_str'] = gdf.departure_datetime.dt.floor('%smin' % seg_time_diff).dt.strftime('%Y%m%d%H%M')
    gdf['flight_id'] = gdf.registration + '_' + gdf.timestamp_str#gdf.date_str + '_' + segment_id.astype(str)

    return gdf


READ_FUNCTIONS = {'.gpx': read_gpx,
                  '.gdb': read_gdb,
                  '.csv': read_csv,
                  '.kml': read_kml,
                  '.xlsx': read_excel
                  }


def check_duplicate_flights(registration, connection, start_time, end_time):
    """
    Check whether an aircraft with this registration has been recorded in the DB for any time within a given start and
    end time

    :param registration: Tail (N-) number for this flight
    :param connection: SQLAlchemy DB connection (from engine.connect()) pointing to postgres backend overflights DB
    :param start_time: starting time for this flight
    :param end_time: ending time for this flight
    :return: pd.DataFrame of matching flight points
    """
    sql = """
    SELECT flights.* 
    FROM flight_points 
    INNER JOIN flights ON flight_points.flight_id = flights.id
    WHERE
        flight_points.ak_datetime BETWEEN '{start_time}' AND '{end_time}' AND
        flights.registration = '{registration}'
    """\
        .format(start_time=start_time.strftime('%Y-%m-%d %H:%M'),
                end_time=end_time.strftime('%Y-%m-%d %H:%M'),
                registration=registration)

    #with engine.connect() as conn, conn.begin():
    matching_flights = pd.read_sql(sql, connection).drop_duplicates(subset=['registration', 'departure_datetime'])

    return matching_flights


def calculate_duration(gdf):
    """
    Helper function to calculate landing time and duration. This needs to happen 
    in format track, but it also needs to be recalculated after edits
    """    
    land_times = gdf.groupby('segment_id').ak_datetime.max().to_frame().rename(columns={'ak_datetime': 'land_time'})
    gdf['landing_datetime'] = gdf.merge(land_times, on='segment_id').land_time
    gdf['duration_hrs'] = (gdf.landing_datetime - gdf.departure_datetime).dt.seconds/3600.0

    return gdf


def format_track(path, seg_time_diff=15, min_point_distance=200, registration='', submission_method='manual', operator_code=None, aircraft_type=None, force_registration=True, **kwargs):

    _, extension = os.path.splitext(path)

    # try to apply function from dict based on file extension
    try:
        gdf = READ_FUNCTIONS[extension.lower()](path)
    except:
        # If that failed, try each track reading function
        for ext in READ_FUNCTIONS.keys():
            if ext == extension.lower():
                continue # already tried this one so no sense in wasting the effort
            else:
                try:
                    gdf = READ_FUNCTIONS[ext](path)
                    # It might be possible for the read function to return a Geodataframe that just doesn't have the
                    #   right cols. If it doesn't, raise a generic exception to try the next function
                    if not VALIDATION_COLUMNS.isin(gdf.columns).all():
                        raise
                    break # if we got here, that means the file was successfully read
                except:
                    continue

    if not 'gdf' in locals():
        error_message = 'Unable to read the file %s' % path
        if not extension.lower() in READ_FUNCTIONS:
            sorted_extensions = [c.replace('.', '').upper() for c in sorted(READ_FUNCTIONS.keys())]
            error_message += ' because it is likely of an unsupported file type: %s. Only %s, and %s currently accepted.' % \
                             (extension.replace('.', '').upper(), ', '.join(sorted_extensions[:-1]), sorted_extensions[-1])
        raise IOError(error_message)

    # Calculate local (AK) time
    timezone = pytz.timezone('US/Alaska')
    if 'ak_datetime' not in gdf.columns:
        # If the timezone is defined (because it was included in the timestamp given by the file), convert to local time
        #   to get AK time
        if gdf.utc_datetime.dt.tz:
            gdf['ak_datetime'] = gdf.utc_datetime.dt.tz_convert(timezone)
        else: # otherwise, calculate by adding the offset
            gdf['ak_datetime'] = gdf.utc_datetime + gdf.utc_datetime.apply(timezone.utcoffset)
    # track points are *usually* in chronological order, but not always Also some files have duplicated track points for
    #   some reason, so get rid of those. Resetting the index is important if this function is being called from 
    #   poll_feature_service.py. The index is used to create a point_index field, which the track-editor app needs
    #   for splitting tracks and deleting points
    gdf = gdf.sort_values('ak_datetime')\
        .drop_duplicates('ak_datetime')\
        .reset_index() 

    # Validate the registration
    if 'registration' in gdf.columns and not force_registration: # Already in a column in the data
        if registration:
            warnings.warn('registration %s was given but the registration column found in the data will be '
                          'used instead' % registration)
        # verify that the reg. in each column matches the right pattern
        regex_mask = gdf.registration.str.contains(REGISTRATION_REGEX, case=False)
        if not all(regex_mask):
            invalid_registrations = gdf.loc[~regex_mask, 'registration'].unique()
            raise ValueError('A column with aircraft registrations was detected in the input file, but the following '
                             'registrations were invalid:\n\t-%s' % '\n\t-'.join(invalid_registrations))
    else:
        # If given, make sure it matches the pattern (N1 to N99999, N1A to N9999Z, or N1AA to N999ZZ)
        if registration:
            if not re.fullmatch(REGISTRATION_REGEX, registration):
                raise ValueError('The registration given, %s, is invalid' % registration)
        # If the N-number wasn't given, try to find it in the file
        else:
            reg_matches = re.findall(REGISTRATION_REGEX, os.path.basename(path))
            if len(reg_matches):
                registration = reg_matches[0].upper()
            else:
                # Generate a bogus registration (starts with Z instead of N)
                registration = 'Z' + \
                               ''.join(random.choices(string.digits, k=random.choice(range(1, 6)))) +\
                               ''.join(random.choices(string.ascii_uppercase, k=random.choice(range(1, 3))))
                warnings.warn('No registration column in the data, none could be found in the filename, and none given.'
                              ' Using %s instead (random alphanumerics and starting with "Z")')
        # Otherwise, use the one given
        gdf['registration'] = registration

    gdf.registration = gdf.registration.str.upper()

    # Get unique flight_ids per line segment in place
    gdf = get_flight_id(gdf, seg_time_diff)\
        .drop(gdf.index[((gdf.diff_m < min_point_distance) & (gdf.utc_datetime.diff().dt.seconds == 0))])\
        .dropna(subset=['ak_datetime'])\
        .sort_values(by=['ak_datetime'])

    gdf = calculate_duration(gdf)

    # Get metadata-y columns
    gdf['submission_time'] = (datetime.now()).strftime('%Y-%m-%d %H:%M')
    gdf['submission_method'] = submission_method
    gdf['is_new_segment'] = False
    if operator_code:
        gdf['operator_code'] = operator_code
    if aircraft_type:
        gdf['aircraft_type'] = aircraft_type
    if 'tracks_notes' in gdf:
        gdf['submitter_notes'] = gdf.tracks_notes

    return gdf


def import_data(connection_txt=None, data=None, path=None, seg_time_diff=15, min_point_distance=200, registration='', submission_method='manual', operator_code=None, aircraft_type=None, silent=False, force_import=False, ssl_cert_path=None, engine=None, force_registration=False, ignore_duplicate_flights=False, **kwargs):


    if type(data) == gpd.geodataframe.GeoDataFrame:
        gdf = data.copy()
    elif path:
        gdf = format_track(path, seg_time_diff=seg_time_diff, min_point_distance=min_point_distance,
                           registration=registration, submission_method=submission_method,
                           operator_code=operator_code, aircraft_type=aircraft_type, force_registration=force_registration)
    else:
        raise ValueError('Either data (a geodataframe) or path (to a valid track file) must be given')

    if not engine and connection_txt:
        engine = db_utils.connect_db(connection_txt)
    elif not engine:
        raise ValueError('Either an SQLAlchemy Engine (from create_engine()) or connection_txt must be given')

    # Recalculate landing time and duration here in case there were edits that changed these
    #   Also drop any segments with only one vertex 
    gdf = calculate_duration(gdf)\
        .loc[gdf.duration_hrs > 0]

    # get columns from DB tables
    flight_columns = db_utils.get_db_columns('flights', engine)
    point_columns = db_utils.get_db_columns('flight_points', engine)
    line_columns = db_utils.get_db_columns('flight_lines', engine)
    ''' ############ add submission table #############'''

    # separate flights, points, and lines
    flights = gdf[[c for c in flight_columns if c in gdf]].drop_duplicates()
    
    flights['end_datetime'] = gdf.groupby('flight_id').ak_datetime.max().values
    # if coming from web app, this should already be in the data so don't overwrite
    #if 'submitter' not in flights.columns:
    #    flights['submitter'] = os.getlogin()
    if 'track_editor' not in flights.columns:
        flights['track_editor'] = os.getlogin()#flights.submitter
    if path and 'source_file' not in flights.columns:
        flights['source_file'] = os.path.join(ARCHIVE_DIR, os.path.basename(path))
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

        # Insert only new flights. Check for new flights by looking for flight points from the same registration number
        #   that are within the start and end times of each flight segment (since an aircraft can't be in 2 locations
        #   at the same time).
        existing_flight_info = []
        existing_flight_ids = []
        for _, f in flights.iterrows():
            matching_flights = check_duplicate_flights(f.registration, conn, f.departure_datetime, f.end_datetime)
            existing_flight_info.extend([(m.registration, m.departure_datetime) for _, m in matching_flights.iterrows()])
            existing_flight_ids.extend(matching_flights.flight_id)
        if len(existing_flight_info) and not force_import and not ignore_duplicate_flights:
            existing_str = '\n\t-'.join(['%s: %s' % f for f in existing_flight_info])
            raise ValueError('The file {path} contains flight segments that already exist in the database as'
                             ' indicated by the following registration and departure times:\n\t-{existing_flights}'
                             '\nEither delete these flight segments from the database or run this script again with'
                             ' the --force_import flag (ONLY USE THIS FLAG IF YOU KNOW WHAT YOU\'RE DOING).'
                             .format(path=path, existing_flights=existing_str))

        new_flights = flights.loc[~flights.flight_id.isin(existing_flight_ids)]
        new_flights.drop(columns='end_datetime')\
            .to_sql('flights', conn, if_exists='append', index=False)

        # Warn the user if any of the flights already exist
        n_flights = len(flights)
        n_new_flights = len(new_flights)
        if n_new_flights == 0:
            raise ValueError('No new flight segments were inserted from this file because they all already exist in'
                             ' the database.')
        if n_flights != n_new_flights:
            warnings.warn('For the file {path}, the following {existing} of {total} flight segments already exist:'
                              '\n\t- {ids}'
                              .format(path=path,
                                      existing=n_flights - n_new_flights,
                                      total=n_flights,
                                      ids='\n\t-'.join(existing_flight_ids)
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

        # INSERT info about this aircraft if it doesn't already exist. If it does, UPDATE it if necessary
        #   disable because this happens now as a separate scheduled task
        if ssl_cert_path:
            ainfo.update_aircraft_info(conn, registration, ssl_cert_path)#'''

    # VACUUM and ANALYZE clean up unused space and recalculate statistics to improve spatial query performance. Attempt
    #   to run these commands on both spatial tables, but if they fail, just warn the user since it's not that big of
    #   a deal
    try:
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute('VACUUM ANALYZE flight_points;')
            conn.execute('VACUUM ANALYZE flight_lines;')
    except:
        warnings.warn("Unable to VACUUM and ANALYZE geometry tables. You should connect to the database and manually"
                      " run 'VACUUM ANALYZE flight_points' and 'VACUUM ANALYZE flight_lines;' to ensure queries are as"
                      " effecient as possible")

    # Archive the data file
    if not os.path.isdir(ARCHIVE_DIR):
        try:
            os.mkdir(ARCHIVE_DIR)
        except:
            pass
    if os.path.isdir(os.path.dirname(path)):
        try:
            shutil.copy(path, ARCHIVE_DIR)
            os.remove(path)
        except Exception as e:
            warnings.warn('Data successfully imported, but could not copy track files because %s. You will have to '
                          'manually copy and paste this file to %s' % (e, ARCHIVE_DIR))

    if not silent:
        sys.stdout.write('%d flight %s imported:\n\t-%s' % (len(flights), 'tracks' if len(flights) > 1 else 'track', '\n\t-'.join(flight_ids.flight_id)))
        sys.stdout.flush()


def print_operator_codes(connection_txt):
    """Helper function to display operator codes in the console for the user"""

    engine = db_utils.connect_db(connection_txt)
    operator_codes = db_utils.get_lookup_table(engine, 'operators', index_col='name', value_col='code')
    operator_code_str = '\n\t-'.join(sorted(['%s: %s' % operator for operator in operator_codes.items()]))

    print('Operator code options:\n\t-%s' % operator_code_str)


def main(connection_txt, track_path, seg_time_diff=15, min_point_distance=200, registration='', submission_method='manual', operator_code=None, aircraft_type=None, email_credentials_txt=None, log_file=None, force_import=False, ssl_cert_path=None):

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
        import_data(connection_txt, path=track_path, seg_time_diff=seg_time_diff, min_point_distance=min_point_distance, registration=registration, submission_method=submission_method, operator_code=operator_code,
                      aircraft_type=aircraft_type, force_import=force_import, ssl_cert_path=ssl_cert_path)
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

