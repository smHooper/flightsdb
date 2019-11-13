import os
import sys
import json
import re
import zipfile
import shutil
import traceback
import pandas as pd
from glob import glob
from datetime import datetime
from sqlalchemy import create_engine

import download_feature_service as download
import db_utils
import process_emails

pd.set_option('display.max_columns', None)# useful for debugging

SUBMISSION_TICKET_INTERVAL = 900 # seconds between submission of a particular user
LOG_CACHE_DAYS = 14 # amount of time to keep a log file
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
LANDING_FEE = 5.15 # per person fee for each passenger on scenic flight or dropped off
# Collect messages to warn either the user, the landings data steward, or the track editors. Also log all of them
MESSAGES = []

LANDINGS_FLIGHT_COLUMNS = {'landing_operator': 'operator_code',
                           'landing_tail_number': 'registration',
                           'landing_datetime': 'departure_datetime',
                           'landing_route': 'scenic_route',
                           'landing_aircraft_type': 'aircraft_type',
                           'globalid': 'agol_global_id',
                           'ticket': 'ticket',
                           'submission_method': 'submission_method',
                           'flight_id': 'flight_id',
                           'landing_flight_notes': 'notes'
                          }
LANDINGS_COLUMNS = {'flight_id': 'flight_id',
                    'landing_location': 'location',
                    'n_passengers': 'n_passengers',
                    'landing_type': 'landing_type',
                    'landing_justification': 'justification',
                    'landing_notes': 'notes'
                    }

def write_log(log_dir, download_dir, timestamp, submission_time, data_was_downloaded, error):

    log_file_path = os.path.join(log_dir, '{0}_log_{1}.json'.format(os.path.basename(__file__).replace('.py', ''),
                                                                    re.sub('\D', '', timestamp)))

    log_info = {'download_dir': download_dir,
                'data_downloaded': data_was_downloaded,
                'error': error,
                'timestamp': timestamp,
                'submission_time': submission_time,
                'log_file_path': log_file_path}

    with open(log_file_path, 'w') as j:
        json.dump(log_info, j, indent=4)


def log_and_exit(log_info, write_log_args):
    ''' Helper function to delete old logs and write a new one before exiting'''
    try:
        # Delete old logs
        log_info.loc[(datetime.now() - log_info.timestamp).days > LOG_CACHE_DAYS, 'log_file_path'].apply(os.remove)
    except:
        pass
    sys.exit(write_log(*write_log_args))


def julian_date_to_datetime(julian_date):
    '''
    createReplica stores datetimes in an sqlite DB as a Julian date (https://simple.wikipedia.org/wiki/Julian_day) so
    convert to Unix Epoch Time
    :param timestamp:
    :return:
    '''

    # Python (really Unix) uses 1970-1-1 00:00:00 as the epoch (i.e., reference timestamp).
    JULIAN_UNIX_EPOCH = 2440587.5 # 1970-1-1 00:00:00 as a Julian date
    if pd.isna(julian_date) or julian_date < JULIAN_UNIX_EPOCH:
        return pd.NaT
    try:
        return datetime.fromtimestamp((julian_date - JULIAN_UNIX_EPOCH) * 60 * 60 * 24)
    except:
        return pd.NaT


def get_ticket(group, connections):
    '''
    Return a dataframe with a ticket number assigned to all records in a new column
    '''

    submission_type = group.submission_type.iloc[0]  # should be all the same submission type
    submitter = group.submitter.iloc[0]
    submission_time = group.submission_time.min()
    conn = connections[submission_type]
    conn.execute("INSERT INTO submissions (submitter, submission_time) VALUES ('%s', '%s');" % (submitter, submission_time))
    group['ticket'] = pd.read_sql("SELECT max(ticket) FROM submissions;", conn).squeeze()

    return group

def get_attachment_info(zip_path):
    ''' Each zip file created by download_feature_service.download_data() has an accompanying JSON metadata file, so return the JSON as a dict'''
    json_meta_path = zip_path.replace('.zip', '.json')
    with open(json_meta_path) as j:
        metadata = json.load(j)
    engine = create_engine('sqlite:///' + metadata['sqlite_path'])
    with engine.connect() as conn:
        submission_data = pd.read_sql("SELECT * FROM %s WHERE globalid='%s';" % (metadata['parent_table_name'], metadata['REL_GLOBALID']), conn).squeeze(axis=1)

    return {**metadata, **submission_data}


def poll_feature_service(log_dir, download_dir, agol_credentials_json, email_credentials_json, db_credentials_json, ssl_cert, landings_conn, tracks_conn):

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)

    # Get the last time any data were downloaded
    script_name = os.path.basename(__file__).replace('.py', '')
    log_info = []
    for json_path in glob(os.path.join(log_dir, '%s_log_*.json' % script_name)):
        # Try to read the file, but if the JSON isn't valid, just skip it
        try:
            with open(json_path) as j:
                log_info.append(json.load(j))
        except:
            continue
    log_info = pd.DataFrame(log_info)
    if len(log_info):
        download_info = log_info.loc[log_info.data_downloaded]
        if len(download_info): # there has been at least one download in the last LOG_CACHE_DAYS
            last_download_time = download_info.loc[download_info.timestamp == download_info.timestamp.max(), 'timestamp']
        else:
            last_download_time = '1970-1-1 00:00:00'
    else:
        # There are no logs because this is the first time the script is being run (or the logs got deleted), so set
        #   the last_download_time to something that will return all records
        last_download_time = '1970-1-1 00:00:00'

    # Get last submission time
    agol_credentials = download.read_credentials(agol_credentials_json)
    service_url = agol_credentials['service_url']
    token = download.get_token(agol_credentials_json, agol_credentials, ssl_cert=ssl_cert)
    service_info = download.get_service_info(service_url, token, ssl_cert)
    layer_info = pd.DataFrame(service_info['layers'] + service_info['tables']).set_index('id')
    layers = layer_info.index.tolist()
    result = download.query_after_timestamp(service_url, token, layers, last_download_time, ssl_cert)
    submissions = pd.DataFrame([feature['attributes'] for feature in result['layers'][0]['features']]) # pretty sure the first layer is always the parent table for a survey123 survey with related tables
    field_types = {layer_info.loc[layer['id'], 'name']:
                       pd.Series({field['name']: field['type'] for field in layer['fields']})
                   for layer in result['layers']}
    if not len(submissions):
        log_and_exit(log_info, [log_dir, download_dir, timestamp, '', False, 'No submission since last download'])
    last_submission_time = datetime.fromtimestamp(submissions.EditDate.max().squeeze()/1000.0) # timestamps are in local tz, surprisingly

    # If the last_sumbission_time was before the last_download_time, these data have already been downloaded
    if last_submission_time < datetime.strptime(last_download_time, TIMESTAMP_FORMAT):
        log_and_exit(log_info, [log_dir, download_dir, timestamp, '', False, 'No submission since last download'])


    # If the last submission time was within SUBMISSION_TICKET_INTERVAL, exit in case a user is making multiple
    #   submission at once that should be grouped into a single ticket. It's possible that two different users could 
    #   be submitting within the SUBMISSION_TICKET_INTERVAL of one another, but it's not worth the overhead and 
    #   management to download data submitted by one user and not another. Their submissions will be assigned different
    #   ticket numbers, but just wait until there's a gap of all submissions of SUBMISSION_TICKET_INTERVAL. 
    '''if (datetime.now() - last_submission_time).seconds < SUBMISSION_TICKET_INTERVAL:
        log_and_exit(log_info, [log_dir, download_dir, timestamp, '', False,
                                'Last submission was within %d minutes' % (SUBMISSION_TICKET_INTERVAL/60)])'''

    # download the data
    sqlite_path = download.download_data(download_dir, token, layers, service_info, service_url, ssl_cert, last_poll_time=last_download_time)

    # Make edits to a working copy of the data
    working_dir = os.path.join(download_dir, 'working')
    if not os.path.isdir(working_dir):
        os.mkdir(working_dir)
    working_sqlite_path = os.path.join(working_dir, os.path.basename(sqlite_path))
    shutil.copyfile(sqlite_path, working_sqlite_path)
    engine = create_engine('sqlite:///' + working_sqlite_path)

    # Create separate tickets for each user for both landing and track data
    submissions['submission_time'] = [datetime.fromtimestamp(ts/1000) for _, ts in submissions['CreationDate'].iteritems()]
    submissions.rename(columns={'Creator': 'submitter'}, inplace=True)

    connection_dict = {'tracks': tracks_conn, 'landings':landings_conn}
    submissions = pd.concat([get_ticket(g, connection_dict)
                             for _, g in submissions.groupby(['submitter', 'submission_type'])])

    # Process and import the landing data. The flight tracks will need to be unzipped, but they need to be
    #   validated/edited before they can be imported. This is handled manually via a separate web app
    main_table_name = pd.DataFrame(service_info['layers']).set_index('id').loc[0, 'name']
    with engine.connect() as conn, conn.begin():
        # Convert datetimes in place and get rid of the braces around IDs
        for table_name, data_types in field_types.items():
            df = pd.read_sql_table(table_name, conn)

            for field, _ in data_types.loc[data_types == 'esriFieldTypeDate'].iteritems():
                df[field] = pd.to_datetime([julian_date_to_datetime(jd) for jd in df[field]]).round('S')

            for field in df.columns[['globalid' in c.lower() for c in df.columns]]:
                df[field] = df[field].str.replace('{', '').str.replace('}', '')
            df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Add the ticket column to the sqlite flights table
        all_flights = pd.read_sql_table(main_table_name, conn)
        # global IDs in query are lowercase but they're uppercase in createReplica
        submissions.globalid = submissions.globalid.str.upper()
        all_flights['ticket'] = all_flights.merge(submissions, on='globalid').ticket
        all_flights.to_sql(main_table_name, conn, if_exists='replace')
        landings = pd.read_sql_table('landing_repeat', conn)

    # make flight IDs
    landing_flights = all_flights.loc[all_flights.submission_type == 'landings']\
        .dropna(axis=1, how='all') # columns for all track submissions will be empty so drop them
    landing_flights.rename(columns=LANDINGS_FLIGHT_COLUMNS, inplace=True)
    landing_flights['flight_id'] = landing_flights.registration + '_' + \
                                   landing_flights.departure_datetime.dt.floor('15min').dt.strftime('%Y%m%d%H%M')
    existing_flight_ids = pd.read_sql("SELECT flight_id FROM flights", landings_conn).squeeze(axis=1)
    new_flights = landing_flights.loc[~landing_flights.flight_id.isin(existing_flight_ids) &
                                      ~landing_flights.departure_datetime.isnull()]

    if len(new_flights) == 0:
        #### send warnings/errors to specific users
        #### I should probably alter this so there's a msg, recipients, and loglevel column
        MESSAGES.append(['All landings already reported', 'user_warn'])
    else:
        new_flights['submission_method'] = 'survey123'
        new_flights['source_file'] = sqlite_path
        new_flights.reindex(columns=LANDINGS_FLIGHT_COLUMNS.values())\
            .to_sql('flights', landings_conn, if_exists='append', index=False)
        global_ids = pd.read_sql("SELECT id, agol_global_id FROM flights WHERE flight_id IN ('%s')"
                                 % "', '".join(new_flights.flight_id),
                                 landings_conn)

        # Calculate concessions fees and attach numeric IDs to landings and fees
        passengers = landings.melt(id_vars='parentglobalid',
                                   value_vars=[c for c in landings.columns if c.startswith('n_passengers')],
                                   value_name='n_passengers',
                                   var_name='landing_type') \
            .dropna(subset=['n_passengers'])
        fees = passengers.groupby('parentglobalid').sum().reset_index()
        fees['fee'] = fees.n_passengers * LANDING_FEE
        fees['flight_id'] = fees.merge(global_ids, left_on='parentglobalid', right_on='agol_global_id').id
        landings['flight_id'] = landings.merge(global_ids, left_on='parentglobalid', right_on='agol_global_id').id
        passengers.landing_type = passengers.landing_type.apply(lambda x: x.split('_')[-1])
        passengers['index'] = passengers.index # keep track of index so that duplicates are dropped on many-to-many merge
        landings = passengers.merge(landings.drop(columns='landing_type'), on='parentglobalid')\
            .drop_duplicates(subset='index')\
            .rename(columns=LANDINGS_COLUMNS)\
            .reindex(columns=LANDINGS_COLUMNS.values())

        # INSERT into backend
        fees.drop(columns='parentglobalid')\
            .to_sql('concession_fees', landings_conn, if_exists='append', index=False)
        landings\
            .to_sql('landings', landings_conn, if_exists='append', index=False)

    attachment_dir = os.path.join(download_dir, 'attachments')
    for zip_path in glob(os.path.join(attachment_dir, '*.zip')):

        attachment_info = get_attachment_info(zip_path)

        with zipfile.ZipFile(zip_path) as z:
            for fname in z.namelist():
                _, ext = os.path.splitext(fname)
                if ext not in ['.csv', '.gpx', '.gdb']:
                    MESSAGES.append(['%s is not in an accepted file format', 'warn_user'])
                try:
                    z.extract(fname)
                except Exception as e:
                    MESSAGES.append(['Could not extract {file} from {zip} because {error}. You should contact the'
                                     ' submitter: {submitter}'
                                    .format(file=fname, zip=zip_path, error=e, submitter=attachment_info['Creator']),
                                     'warn_track'])

    # Send email to track editors

    # Send confirmation email to submitter

    import pdb; pdb.set_trace()




def main(log_dir, download_dir, agol_credentials_json, email_credentials_json, db_credentials_json, ssl_cert):
    '''
    Ensure that a log file is always written (unless the error occurs in write_log(), which is unlikely)
    '''
    #try:

    # Open connections to the DBs and begin transactions so that if there's an exception, no data are inserted
    with open(db_credentials_json) as j:
        connection_info = json.load(j)
    connection_template = 'postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'
    landings_engine = create_engine(connection_template.format(**connection_info['landings']))
    tracks_engine = create_engine(connection_template.format(**connection_info['tracks']))

    with landings_engine.connect() as l_conn, tracks_engine.connect() as t_conn, l_conn.begin(), t_conn.begin():
        poll_feature_service(log_dir, download_dir, agol_credentials_json, email_credentials_json, db_credentials_json, ssl_cert, l_conn, t_conn)

    '''except Exception as error:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'line_number': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': exc_type.__name__,
            'message': str(error),  # or see traceback._some_str()
            'full_traceback': traceback.format_exc()
        }
        write_log(log_dir, download_dir, timestamp, '', False, traceback_details)'''


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))