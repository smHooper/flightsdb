import os
import sys
import json
import re
import zipfile
import shutil
import smtplib
import openpyxl
import traceback
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import download_feature_service as download
import import_track
import db_utils
import process_emails

pd.set_option('display.max_columns', None)# useful for debugging
pd.options.mode.chained_assignment = None

SUBMISSION_TICKET_INTERVAL = 1#900 # seconds between submission of a particular user
LOG_CACHE_DAYS = 14 # amount of time to keep a log file
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
LANDING_FEE = 5.15 # per person fee for each passenger on scenic flight or dropped off
# Collect messages to warn either the user, the landings data steward, or the track editors. Also log all of them
MESSAGES = []

LANDINGS_FLIGHT_COLUMNS = {'landing_operator':      'operator_code',
                           'landing_tail_number':   'registration',
                           'landing_datetime':      'departure_datetime',
                           'landing_route':         'scenic_route',
                           'landing_aircraft_type': 'aircraft_type',
                           'globalid':              'agol_global_id',
                           'ticket':                'ticket',
                           'submission_method':     'submission_method',
                           'submission_time':       'submission_time',
                           'flight_id':             'flight_id',
                           'source_file':           'source_file',
                           'landing_flight_notes': 'notes'
                          }
LANDINGS_COLUMNS = {'flight_id':            'flight_id',
                    'landing_location':     'location',
                    'n_passengers':         'n_passengers',
                    'landing_type':         'landing_type',
                    'landing_justification':'justification',
                    'landing_notes':        'notes',
                    'sort_order':           'sort_order'
                    }

LANDING_RECEIPT_COLUMNS = ['departure_datetime',
                           'registration',
                           'aircraft_type',
                           'scenic_1_passengers',
                           'scenic_route',
                           'scenic_1_location',
                           'dropoff_1_passengers',
                           'dropoff_1_location',
                           'pickup_1_passengers',
                           'pickup_1_location',
                           'dropoff_2_passengers',
                           'dropoff_2_location',
                           'pickup_2_passengers',
                           'pickup_2_location',
                           'dropoff_3_passengers',
                           'dropoff_3_location',
                           'pickup_3_passengers',
                           'pickup_3_location',
                           'notes',
                           'n_passengers',
                           'n_fee_passengers',
                           'fee'
                           ]

def read_json_params(params_json):
    '''
    Read and validate a parameters JSON file
    :param params_json: path to JSON file
    :return: dictionary of params
    '''

    with open(params_json) as j:
        params = json.load(j)
    required = pd.Series(['agol_credentials', 'db_credentials', 'agol_users'])
    missing = required.loc[~required.isin(params.keys())]
    if len(missing):
        raise ValueError('Invalid config JSON: {file}. It must contain all of "{required}" but "{missing}" are missing'
                         .format(file=params_json, required='", "'.join(required), missing='", "'.join(missing)))
    
    return params
    

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

    return log_file_path


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
        epoch_timestamp = round((julian_date - JULIAN_UNIX_EPOCH) * 60 * 60 * 24) # number of seconds since Unix epoch
        return datetime(1970, 1, 1) + timedelta(seconds=epoch_timestamp)#datetime.fromtimestamp((julian_date - JULIAN_UNIX_EPOCH) * 60 * 60 * 24)
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

def get_submitter_email(agol_user, agol_user_emails):
    '''
    Helper function to get an email address for the given AGOL account
    :param agol_user:
    :return:
    '''
    # All NPS accounts have an alternate email alias that is just their UPN and all NPS AGOl accounts are the UPN
    #   with "_nps" appended
    agol_user = agol_user.lower()
    if agol_user.endswith('_nps'):
        return agol_user.replace('_nps', '')
    # Otherwise, try to look up the user in the
    elif agol_user in agol_user_emails:
        return agol_user_emails[agol_user]
    else:
        return None


def get_landings_html_table(data):

    thead_tr_style = 'style="font-weight: bold; height: 40px; background-color: #cccccc;'
    cell_style = 'style="padding-left: 20px; padding-right: 20px;"'
    thead_html = ('<tr {tr_style}"><th {cell_style}>' + '</th><th {cell_style}>'.join(data.columns) + '</th></tr>')\
        .format(tr_style=thead_tr_style, cell_style=cell_style)
    tr_bg_dark = ' background-color: #f2f2f2;'
    tr_content = data.apply(lambda x: ('<td {cell_style}>' + '</td><td {cell_style}>'.join(x.astype(str)) + '</td>').format(cell_style=cell_style),
                            axis=1)
    tbody_html = ''.join(['<tr style="height: 40px; {bg_style}">{td_elements}'.format(
        bg_style=tr_bg_dark if i % 2 == 1 else '', td_elements=td_elements, cell_style=cell_style) for i, td_elements in
                          tr_content.items()]) + '</tr>'
    html =\
    """<table style="border-spacing: 0px;">
        <thead>
            {table_header}
        </thead>
        <tbody>
            {table_body}
        </tbody>
    </table>""".format(table_header=thead_html, table_body=tbody_html)

    return html


def get_email_html(data, start_datetime, end_datetime):

    start_date_str = start_datetime.strftime('%B %d, %Y')
    start_time_str = start_datetime.strftime('%I:%M %p').lstrip('0')
    end_time_str = end_datetime.strftime('%I:%M %p').lstrip('0')
    #table_html = '%s %s' % ('<h4>Landings</h4><br>' if len(track_data) else '', get_landings_html_table(landings_data))
    table_html = get_landings_html_table(data)
    html = """
    <html>
        <head></head>
        <body>
            <p>
                Thank you for your flight data submission. Below is a receipt of the data you submitted on {start_date} between {start_time} and {end_time}.
                <br>
                <br>
            </p>
                {table}
            <p>
                <br>
                Note that any data you submitted after more than a 15 minute break will be included on a separate receipt.
                <br>
                <br>
                If you have any questions about this receipt or if you think there is an error in the data, you can reply to this message and a member of our team will get back to you as soon as possible. 
            </p>
        </body>
    </html>
    """.format(start_date=start_date_str, start_time=start_time_str, end_time=end_time_str, table=table_html)

    return html


def get_attachment_info(file_path, ext='.zip'):
    ''' Each zip file created by download_feature_service.download_data() has an accompanying JSON metadata file, so return the JSON as a dict'''
    json_meta_path = file_path.replace(ext, '.json')
    with open(json_meta_path) as j:
        metadata = json.load(j)
    engine = create_engine('sqlite:///' + metadata['sqlite_path'])
    with engine.connect() as conn:
        submission_data = pd.read_sql("SELECT * FROM %s WHERE globalid='%s';" % (metadata['parent_table_name'], metadata['REL_GLOBALID']), conn).squeeze(axis=0)

    return pd.concat([pd.Series(metadata), submission_data])


def track_to_json(gdf, attachment_info):

    unserializable_fields = ~gdf.dtypes.astype(str).isin(['int64', 'int32', 'float64', 'object'])
    gdf.loc[:, unserializable_fields] = gdf.loc[:, unserializable_fields].astype(str)
    geojson_strs = {}#seg_id: json.loads(df.to_json()) for seg_id, df in gdf.groupby('segment_id')}
    for seg_id, df in gdf.groupby('segment_id'):#.apply(lambda df: df.index.min())
        df['min_index'] = df.index.min()
        df['point_index'] = df.index
        geojson_strs[seg_id] = json.loads(df.to_json())

    return {'geojsons': geojson_strs, 'track_info': attachment_info.astype(str).to_dict()}


def make_pretty_landing_report(ticket, flights, landings, fees):

    # For each flight, get the ordinal for each landing type (e.g., dropoff 1, dropoff 2, pickup 1, pickup 2)
    these_landings = landings.loc[landings.flight_id.isin(flights.id)]
    for flight_id, flight in these_landings.groupby(['flight_id', 'landing_type']):
        these_landings.loc[flight.index, 'landing_order'] = list(range(1, len(flight) + 1))
    these_landings['landing_type_order'] = these_landings.landing_type + '_' + these_landings.landing_order.astype(int).astype(str)
    #

    # Pivot the table for passengers and landings so that each row represents 1 flight
    pivoted_passengers = these_landings.pivot(index='flight_id', columns='landing_type_order', values='n_passengers')
    pivoted_locations = these_landings.pivot(index='flight_id', columns='landing_type_order', values='location')
    pivoted_passengers.rename(columns={c: c + '_passengers' for c in pivoted_locations.columns}, inplace=True)
    pivoted_locations.rename(columns={c: c + '_location' for c in pivoted_locations.columns}, inplace=True)

    report = flights.drop(columns='flight_id')\
        .merge(pivoted_passengers, left_on='id', right_index=True)\
        .merge(pivoted_locations, left_on='id', right_index=True)\
        .merge(fees, left_on='id', right_on='flight_id')
    report['departure_date'] = report.departure_datetime.dt.strftime('%m/%d/%Y')
    report['departure_time'] = report.departure_datetime.dt.strftime('%I:%M %p')

    # Concat all notes
    these_landings['landing_notes'] = these_landings.drop_duplicates(['flight_id', 'location'])\
        .loc[:, ['justification', 'notes']]\
        .apply(lambda x: ('; '.join(x.fillna('').astype(str)).strip('; ')), axis=1)\
        .fillna('')
    notes = these_landings.groupby('flight_id').apply(lambda group: '; '.join(group.landing_notes.dropna()).strip('; '))
    notes.index = notes.index.tolist()
    report['landing_notes'] = report.merge(pd.DataFrame({'flight_landing_notes': notes, 'flight_id': notes.index}),
                                           on='flight_id').flight_landing_notes
    report['all_notes'] = report.loc[:, ['landing_notes', 'notes']]\
        .apply(lambda x: ('; '.join(x.fillna('').astype(str)).strip('; ')), axis=1)\
        .fillna('')
    report['n_fee_passengers'] = report.n_passengers
    report.n_passengers = report[[c for c in report if c.endswith('_passengers') and c != 'n_passengers']].sum(axis=1)

    receipt_info = pd.DataFrame([{'operator': report.operator_code.iloc[0],
                                 'ticket': ticket,
                                 'date': datetime.now().strftime('%m/%d/%Y'),
                                 'pax_fee': LANDING_FEE}],)

    return report.reindex(columns=LANDING_RECEIPT_COLUMNS), receipt_info


def save_excel_receipt(template_path, receipt_dir, ticket, data_type, data, info=pd.DataFrame()):
    ''' Save a formatted data frame as an Excel file to attach to notification emails to submitters'''

    # Remove the "data" sheet from the template so it can be replaced with the dataframe
    xl_path = os.path.join(receipt_dir, '{}_receipt_ticket{}.xlsx'.format(data_type, ticket))
    wb = openpyxl.Workbook(template_path)
    wb.remove(wb['data'])
    if 'info' in wb.sheetnames and len(info):
        wb.remove(wb['info'])
    wb.save(xl_path)
    wb.close()

    # Add the data and info if it was given
    with pd.ExcelWriter(xl_path, engine='openpyxl', mode='a') as writer:
        data.to_excel(writer, sheet_name='data', index=False)
        if len(info):
            info.to_excel(writer, sheet_name='info', index=False)

    return xl_path


def import_landings(flights, landings_conn, sqlite_path, landings, receipt_dir, receipt_template):
    '''
    Process and import landing data into Postgres backend. Send receipt to the submitter

    :param flights: dataframe of newly submitted flights
    :param landings_conn: connection object to Postgres DB of landings
    :param sqlite_path: downloaded data from AGOL
    :param landings: data frame of landings
    :param receipt_dir: directory to save Excel file submission receipts
    :param receipt_template: Excel file to use as template for making receipts
    :param email_server: smtplib server instance (smtlib.SMTP) for sending emails
    :return:
    '''


    # Create flight IDs
    landing_flights = flights.loc[flights.submission_type == 'landings']\
        .dropna(axis=1, how='all') # columns for all track submissions will be empty so drop them
    landing_flights.rename(columns=LANDINGS_FLIGHT_COLUMNS, inplace=True)
    landing_flights['flight_id'] = landing_flights.registration + '_' + \
                                   landing_flights.departure_datetime.dt.floor('15min').dt.strftime('%Y%m%d%H%M')
    existing_flight_ids = pd.read_sql("SELECT flight_id FROM flights", landings_conn).squeeze(axis=1)
    new_flights = landing_flights.loc[~landing_flights.flight_id.isin(existing_flight_ids) &
                                      ~landing_flights.departure_datetime.isnull()]

    # Check if there are any new flights. If not, warn the submitter(s) that no new flights were found
    if len(new_flights) == 0:
        #### send warnings/errors to specific users
        #### I should probably alter this so there's a msg, recipients, and loglevel column
        ####### I might need to make separate functions for different error messages
        MESSAGES.append(['All landings already reported', 'user_warn'])
    elif len(new_flights) != len(landing_flights):
        duplicate_flights = landing_flights.loc[~landing_flights.flight_id.isin(new_flights.flight_id)]
        ########### create a table of just duplicated flights to send to Alex
    else:
        new_flights['submission_method'] = 'survey123'
        new_flights['source_file'] = sqlite_path
        new_flights.reindex(columns=LANDINGS_FLIGHT_COLUMNS.values())\
            .to_sql('flights', landings_conn, if_exists='append', index=False)
        global_ids = pd.read_sql("SELECT id, agol_global_id FROM flights WHERE flight_id IN ('%s')"
                                 % "', '".join(new_flights.flight_id),
                                 landings_conn)

        #def get_landing_order(flight):

        # Calculate concessions fees and attach numeric IDs to landings and fees
        # some landings might have multiple landing types selected so split them
        landings['sort_order'] = range(len(landings))
        passenger_cols = pd.Series([c for c in landings.columns if c.startswith('n_passengers')])

        def clean_n_passenger_cols(row, cols):
            row.loc[cols.loc[~cols.str.endswith(row.landing_type)]] = np.nan
            return row

        landings['landing_type'] = landings.landing_type.str.split(',')
        landings = pd.DataFrame([clean_n_passenger_cols(row, passenger_cols) for i, row in landings.explode('landing_type').iterrows()]).reset_index()
        landings['index'] = landings.index
        landings_with_fees = pd.read_sql("SELECT name FROM landing_types WHERE fee_charged;", landings_conn).squeeze(
            axis=1).tolist()

        passengers = landings\
            .melt(id_vars='globalid',
                  value_vars=passenger_cols,
                  value_name='n_passengers',
                  var_name='landing_type')\
            .dropna(subset=['n_passengers'])#'''
        passengers['landing_type'] = passengers.landing_type.apply(lambda x: x.split('_')[-1])
        fee_passengers = passengers.loc[passengers.landing_type.isin(landings_with_fees)]

        # Get the parentglobalid (from AGOL) so fees can be calculated per flight
        fee_passengers['index'] = fee_passengers.index
        fee_passengers['parentglobalid'] = fee_passengers.merge(landings.drop_duplicates(subset=['globalid']), on='globalid').set_index('index_x').parentglobalid
        fees = fee_passengers.groupby('parentglobalid').sum().reset_index()
        fees['fee'] = fees.n_passengers * LANDING_FEE
        fees['flight_id'] = fees.merge(global_ids, left_on='parentglobalid', right_on='agol_global_id').id
        fees = fees.loc[~fees.flight_id.isnull()] # drop flights without match

        # Get the flight ID for the landings table
        landings['flight_id'] = landings.merge(global_ids, left_on='parentglobalid', right_on='agol_global_id').id

        landings = passengers.merge(landings.drop(columns='landing_type'), on='globalid')\
            .drop_duplicates(subset=['globalid', 'landing_type'])\
            .rename(columns=LANDINGS_COLUMNS)\
            .reindex(columns=LANDINGS_COLUMNS.values())\
            .dropna(subset=['flight_id'])\
            .sort_values('sort_order')

        #import pdb; pdb.set_trace()
        #new_flights.loc[new_flights.ticket == 319].merge(global_ids, on='agol_global_id').drop(columns=['flight_id'])
        # INSERT into backend
        fees.drop(columns=['parentglobalid', 'index'])\
            .to_sql('concession_fees', landings_conn, if_exists='append', index=False)
        landings.drop(columns='sort_order')\
            .to_sql('landings', landings_conn, if_exists='append', index=False)#'''

        # Format the table for the receipt(s)
        # Replace codes with names
        new_flights.reset_index(inplace=True)
        new_flights['id'] = new_flights.merge(global_ids.reset_index(), on='agol_global_id').id
        aircraft_types = db_utils.get_lookup_table(table='aircraft_types', conn=landings_conn)
        operators = db_utils.get_lookup_table(table='operators', conn=landings_conn)
        new_flights.replace({'aircraft_type': aircraft_types, 'operator_code': operators}, inplace=True)
        landing_receipts = {}
        for ticket, flights in new_flights.groupby('ticket'):
            report, info = make_pretty_landing_report(ticket, flights, landings, fees)
            receipt_path = save_excel_receipt(receipt_template, receipt_dir, ticket, 'landing_data', report, info)
            landing_receipts[ticket] = receipt_path

        import pdb; pdb.set_trace()

def prepare_track(track_path, attachment_info, import_params, submissions, submitter, operator_codes, mission_codes, attachment_dir):
    '''Prepare the track file to be read by the editing web app (inpdenards.nps.doi.net/track-editor.html). It needs to
    be in format that both the web app and the rest of import_track functions will understand'''

    fname = os.path.basename(track_path)

    try:
        gdf = import_track.format_track(track_path,
                                        registration=attachment_info['tracks_tail_number'],
                                        submission_method='survey123',
                                        **import_params)
    except Exception as e:
        MESSAGES.append(['Error on file %s: %s' % (fname, e), 'warn_track'])

    if not len(gdf):
        MESSAGES.append(['The file %s did not contain any readable tracks' % fname, 'warn_user'])
        return

    # Get submission info
    this_agol_id = attachment_info['REL_GLOBALID'].replace('{', '').replace('}', '')
    for k, v in submissions.loc[submissions.globalid == this_agol_id].squeeze().iteritems():
        # most object types can't be serialized as JSON, so make them strs
        attachment_info[k] = str(v) if v and v != 0 else ''
    attachment_info['submitter'] = submitter
    attachment_info['submitter_notes'] = attachment_info['tracks_notes']

    # Set the operator code to the operator name, not the code (easier to read for editors). The web app
    #   will replace the name with the code on import
    if attachment_info['tracks_operator'] in operator_codes:
        attachment_info['operator_code'] = operator_codes[attachment_info['tracks_operator']]
    else:
        attachment_info['operator_code'] = ''

    if attachment_info['tracks_mission'] in mission_codes:
        attachment_info['nps_mission_code'] = attachment_info[
            'tracks_mission']  # , mission_codes[attachment_info['tracks_mission']]
    else:
        attachment_info['nps_mission_code'] = ''

    attachment_info['source_file'] = os.path.join(import_track.ARCHIVE_DIR, fname)

    # Dump each line segment to a geojson string. Write a list of these strings as a JSON file to be read
    #   by the web app
    geojson_strs = track_to_json(gdf, attachment_info)
    basename, _ = os.path.splitext(fname)
    basename = re.sub('\.|#', '_', basename) # strip chars for Javascript (i.e., escape and CSS selectors)
    with open(os.path.join(attachment_dir, basename) + '_geojsons.json', 'w') as j:
        json.dump(geojson_strs, j, indent=4)


def prepare_track_data(param_dict, download_dir, tracks_conn, submissions):

    import_params = param_dict['import_params'] if 'import_params' in param_dict else {}
    attachment_dir = os.path.join(download_dir, 'attachments')
    operator_codes = db_utils.get_lookup_table(table='operators', conn=tracks_conn)
    mission_codes = db_utils.get_lookup_table(table='nps_mission_codes', conn=tracks_conn)
    import pdb; pdb.set_trace()

    # Find all the files that were downloaded without being zipped. This will only include files with valid extensions
    #   because Survey123 will reject all others
    for ext in import_track.READ_FUNCTIONS:
        for path in glob(os.path.join(attachment_dir, '*%s' % ext)):
            attachment_info = get_attachment_info(path, ext)
            submitter = get_submitter_email(attachment_info['Creator'], param_dict['agol_users'])
            prepare_track(path, attachment_info, import_params, submissions, submitter, operator_codes, mission_codes)

    # Unzip all zipped files and add the extracted files to the list of tracks to process
    for zip_path in glob(os.path.join(attachment_dir, '*.zip')):

        attachment_info = get_attachment_info(zip_path)
        submitter = get_submitter_email(attachment_info['Creator'], param_dict['agol_users'])
        with zipfile.ZipFile(zip_path) as z:
            for fname in z.namelist():
                try:
                    z.extract(fname, attachment_dir)
                except Exception as e:
                    MESSAGES.append(['Could not extract {file} from {zip} because {error}. You should contact the'
                                     ' submitter: {submitter}'
                                    .format(file=fname,
                                            zip=zip_path,
                                            error=e,
                                            submitter=submitter),
                                     'warn_track'])

                basename, ext = os.path.splitext(fname)
                if ext not in import_track.READ_FUNCTIONS:
                    MESSAGES.append(['%s is not in an accepted file format', 'warn_user'])
                    continue
                prepare_track(path, attachment_info, import_params, submissions, submitter, operator_codes, mission_codes)


def poll_feature_service(log_dir, download_dir, param_dict, ssl_cert, landings_conn, tracks_conn):

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
    agol_credentials = param_dict['agol_credentials']#download.read_credentials(agol_credentials_json)
    service_url = agol_credentials['service_url']
    token = download.get_token(**agol_credentials, ssl_cert=ssl_cert)
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
    last_submission_time = datetime.fromtimestamp(submissions.EditDate.max()/1000.0) # timestamps are in local tz, surprisingly

    # If the last_sumbission_time was before the last_download_time, these data have already been downloaded
    if last_submission_time < datetime.strptime(last_download_time, TIMESTAMP_FORMAT):
        log_and_exit(log_info, [log_dir, download_dir, timestamp, '', False, 'No submission since last download'])


    # If the last submission time was within SUBMISSION_TICKET_INTERVAL, exit in case a user is making multiple
    #   submission at once that should be grouped into a single ticket. It's possible that two different users could 
    #   be submitting within the SUBMISSION_TICKET_INTERVAL of one another, but it's not worth the overhead and 
    #   management to download data submitted by one user and not another. Their submissions will be assigned different
    #   ticket numbers, but just wait until there's a gap of all submissions of SUBMISSION_TICKET_INTERVAL. 
    if (datetime.now() - last_submission_time).seconds < SUBMISSION_TICKET_INTERVAL:
        log_and_exit(log_info, [log_dir, download_dir, timestamp, '', False,
                                'Last submission was within %d minutes' % (SUBMISSION_TICKET_INTERVAL/60)])#'''

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
    submissions['submission_time'] = [datetime.fromtimestamp(round(ts/1000)) for _, ts in submissions['CreationDate'].iteritems()]
    submissions.rename(columns={'Creator': 'submitter'}, inplace=True)

    connection_dict = {'tracks': tracks_conn, 'landings':landings_conn}
    submissions = pd.concat([get_ticket(g, connection_dict)
                             for _, g in submissions.groupby(['submitter', 'submission_type'])])

    # Process and import the landing data. The flight tracks will need to be unzipped, but they need to be
    #   validated/edited before they can be imported. This is handled manually via a separate web app
    main_table_name = pd.DataFrame(service_info['layers']).set_index('id').loc[service_info['layers'][0]['id'], 'name']
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
        merged = all_flights.merge(submissions, on='globalid')
        all_flights['ticket'] = merged.ticket
        all_flights['submission_time'] = merged.submission_time
        all_flights.to_sql(main_table_name, conn, if_exists='replace')
        landings = pd.read_sql_table('landing_repeat', conn)

    # If there were any landings submitted, import them into the database
    receipt_dir = os.path.join(log_dir, 'receipts')
    if (submissions.submission_type == 'landings').any():
        import_landings(all_flights, landings_conn, sqlite_path, landings, receipt_dir)

    # Pre-process track data
    if (submissions.submission_type == 'tracks').any():
        prepare_track_data(param_dict, download_dir, tracks_conn, submissions)

        # Send email to track editors
        msg =\
            '''There are new track data to edit and import into the overflights 
            database. Go to {url} 
            in a Google Chrome window to view and edit the tracks.'''.format(url=param_dict['track_editor_url'])
        subject = 'New track data submitted on %s' % datetime.now().strftime('%b %d, %Y')
        recipients = param_dict['track_data_stewards']
        #process_emails.send_email(msg, subject, param_dict['mail_sender'], recipients, server)

    import pdb; pdb.set_trace()




def main(config_json):
    '''
    Ensure that a log file is always written (unless the error occurs in write_log(), which is unlikely)
    '''
    #try:
    params = read_json_params(config_json)
    # Open connections to the DBs and begin transactions so that if there's an exception, no data are inserted
    connection_info = params['db_credentials']
    connection_template = 'postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'
    landings_engine = create_engine(connection_template.format(**connection_info['landings']))
    tracks_engine = create_engine(connection_template.format(**connection_info['tracks']))

    with landings_engine.connect() as l_conn, tracks_engine.connect() as t_conn, l_conn.begin(), t_conn.begin():
        poll_feature_service(params['log_dir'], params['download_dir'], params, params['ssl_cert'], l_conn, t_conn)

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
        log_file = write_log(log_dir, download_dir, timestamp, '', False, traceback_details)'''

    # send emails here so that all DB transactions are committed before actually sending anything
    # still wrap in try/except block so if any emails fail, someone can be notified (and the log can be amended)
    # Start the mail server
    server = smtplib.SMTP(params['mail_server_credentials']['server_name'], params['mail_server_credentials']['port'])
    server.starttls()
    server.ehlo()

    failed_emails = []
    for msg_info in MESSAGES:
        try:
            process_emails.send_email(**msg_info)
        except Exception as error:
            msg_info['error'] = error
            failed_emails.append(msg_info)
    # If any failed, add them to the log file
    if len(failed_emails):
        log_file = ''
        with open(log_file) as j:
            log = json.load(j)
        log['failed_emails'] = failed_emails
        with open(log_file) as j:
            json.dumps(log, j, indent=4)
            

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))