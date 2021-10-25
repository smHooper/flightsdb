"""
Manual entry point into workflow to process/import flight data that were downloaded. This script, 
however, will not send email receipts/notfications

Usage:
    process_sqlite_file.py <sqlite_path> <config_json> [--overwrite_attachments] [--object_ids=<str>] 

Examples:
    process_sqlite_file.py ..\poll_feature_service_params\dena_flight_data_query_view_20211025-104441.db ..\config\poll_feature_service_params.json
    process_sqlite_file.py dena_flight_data_query_view_20211025-104441.db ..\config\poll_feature_service_params.json --overwrite_attachments
    process_sqlite_file.py dena_flight_data_query_view_20211025-104441.db ..\config\poll_feature_service_params.json --object_ids=189,190


Required parameters:
    sqlite_path     Path of the SQLite database file to process
    config_json     Configuration file passed to poll_feature_service.py

Options:
    -h, --help                      Show this screen.
    -o, --overwrite_attachments     Boolean flag indicating whether or not to overwrite attachments 
                                    if they already exist
    -i, --object_ids=<str>          Comma-separated list of object IDs from the objectid field of 
                                    the SQLite file. Only rows with an ID specified in this list 
                                    will be processed
"""


import sqlalchemy
from poll_feature_service import * # all modules and constants impored in poll_feature_service
from utils import get_cl_args

def main(sqlite_path, config_json, overwrite_attachments=False, object_ids=None):

    params = read_json_params(config_json)

    agol_credentials = params['agol_credentials']
    service_url = agol_credentials['service_url']
    ssl_cert = params['ssl_cert']
    download_dir = params['download_dir']

    engine = create_engine('sqlite:///' + sqlite_path)
    with engine.connect() as conn:
        all_flights = pd.read_sql_table('flights', conn)
        attachments = pd.read_sql_table('attachments', conn)
        if 'landing_repeat' in sqlalchemy.inspect(engine).get_table_names():
            all_landings = pd.read_sql_table('landing_repeat', conn)
        else:
            all_landings = pd.DataFrame()

        all_flights.landing_datetime = (all_flights.landing_datetime/1000).dropna().apply(datetime.fromtimestamp)

    # If specific IDs (for the flights table) were passed, only process those
    if object_ids:
        if type(object_ids) == str:
            try:
                object_ids = [int(id_str.strip()) for id_str in object_ids.split(',')]
            except Exception as e:
                raise ValueError(f'Could not parse object_ids string {object_ids} because {e}')
        all_flights = all_flights.loc[all_flights.objectid.isin(object_ids)]
        global_ids = all_flights.globalid
        attachments = attachments.loc[attachments.parentglobalid.isin(global_ids)]
        if len(all_landings):
            all_landings.loc[all_landings.parentglobalid.isin(global_ids)]

    # Open connections to the DBs and begin transactions so that if there's an exception, no data are inserted
    connection_info = params['db_credentials']
    connection_template = 'postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'
    landings_engine = create_engine(connection_template.format(**connection_info['landings']))
    tracks_engine = create_engine(connection_template.format(**connection_info['tracks']))

    # write the attachments to disk
    attachment_dir = os.path.join(download_dir, 'attachments')
    if not os.path.isdir(attachment_dir):
        os.makedirs(attachment_dir)

    for _, file_info in attachments.iterrows():
        attachment_path = os.path.join(attachment_dir, file_info['name'])
        if os.path.isfile(attachment_path) and overwrite_attachments:
            continue
        _, extension = os.path.splitext(attachment_path)

        # Write track file
        with open(attachment_path, 'wb') as f:
            f.write(file_info.content)

        # WRite track info
        row_dict = file_info.to_dict()
        row_dict['parent_table_name'] = 'flights'
        row_dict['REL_GLOBALID'] = file_info.parentglobalid # This is what's called in the createReplica result so just make it consistent
        row_dict['sqlite_path'] = sqlite_path
        del row_dict['content']
        json_path = attachment_path.rstrip(extension) + '.json'
        with open(json_path, 'w') as json_pointer:
            json.dump(row_dict, json_pointer, indent=4)


    
    with landings_engine.connect() as landings_conn, tracks_engine.connect() as tracks_conn, landings_conn.begin(), tracks_conn.begin():


        # Get operator codes from usernames/submitter
        operator_codes = db_utils.get_lookup_table(table='operators', index_col='code', value_col='agol_username', conn=landings_conn)

        submissions = all_flights.copy()

        # Create separate tickets for each user for both landing and track data
        submissions['submission_time'] = [datetime.fromtimestamp(round(ts/1000)) for _, ts in submissions['CreationDate'].items()]
        submissions.rename(columns={'Creator': 'submitter'}, inplace=True)
        submissions.loc[submissions.submitter == '', 'submitter'] = [operator_codes[o] for _, o in submissions.loc[submissions.submitter == '', 'operator'].items()]

        connection_dict = {'tracks': tracks_conn, 'landings':landings_conn}
        submissions = pd.concat([get_ticket(g, connection_dict)
                                 for _, g in submissions.groupby(['submitter', 'submission_type'])])
        merged = all_flights.merge(submissions, on='globalid')
        all_flights['ticket'] = merged.ticket
        all_flights['submission_time'] = merged.submission_time

        # Check for any flights with landings submitted via Excel. Need to do this inside the with/as 
        #   block because process_excel_landings() references the landings_conn connection. Also the submission
        #   needs to be created in the landings database
        all_flights['is_from_excel'] = False
        excel_landing_flights = all_flights.loc[all_flights.landings_submission_type == 'excel']\
            .merge(attachments, left_on='globalid', right_on='parentglobalid')
        if len(excel_landing_flights):
            for _, info in excel_landing_flights.iterrows():
                excel_path = os.path.join(download_dir, 'attachments', info['name'])
                try:
                    excel_flights, excel_landings = process_excel_landings(
                        excel_path,
                        landings_conn,
                        info,
                        params['landing_data_stewards'],
                        all_flights.columns,
                        error_handling='raise',
                        **params['excel_landing_template']
                    )
                except Exception as e:
                    raise RuntimeError('Could not process {excel_path} because {error}'.format(excel_path=excel_path, error=e))

                # If there were errors, the flights and landing DataFrames will be empty
                if len(excel_flights):
                    excel_landings.CreationDate = info.CreationDate
                    all_flights = pd.concat([all_flights, excel_flights], ignore_index=True)
                    all_landings = pd.concat([all_landings, excel_landings], ignore_index=True)

                # Remove the flight record that just had the Excel file because it
                all_flights = all_flights.loc[all_flights.objectid != info.objectid]

            # sqlite treats ISO datetimes as string, so explicitly cast as pandas datetime
            all_flights.landing_datetime = pd.to_datetime(all_flights.landing_datetime)

        # Get operator emails from landings DB
        operator_emails = db_utils.get_lookup_table(table='operators', index_col='agol_username', value_col='email',
                                                    conn=landings_conn)
        params['agol_users'] = operator_emails
        submitter_emails = submissions.groupby(['ticket', 'submission_type']).first() \
            .apply(lambda r: get_submitter_email(r.submitter, params['agol_users']), axis=1)

        submissions['tracks_operator'] = submissions.loc[submissions.submission_type == 'tracks', 'operator'].fillna(
            'NPS')
        all_flights['tracks_operator'] = all_flights.loc[all_flights.submission_type == 'tracks', 'operator']
        all_flights['landing_operator'] = all_flights.loc[all_flights.submission_type == 'landings', 'operator']
        # The submitter being logged in *and* the username ending in _nps is the only way any non-operator could submit
        #   data. The operator field will be blank though, so fill it in
        all_flights.operator.fillna('NPS', inplace=True)

        # If there were any landings submitted, import them into the database
        receipt_dir = os.path.join(params['log_dir'], 'receipts')
        receipt_params = params['landing_receipt']

        if (submissions.submission_type == 'landings').any():
            # Try to import 1 ticket at a time so submissions from all users aren't affected by an error from 1 submitter
            _html_receipt_columns = ['departure_datetime', 'scenic_route', 'registration', 'aircraft_type', 'submission_time', 'objectid']
            for ticket, flights in all_flights.loc[all_flights.submission_type == 'landings'].groupby('ticket'):
                landings = all_landings.loc[all_landings.parentglobalid.isin(flights.globalid)]
                try:
                    excel_path, html_df, imported_flights = \
                        import_landings(flights, ticket, landings_conn, sqlite_path, landings, receipt_dir,
                                        receipt_params['template'], receipt_params['header_img'],
                                        receipt_params['sheet_password'], params['landing_data_stewards'])
                except Exception as e:
                        tb_frame = get_traceback_frame()
                        message = ('An unexpected error, "{error}", occurred on line {lineno} of {script} while '
                                   'processing landing data with object IDs {object_ids}'
                                   .format(error=e, lineno=tb_frame.lineno, script=os.path.basename(tb_frame.filename), object_ids=', '.join(landings.objectid))
                                   )
                        ERRORS.append(message)
                        continue#'''

                if not excel_path: # There weren't any new landings
                    continue

                agol_ids = imported_flights.agol_global_id
                failed_object_ids = []
                try:
                    if 'token' not in locals():
                        token = download.get_token(ssl_cert=ssl_cert, **agol_credentials)
                    failed_object_ids = delete_from_feature_service(agol_ids, token, service_url, ssl_cert)
                except Exception as e:
                    message = 'The following error occurred while trying to delete features with AGOL IDs {agol_ids}, "{error}. These features will have to be manually deleted"'\
                            .format(agol_ids=', '.join(agol_ids.astype(str)), error=e)
                    ERRORS.append(message)

                if len(failed_object_ids):
                    message = ('Unable to delete features with Object IDs {object_ids}')\
                            .format(object_ids=', '.join(failed_object_ids.astype(str)))
                    ERRORS.append(message)

        # Pre-process tracks
        track_submissions = submissions.loc[submissions.submission_type == 'tracks']
        submitted_files = pd.Series(dtype=object)
        if len(track_submissions):
            geojson_paths, submitted_files = prepare_track_data(params, download_dir, tracks_conn, submissions)

        if len(submitted_files):
            try:
                failed_object_ids = delete_from_feature_service(submitted_files.index, token, service_url, ssl_cert)
                if len(failed_object_ids):
                    ERRORS.append('Unable to delete the following features: %s. These should be manually deleted.' % failed_object_ids.astype(str))
            except Exception as e:
                ERRORS.append('Unable to delete the following features: %s. These should be manually deleted.' % submitted_files.index.tolist())

        if len(ERRORS):
            break_str = '#' * 50
            print('All data from {sqlite_path} processed except for the following errors:\n\n{break_str}\n{errors}'.format(sqlite_path=sqlite_path, break_str=break_str, errors=('\n%s\n' % break_str).join(ERRORS)))
        else:
            print('All data from {} successfully processed'.format(sqlite_path))



if __name__ == '__main__':
    args = get_cl_args(__doc__)
    sys.exit(main(**args))