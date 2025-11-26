"""
Manually import an Excel file directly into the scenic landings database. 

Usage:
    process_sqlite_file.py <excel_path> <submission_time> <config_json> [--ignore_warnings]

Examples:
    process_sqlite_file.py ..\poll_feature_service_logs\landings.excel '2024-6-1 11:00' '..\config\poll_feature_service_params.json


Required parameters:
    excel_path      Path of the Excel file to process
    submission_time ISO-8601 format timestamp that the Excel file was submitted
    config_json     Configuration file passed to poll_feature_service.py

Options:
    -h, --help              Show this screen.
    -i, --ignore_warnings  Prevent warnings from 
"""


from sqlalchemy import create_engine
from poll_feature_service import * # all modules and constants imported in poll_feature_service
from utils import get_cl_args
from warnings import filterwarnings

AGOL_FLIGHT_COLUMNS = [
    'objectid', 
    'globalid', 
    'submission_type', 
    'landing_operator', 
    'landing_datetime', 
    'landing_route', 
    'landing_tail_number', 
    'landing_aircraft_type', 
    'landing_other_aircraft', 
    'landing_flight_notes', 
    'CreationDate', 
    'Creator', 
    'EditDate', 
    'Editor', 
    'username',  
    'status', 
    'operator', 
    'landings_submission_type',
    'operator_code',
    'ticket'
]


if __name__ == '__main__':

    # Wrap the whole thing a try/except block to trap unexpected errors
    try:
        args = get_cl_args(__doc__)
        excel_path = args['excel_path']
        submission_time = args['submission_time']

        if args['ignore_warnings']:
            # Ignore openpyxl and pandas warnings. This suppresses all warnings, but they're 
            #   inconsequential to the web client anyway
            filterwarnings('ignore')

        with open(args['config_json']) as f:
            config = json.load(f)

        connection_info = config['db_credentials']
        engine = create_engine('postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'.format(**connection_info['landings']))

        # Get the submitter from the excel file
        with engine.connect() as conn, conn.begin():
            # Get operator code
            operator_info = pd.read_excel(excel_path, sheet_name='info')
            operator_code = operator_info.iloc[0].operator_code
            operator_agol_username = pd.read_sql(f""" SELECT agol_username FROM operators WHERE code='{operator_code}' """, conn).squeeze()
            operator_name = operator_info.operator_name.iloc[0]

            submission_info = pd.DataFrame([{
                'submission_type': 'landings', 
                'submission_time': submission_time,
                'submitter': operator_agol_username,
                'parentglobalid': str(uuid.uuid4()) #make a dummy ID so it's compatible with AGOL submissions
            }])
            submission_info = get_ticket(submission_info, {'landings': conn})

            flights, landings = process_excel_landings(excel_path, conn, submission_info, config['landing_data_stewards'], AGOL_FLIGHT_COLUMNS, error_handling='raise', data_sheet_name='landings') # TODO: errors arent' returned here
            flights['is_from_excel'] = True
            flights['operator_code'] = operator_code
            ticket = submission_info.ticket.iloc[0]
            flights['ticket'] = ticket

            messages = pd.DataFrame(MESSAGES)
            errors = messages.loc[messages.level == 'error']
            if len(errors):
                print({'errors': '\n'.join(errors.message.tolist())})
                conn.rollback()
                sys.exit()

            receipt_dir = os.path.join(config['log_dir'], 'receipts')
            receipt_params = config['landing_receipt']

            try:
                import_landings(flights, ticket, conn, '', landings, receipt_dir, receipt_params['template'], receipt_params['header_img'], receipt_params['sheet_password'], config['landing_data_stewards'], submission_method='email')
            except Exception as e:
                tb_frame = get_traceback_frame()
                message = (
                    '<li>An unexpected error, "{error}", occurred on line {lineno} of {script} while '
                    'processing landing data</li>'
                       .format(error=e, lineno=tb_frame.lineno, script=os.path.basename(tb_frame.filename))
                )
                print(json.dumps({'errors': message}))
                conn.rollback()
                sys.exit()

            # Print to stdout to rely back to PHP
            #   The result gives the parameters to be able to 
            messages = pd.DataFrame(MESSAGES)
            warnings = messages.loc[messages.level == 'warning']
            print(json.dumps(
                {
                    'errors': None, 
                    'ticket': int(ticket),
                    'operator_name': operator_name,
                    'warnings': '\n'.join(warnings.message.tolist())
                }
            ))
            
    except Exception as e:
        tb_frame = get_traceback_frame()
        message = (
            '<li>An unexpected error, "{error}", occurred on line {lineno} of {script} while '
            'processing landing data</li>'
               .format(error=e, lineno=tb_frame.lineno, script=os.path.basename(tb_frame.filename))
        )
        print(json.dumps({'error': message}))