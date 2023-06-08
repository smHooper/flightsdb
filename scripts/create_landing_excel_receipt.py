from poll_feature_service import *

def main(poll_feature_service_param_path, output_dir, flight_ids=None, tickets=None):
    params = read_json_params(poll_feature_service_param_path)
    connection_dict = params['db_credentials']['landings'];
    engine = create_engine('postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'.format(**connection_dict))

    where_stmt = f'WHERE id IN ({flight_ids})' if len(flight_ids) else f'WHERE ticket IN ({tickets})'
    flights = pd.read_sql(f'SELECT * FROM flights {where_stmt}', engine)
    flight_ids = ','.join(flights.id.astype(str))
    landings = pd.read_sql(f'SELECT * FROM landings WHERE flight_id IN ({flight_ids})', engine)
    operators = pd.read_sql('SELECT * FROM operators', engine)
    fees = pd.read_sql(f'SELECT * FROM concession_fees_view WHERE flight_id IN ({flight_ids})', engine)

    receipt_paths = []
    for ticket, submission in flights.groupby('ticket'):
        operator_code = submission.operator_code.iloc[0]
        contract = operators.loc[operators.code == operator_code, 'contract']
        fee_per_passenger = submission.fee_per_passenger.iloc[0] #all the same for a single ticket
        receipt, info = format_landing_excel_receipt(
            submission.ticket.iloc[0],
            submission,
            landings,
            fees.loc[fees.flight_id.isin(submission.id)],
            contract,
            fee_per_passenger
        )

        excel_receipt_params = params['landing_receipt']
        output_path = save_excel_receipt(
            excel_receipt_params['template'],
            output_dir,
            ticket,
            'landing_data',
            receipt,
            excel_receipt_params['header_img'],
            excel_receipt_params['sheet_password'],
            info
        )
        receipt_paths.append(output_path)

    print(f'{len(receipt_paths)} receipts saved to {output_dir}')


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))