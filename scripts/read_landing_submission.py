import sys, os
import pandas as pd
import db_utils

FLIGHT_TBL_COLS = ['operator_code', 'departure_datetime', 'aircraft_type']
FEE_TBL_COLS = ['scenic_route', 'n_passengers', 'fee']

def validate_landings(df):

    errors = []

    # date is empty
    if df.submission_date.isnull().any():
        # send error email
        errors.append("A submission date was not given")

    df['row_str_id'] = df.date.dt.strftime('%m/%d/%y ') + \
                       df.time.apply(lambda x: x.strftime('%I:%M')) + \
                       pd.Series(df.index).apply(lambda x: ' (row %s)' % x)

    for i, row in df.drop('n_passengers', axis=1).iterrows():
        location_cols = row.reindex([c for c in df if c.startswith('location_')]).dropna().index
        passenger_cols = row.reindex([c for c in df if c.startswith('n_')]).dropna().index

        # Check that at least one landing location/passenger count has been recorded per row
        if not len(location_cols):
            errors.append('No landing location given for the flight recorded as departing {row_str_id}'.format(**row))
        if not len(passenger_cols):
            errors.append('No passenger count given for the flight recorded as departing {row_str_id}'.format(**row))

        # Check that for each passenger count there's a location and vice versa
        missing_locations = passenger_cols[~passenger_cols.isin([c.replace('location_', 'n_') for c in location_cols])]
        missing_passengers = location_cols[~location_cols.isin([c.replace('n_', 'location_') for c in passenger_cols])]
        if len(missing_locations):
            errors.append('For the flight departing %s, the landings at the following locations were missing a location: %s' % (row.row_str_id, ', '.join(missing_locations)))
        if len(missing_passengers):
            errors.append('For the flight departing %s, the landings at the following locations were missing passenger counts: %s' % (row.row_str_id, ', '.join(missing_passengers)))

    # Check that there's a justification given for any scenic landings that require it
    scenic_landings = df.dropna(subset=['location_scenic'])
    needs_justification = scenic_landings.loc[scenic_landings.location_scenic.apply(lambda x: 'Provide justification' in x) & scenic_landings.justification.isnull()]
    if len(needs_justification):
        errors.append('Flights that departed at the following times need justification for scenic landing'
                      ' locations: %s' % ', '.join(needs_justification.row_str_id))

    if len(errors):
        raise ValueError('Invalid or missing values found in file:\n\t-%s' % '\n\t-'.join(errors))


def import_data(landings_path, connection_txt, sheet_name='data'):

    df = pd.read_excel(landings_path, sheet_name)

    # The data sheet has formulas for the first 2000 rows, which pandas reads as blank data, so drop those rows
    df.drop(df.index[df.isnull().all(axis=1)], inplace=True)
    validate_landings(df.copy())

    # can't parse_dates when reading file because there's an extra '00:00:00' in the date for some stupid reason
    df['departure_datetime'] = pd.to_datetime(df.date.dt.strftime('%Y-%m-%d ') + df.time.apply(str))
    df.drop(['date', 'time'], axis=1, inplace=True)

    # Create a column with a unique value per flight (i.e., row)
    engine = db_utils.connect_db(connection_txt)
    operator_codes = db_utils.get_lookup_table(engine, 'operators', index_col='name', value_col='code')
    df.replace({'operator_code': operator_codes}, inplace=True)
    df['flight_identifier'] = df.apply(lambda row: '{operator}_{aircraft}_{departure}'
                                       .format(operator=row.operator_code,
                                               aircraft=row.aircraft_type.replace(' ', ''),
                                               departure=row.departure_datetime.strftime('%Y%m%d_%H%M')),
                                       axis=1)

    # Separate the flight info from landing data
    flights = df.loc[:, ['flight_identifier'] + FLIGHT_TBL_COLS]
    fees = df.loc[:, ['flight_identifier'] + FEE_TBL_COLS]
    landing_data = df.drop(FLIGHT_TBL_COLS + FEE_TBL_COLS, axis=1)

    flights['time_submitted'] = pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    flights['submission_method'] = 'email'

    # Transpose the landing data such that each landing/dropoff/pickup is its own line
    landings = landing_data.melt(id_vars=[c for c in landing_data if not c.startswith('n_')],
                                 value_name='n_passengers')\
        .dropna(subset=['n_passengers'])
    landings['landing_type'] = landings.variable.apply(lambda x: x.split('_')[1])
    landings['location'] = [landings.loc[i, c.replace('n_', 'location_')] for i, c in landings.variable.iteritems()]
    location_cols = [c for c in landing_data if c.startswith('location_')]
    landings.drop(location_cols + ['variable'], axis=1, inplace=True)
    landings.location = landings.location.apply(lambda x: x.split(' - ')[0])

    with engine.connect() as conn, conn.begin():
        # Insert only new flights because if there were already tracks for these flights that were already processed,
        #   the flights are already in the DB
        existing_flight_ids = pd.read_sql("SELECT flight_identifier FROM flights", conn).squeeze()
        flights.loc[~flights.flight_identifier.isin(existing_flight_ids)].to_sql('flights', conn, if_exists='append', index=False)

        # Get the IDs of flights that were just inserted
        flight_ids = pd.read_sql("SELECT id, flight_identifier FROM flights WHERE flight_identifier IN ('%s')"
                                 % "', '".join(flights.flight_identifier),
                                 conn)
        fees = fees.merge(flight_ids, on='flight_identifier').rename(columns={'id': 'flight_id'})
        fees.loc[~fees.flight_identifier.isin(existing_flight_ids)]\
            .drop('flight_identifier', axis=1)\
            .to_sql('concession_fees', conn, if_exists='append', index=False)
        landings = landings.merge(flight_ids, on='flight_identifier').rename(columns={'id': 'flight_id'})

        # Remove any notes that were intended for data entry, then replace location names with codes
        locations = pd.read_sql_table('landing_locations', conn)
        location_codes = locations.set_index('name').code.to_dict()#db_utils.get_lookup_table(engine, 'landing_locations', index_col='name', value_col='code')
        scenic_location_notes = locations.dropna(subset=['scenic_data_entry_note'])
        scenic_notes = scenic_location_notes.set_index(scenic_location_notes.apply(lambda x: '%s - %s' % (x['name'], x.scenic_data_entry_note), axis=1))['name'].to_dict()
        taxi_location_notes = locations.dropna(subset=['taxi_data_entry_note'])
        taxi_notes = taxi_location_notes.set_index(taxi_location_notes.apply(lambda x: '%s - %s' % (x['name'], x.taxi_data_entry_note), axis=1))['name'].to_dict()

        landings = landings.replace({'location': scenic_notes})\
            .replace({'location': taxi_notes})\
            .replace({'location': location_codes})

        # Remove extraneous columns from landing data before inserting
        landings_cols = pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'landings'",
                                    conn).squeeze()
        landings[[c for c in landings if c in landings_cols.values]].to_sql('landings', conn, if_exists='append', index=False)


if __name__ == '__main__':
    sys.exit(import_data(*sys.argv[1:]))