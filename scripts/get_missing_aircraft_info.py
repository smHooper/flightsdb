import sys
import re
import bs4
import docopt
import requests
import warnings
import pandas as pd

import db_utils
#from import_track import get_cl_args

def get_aircraft_info(registration, ssl_cert):
    """
    Scrape the FAA registry for info about an aircraft from its registration (N-) number

    :param registration: N-number of the aircraft
    :param ssl_cert: path to an SSL .crt or .pem file
    :return: pd.Series of aircraft info (index is the property name)
    """

    faa_url = 'https://registry.faa.gov/aircraftinquiry/NNum_Results.aspx?NNumbertxt={}'
    response = requests.get(faa_url.format(registration),
                            verify=ssl_cert)
    response.raise_for_status() # exception should be caught when calling this function
    soup = bs4.BeautifulSoup(response.text, features='html5lib')

    # remove special characters from what will be field names and strip trailing spaces from values
    keys = [re.sub('[^\w0-9a-zA-Z]+', '_', k.getText()).lower() for k in soup.find_all(class_='Results_DataLabel')]
    values = [v.getText().strip() for v in soup.find_all(class_='Results_DataText')]

    # There should be the same number of keys and values since each piece of info (value) in the table has a label (key)
    #   so it should work just to zip them up
    info = pd.Series(dict(zip(keys, values)))
    info['registration'] = registration

    return info

def update_aircraft_info(conn, registration, ssl_cert_path):

    existing_aircraft = pd.read_sql("SELECT * FROM aircraft_info;", conn)
    aircraft_info = pd.Series()  # initialize in case the try block fails
    try:
        aircraft_info = get_aircraft_info(registration, ssl_cert_path)
    except Exception as e:
        warnings.warn('Could not retrieve aircraft info because %s' % e)
    if len(aircraft_info):
        aircraft_info = aircraft_info.loc[[c for c in existing_aircraft.columns if c in aircraft_info]]
        existing_info = existing_aircraft.loc[existing_aircraft.registration == registration]
        if len(existing_info):  # would be an empty df if the registration doesn't already exist
            # check if the record needs to be updated
            if not (existing_info == aircraft_info).squeeze().all():
                sql = "UPDATE aircraft_info SET {columns} WHERE registration = '{registration}';" \
                    .format(columns=', '.join(["%s = '%s'" % (k, v) for k, v in aircraft_info.iteritems()]),
                            registration=registration)
                conn.execute(sql)
        else:
            pd.DataFrame([aircraft_info]).to_sql('aircraft_info', conn, if_exists='append', index=False)


def main(connection_txt, ssl_cert_path):

    engine = db_utils.connect_db(connection_txt)

    with engine.connect() as conn, conn.begin():
        registration_numbers = pd.read_sql("SELECT DISTINCT registration FROM flights;", conn).squeeze()
        for registration in registration_numbers:
            try:
                update_aircraft_info(conn, registration, ssl_cert_path)
            except Exception as e:
                print('Could not update info for %s because %s' % (registration, e))
                continue
            print('Inserted/updated info for %s' % registration)


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
