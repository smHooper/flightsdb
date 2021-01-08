"""
Update/append information from the FAA registry website to the aircraft_info table of the overflights DB. 

Usage:
    get_missing_aircraft_info.py <connection_txt> <url> [--ssl_cert_path=<str>] [--update] [--silent]

Examples:

Required parameters:
    connection_txt      Path of a text file containing information to connect to the DB. Each line
                        in the text file must be in the form 'variable_name; variable_value.'
                        Required variables: username, password, ip_address, port, db_name.
    url                 Earliest date of interest to return data from

Options:
    -h, --help                  Show this screen.
    -s, --ssl_cert_path=<str>   Path to an SSL .crt file for submitting an HTTPS request
    -u, --update                Option to specify that existing aircraft info should be updated. If the 
                                aircraft's information isn't already in the DB, it will be added regardless
    -s, --silent                If specified, only errors and warnings will be sent to stdout
"""

import os
import sys
import re
import bs4
import time
import docopt
import requests
import warnings
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

import db_utils
import utils



def get_html_with_selenium(url, registration):

    options = webdriver.ChromeOptions()
    options.add_experimental_option('useAutomationExtension', False) #throws an error without this
    options.add_argument('headless') #keep browser hidden
    #options.add_argument('log-level=1') #suppress all message except fatal ones
    #options.add_experimental_option('excludeSwitches', ['enable-logging']) # turn off all logging
    try:
        driver = webdriver.Chrome(options=options, desired_capabilities=options.to_capabilities())
    except Exception as e:
        raise RuntimeError('Could not retrieve aircraft info because selenium is not properly configured: %s' % e)

    # Go to page
    driver.get(url)

    # This will throw an error if there's no element with the id 'input-error'. This would only be 
    #   the case if FAA reconfigured their website
    try:
        registration_input = driver.find_element_by_css_selector('#input-error')
    except Exception as e:
        raise RuntimeError(f'Invalid URL: {url}. Error: {e}')
    
    # Enter registration and visit result page
    registration_input.send_keys(registration)
    registration_input.send_keys(Keys.RETURN)

    html = driver.page_source
    driver.close()

    return html


def get_aircraft_info(registration, url, ssl_cert=False):
    """
    Scrape the FAA registry for info about an aircraft from its registration (N-) number

    :param registration: N-number of the aircraft
    :param url: FAA website URL
    :param ssl_cert: path to an SSL .crt or .pem file
    :return: pd.Series of aircraft info (index is the property name)
    """

    # If ssl_cert is given, try to get the html text with just a request
    if ssl_cert:
        response = requests.get(url.format(registration), verify=ssl_cert)
        response.raise_for_status() # exception should be caught when calling this function
        html = response.text
    else:
        html = get_html_with_selenium(url, registration)
    
    soup = bs4.BeautifulSoup(html, features='html5lib')

    # Find table cells with data. Cells are in the format 
    #   <td data-label="Engine Type">Turbo-shaft</td>
    get_tds = lambda tag: tag.name == 'td' and tag['data-label'] if tag.has_attr('data-label') else False
    tds = soup.find_all(get_tds)

    # remove special characters from what will be field names and strip trailing spaces from values
    info = pd.Series({
        re.sub('[^\w0-9a-zA-Z]+', '_', tag['data-label'].strip().lower()): 
        tag.getText().strip() 
        for tag in tds
    })

    info['registration'] = registration

    return info


def update_aircraft_info(conn, registration, url, ssl_cert_path=None, update=False):

    existing_aircraft = pd.read_sql("SELECT * FROM aircraft_info;", conn)
    aircraft_info = pd.Series(dtype=object)  # initialize in case the try block fails
    try:
        aircraft_info = get_aircraft_info(registration, url, ssl_cert_path)
    except Exception as e:
        warnings.warn(f'Could not retrieve aircraft info for {registration} because {e}')
        return False
    
    if len(aircraft_info):
        aircraft_info = aircraft_info.loc[[c for c in existing_aircraft.columns if c in aircraft_info]]
        existing_info = existing_aircraft.loc[existing_aircraft.registration == registration]
        if len(existing_info):  # would be an empty df if the registration doesn't already exist

            # updatet the record only if need be and the user specified to do updates
            if (existing_info != aircraft_info).squeeze().any() and update:
                sql = '''UPDATE aircraft_info SET {columns} WHERE registration = '{registration}';''' \
                    .format(columns=', '.join(["%s = '%s'" % (k, v) for k, v in aircraft_info.iteritems()]),
                            registration=registration)
                conn.execute(sql)
            else:
                return False
        else:
            pd.DataFrame([aircraft_info]).to_sql('aircraft_info', conn, if_exists='append', index=False)

        return True


def main(connection_txt, url, ssl_cert_path=None, update=False, silent=False):  

    engine = db_utils.connect_db(connection_txt)

    where_clause = '' if update else ' WHERE registration NOT IN (SELECT registration FROM aircraft_info)'

    with engine.connect() as conn:
        registration_numbers = pd.read_sql(f'''SELECT DISTINCT registration FROM flights{where_clause};''', conn).squeeze()
        for registration in registration_numbers:
            was_updated = False
            try:
                was_updated = update_aircraft_info(conn, registration, url, ssl_cert_path, update)
            except Exception as e:
                print('Could not update info for %s because %s' % (registration, e))
                continue
            
            if was_updated and not silent:
                print('Inserted/updated info for %s' % registration)


if __name__ == '__main__':
    args = utils.get_cl_args(__doc__)
    sys.exit(main(**args))
