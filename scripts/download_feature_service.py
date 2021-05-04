"""
Download data from an AGOL feature service with an optional query to just return records after a given timestamp.

Usage:
    download_feature_service.py --help
    download_feature_service.py <out_dir> (--credentials_json=<str>) [--last_poll_time=<str>] [--ssl_cert=<str>] [--verbose]
    download_feature_service.py <out_dir> (--portal_url=<str> --service_url=<str> --client_id=<str> --client_secret=<str>) [--last_poll_time=<str>] [--ssl_cert=<str>] [--verbose]
    download_feature_service.py <out_dir> (--portal_url=<str> --service_url=<str> --username=<str>) [--last_poll_time=<str>] [--ssl_cert=<str>] [--verbose]

Required parameters:
    out_dir      Output directory to write downloaded files to

Options:
    -h, --help                      Show this screen.
    -j, --credentials_json=<str>    Path to a JSON file where each property is an AGOL login credential
    -p, --portal_url=<str>          URL for the user's AGOL portal [default: https://www.arcgis.com]
    -u, --service_url=<str>         AGOL URL for the feature service
    -i, --client_id=<str>           AGOL-issued client ID for this app to access user data (i.e., the feature service)
    -s, --client_secret=<str>       AGOL-issued client secret for this app to access user data
    -n, --username=<str>            AGOL username
    -t, --last_poll_time=<str>      Timestamp in the form YYYY-MM-DD HH:MM:SS to query records created after this time.
                                    If not specified, all records will be returned
    -c, --ssl_cert=<str>            Path to CA_BUNDLE (.crt or .pem file) to allow access through a firewall
                                    to make HTTP requests
    -v, --verbose                   Flag indicating whether to print progress
"""

import os, sys
import requests
import time
import json
import getpass
import re
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

import import_track


RESULT_TIMEOUT = 120 # number of seconds
CHECK_STATUS_INTERVAL = 2
APP_CREDENTIALS = ['service_url', 'portal_url', 'client_id', 'client_secret']
LOGIN_CREDENTIALS = ['service_url', 'portal_url', 'username', 'password']

def check_http_error(attempted_action, response):
    '''
    Validate an HTTP response from an ArcGIS REST API call
    :param attempted_action: string to indicating the attempted action that might have caused a failure
    :param response: the requests.Response object returned from requests.post() or requests.get()
    '''
    try:
        response.raise_for_status()
    except Exception as e:
        raise requests.HTTPError('failed to {action} because {error}'.format(action=attempted_action, error=e))

    # status codes are often valid even if there was an error, so check the response json
    try:
        # The result of a GET request for a data URL doesn't contain valid JSON
        response_json = response.json()
    except:
        return
    if 'error' in response_json:
        raise requests.HTTPError('failed to {action} because {error}'.format(action=attempted_action, error=response_json['error']['details']))


def read_credentials(credentials_json):
    '''
    Return a dictionary of credentials from a JSON file
    '''
    try:
        with open(credentials_json) as j:
            credentials = json.load(j)
    except Exception as e:
        raise IOError('Could not read credentials_json {0} because {1}'.format(credentials_json, e))

    return credentials


def get_token(credentials_json=None, credentials={}, portal_url=None, service_url=None, client_id=None, client_secret=None, username=None, ssl_cert=True):
    '''
    Return a token from the ArcGIS REST API:
        - https://developers.arcgis.com/rest/users-groups-and-items/generate-token.htm
        - https://developers.arcgis.com/documentation/core-concepts/security-and-authentication/accessing-arcgis-online-services/
    '''
    portal_url = credentials['portal_url'] if 'portal_url' in credentials else portal_url
    # Allow for retrieving a token either using login credentials or an authorized AGOL app
    if sorted(credentials.keys()) == sorted(LOGIN_CREDENTIALS) or all([portal_url, service_url, username]):
        username = credentials['username'] if credentials_json else username
        token_params = {'username': username,
                        'password': credentials['password'] if (credentials_json or credentials)
                                    else getpass.getpass("AGOL password for '%s':" % username),
                        'client': 'referer',
                        'referer': credentials['portal_url'] if credentials_json else portal_url,
                        'f': 'json'
                        }
        # generateToken only responds to POST request
        token_response = requests.post(portal_url + '/sharing/rest/generateToken', params=token_params, verify=ssl_cert)

    elif sorted(credentials.keys()) == sorted(APP_CREDENTIALS) or all([portal_url, service_url, client_id, client_secret]):
        token_params = {'client_id': credentials['client_id'] if credentials_json else client_id,
                        'client_secret': credentials['client_secret'] if credentials_json else client_secret,
                        'grant_type': 'client_credentials'
                        }
        # oauth2/token only responds to GET request
        token_response = requests.get(portal_url + '/sharing/rest/oauth2/token/', params=token_params, verify=ssl_cert)

    else:
        raise ValueError('invalid credentials in {0}. credentials_json must include all of either {1} or {2}'
                         .format(credentials_json, LOGIN_CREDENTIALS, APP_CREDENTIALS))

    check_http_error('get token', token_response)
    token_json = token_response.json()

    # Annoyingly, the generateToken and oauth2/token API calls return the token via a slightly different key
    return token_json['access_token'] if 'access_token' in token_json else token_json['token']


def get_service_info(service_url, token, ssl_cert=True):

    info_response = requests.get(service_url, params={'f': 'json', 'token': token}, verify=ssl_cert)
    check_http_error('get service info', info_response)

    return info_response.json()


def query_after_timestamp(service_url, token, layers, last_poll_time, ssl_cert=True, return_counts=False):
    '''
    Return an integer count of records matching the query in all layers/tables of the feature service
    '''

    # Check if there is any data to download
    query_params = {'f': 'json',
                    'token': token,
                    'returnCountOnly': return_counts
                    }
    if last_poll_time:
        query_params['layerDefs'] = json.dumps(
            {str(layer_id): "CreationDate > '%s'" % last_poll_time for layer_id in layers})
    else:
        # Return everything
        query_params['layerDefs'] = json.dumps(
            {str(layer_id): "CreationDate > TIMESTAMP '1970-1-1 00:00:00'" for layer_id in layers})

    query_response = requests.post('%s/query' % service_url, params=query_params, verify=ssl_cert)
    check_http_error('query number of records', query_response)
    query_json = query_response.json()

    if len(query_json) and 'layers' in query_json and return_counts:
        return sum([layer['count'] for layer in query_json['layers']])
    elif return_counts:
        return 0
    else:
        return query_json


def download_attachments(agol_ids, token, layer, layer_info, service_url, out_dir, ssl_cert=True, additional_info={}, overwrite=True):
    '''
    Download attachments from the specified layer of feature layer
    :param agol_ids: AGOL feature global IDs (not global IDs of attachments) to download attachments for
    :param token: REST API token string from getToken()
    :param layer: layer ID to look for attachments
    :param layer_info: pd.DataFrame of layer info from get_service_info()['layers']
    :param service_url: URL of feature service
    :param out_dir: path to directory to save attachments to
    :param ssl_cert: any valid value for requests.post() verify param (SSL .crt path or boolean)
    :param additional_info: dictionary of additional info to save in the metadata for each attachment
    :return: DataFrame of attachment info
    '''
    agol_id_str = ','.join(agol_ids).lower()
    query_params = {'f': 'json',
                    'token': token,
                    'globalIds': agol_id_str,
                    'returnUrl': True}
    query_response = requests.post('{service_url}/{layer}/queryAttachments'.format(service_url=service_url, layer=layer),
                                   params=query_params, verify=ssl_cert)
    check_http_error('downloading attachments', query_response)
    # Get attachment info as a dataframe. Note: g['attachmentInfos'][0] this assumes one attachment per feature
    df = pd.DataFrame({g['parentGlobalId']: {k.lower(): v for k, v in g['attachmentInfos'][0].items()} for g in  query_response.json()['attachmentGroups']})\
        .T\
        .reset_index()\
        .rename(columns={'index': 'parentglobalid'})

    attachments_dir = os.path.join(out_dir, 'attachments')
    # Create a separate dir for saving attachments (if it doesn't already exist).
    if len(df) and not os.path.isdir(attachments_dir):
        os.mkdir(attachments_dir)

    for i, row in df.iterrows():
        name, extension = os.path.splitext(row['name'])
        result_response = requests.get(row.url, params={'token': token}, verify=ssl_cert)
        check_http_error('download attachment %s' % row.parentglobalid, result_response)
        content = result_response.content
        attachment_path = os.path.join(attachments_dir, row['name'])

        if not overwrite:
            counter = 1
            while os.path.isfile(attachment_path):
                attachment_path = re.sub(r'-*\d*\{}$'.format(extension), '-{}{}'.format(counter, extension), attachment_path)
                counter += 1
            filename = os.path.basename(attachment_path)
            df.loc[i, 'name'] = filename
            row['name'] = filename

        with open(attachment_path, 'wb') as attachment_pointer:
            attachment_pointer.write(content)
        df.loc[i, 'content'] = content
        # write a JSON file with some metadata so the attachment can be related back to this DB file
        row_dict = row.to_dict()
        row_dict['parent_table_name'] = layer_info.loc[layer, 'name']
        row_dict['REL_GLOBALID'] = row.parentglobalid # This is what's called in the createReplica result so just make it consistent
        for k, v in additional_info.items():
            row_dict[k] = v
        json_path = attachment_path.rstrip(extension) + '.json'
        with open(json_path, 'w') as json_pointer:
            json.dump(row_dict, json_pointer, indent=4)

    return df


def download_from_replica(out_dir, token, layers, service_info, service_url, ssl_cert=True, last_poll_time=None, asynchronous=False):
    '''
    Download data from an AGOL feature service using createReplica. If last_poll_time is given, only return records
    created after this time
    :return: path to the SQLite DB of downloaded data
    '''

    # Submit POST request to get data with createReplica
    create_replica_params = {
        'f': 'json',
        'token': token,
        'layers': layers,
        'geometry': '-180,-90,180,90',
        'geometryType': 'esriGeometryEnvelope',
        'inSR': 4326,
        'dataFormat': 'sqlite',
        'returnAttachments': True,
        'returnAttachmentsDatabyURL': False,
        'async': asynchronous,
        'syncModel': 'none'
    }
    # Define a query if last poll time was given. Otherwise, all records will be returned

    if last_poll_time:
        layer_queries = {}
        for layer_id in layers:
            str_id = str(layer_id)
            layer_queries[str_id] = {
                'includeRelated': True,
                'queryOption': 'useFilter',
                'where': "CreationDate > '%s'" % last_poll_time
            }
        create_replica_params['layerQueries'] = json.dumps(layer_queries)
    replica_response = requests.post('%s/createReplica' % service_url, params=create_replica_params, verify=ssl_cert)
    check_http_error('create replica', replica_response)

    if asynchronous:
        status_url = replica_response.json()['statusUrl']

        # Query was set as asynchronous, so check the status at a set interval
        for i in range(0, RESULT_TIMEOUT, CHECK_STATUS_INTERVAL):
            status_response = requests.get(status_url, params={'f': 'json', 'token': token}, verify=ssl_cert)
            check_http_error('check status after %s iterations', status_response)
            status_json = status_response.json()

            # If the resultUrl isn't empty, that means the result is ready to download
            result_url = status_json['resultUrl']
            if result_url != '':
                break
            elif status_json['status'] in ('CompletedWithErrors', 'Failed'):
                raise requests.HTTPError('the query failed for an unspecified reason')
            else:
                if i + CHECK_STATUS_INTERVAL >= RESULT_TIMEOUT:  # timeout exceeded
                    raise requests.exceptions.ConnectTimeout(
                        'Asynchronous query exceeded RESULT_TIMEOUT of %.1f minutes' % (RESULT_TIMEOUT / 60.0)
                    )
                time.sleep(CHECK_STATUS_INTERVAL)

        # Write the result to disk
        result_response = requests.get(result_url, params={'f': 'json', 'token': token}, verify=ssl_cert)
        check_http_error('get createReplica result', result_response)
        content = result_response.content
    else:
        import pdb; pdb.set_trace()
        content = replica_response.content

    service_name = service_info['serviceDescription']
    sqlite_path = os.path.join(out_dir, '{0}_{1}.db'.format(service_name, datetime.now().strftime('%Y%m%d-%H%M%S')))
    with open(sqlite_path, 'wb') as f:
        f.write(content)

    # Get attachments (stored as bytes in Blob dtype column of the sqlite DB)
    engine = create_engine('sqlite:///' + sqlite_path)
    with engine.connect() as conn, conn.begin():
        attachment_tables = pd.read_sql("SELECT name FROM sqlite_master WHERE name LIKE('%__ATTACH');", conn)\
                                    .squeeze(axis=1)

        attachments_dir = os.path.join(out_dir, 'attachments')
        for table in attachment_tables:
            df = pd.read_sql_table(table, conn)
            # Create a separate dir for saving attachments (if it doesn't already exist). Check if it should be created
            #   here because the attachment table might exist but the table might be empty
            if len(df) and not os.path.isdir(attachments_dir):
                os.mkdir(attachments_dir)

            for _, row in df.iterrows():
                # Write the attachment with a name that is unique so if another submission comes in with an attachment
                #   that has the same name, the original doesn't get written over
                name, extension = os.path.splitext(row.ATT_NAME)
                attachment_id = row.GLOBALID.replace('{', '').replace('}', '') #for some annoying reason, ids have braces
                attachment_path = os.path.join(attachments_dir, '{name}{ext}'
                                               .format(name=name, ext=extension))
                with open(attachment_path, 'wb') as attachment_pointer:
                    attachment_pointer.write(row.DATA)

                # write a JSON file with some metadata so the attachment can be related back to this DB file
                row_dict = row.drop('DATA').to_dict() # drop 'DATA' field because it contains non-serializable bytes
                row_dict['sqlite_path'] = sqlite_path
                row_dict['parent_table_name'] = table.rstrip('__ATTACH')
                json_path = attachment_path.rstrip(extension) + '.json'
                with open(json_path, 'w') as json_pointer:
                    json.dump(row_dict, json_pointer, indent=4)

    return sqlite_path


def main(out_dir, ssl_cert=True, credentials_json=None, portal_url=None, service_url=None, client_id=None, client_secret=None, username=None, last_poll_time=None, verbose=False):

    # Get token for REST API
    # If credentials_json given, read the file
    credentials = {}
    if credentials_json:
        credentials_json = os.path.abspath(credentials_json)
        if verbose:
            sys.stdout.write('Reading credential info from %s...\n' % credentials_json)
            sys.stdout.flush()
        credentials = read_credentials(credentials_json)
        if 'service_url' in credentials:
            service_url = credentials['service_url']
        else:
            raise ValueError('service_url not specified in credentials_json %s' % credentials_json)
    if verbose:
        sys.stdout.write('Retrieving token...\n')
        sys.stdout.flush()
    token = get_token(credentials_json, credentials, portal_url, service_url,
                      client_id, client_secret, username, ssl_cert)

    # Get feature service info
    if verbose:
        sys.stdout.write('Retrieving feature service info...\n')
    service_info = get_service_info(service_url, token, ssl_cert)
    layers = [layer_info['id'] for layer_info in service_info['layers'] + service_info['tables']]

    # If there are any records that match the query, download them
    matching_records = query_after_timestamp(service_url, token, layers, last_poll_time, ssl_cert=ssl_cert, return_counts=True)
    if matching_records:
        data_path = download_from_replica(out_dir, token, layers, service_info, service_url,
                                  ssl_cert=ssl_cert, last_poll_time=last_poll_time)
        if verbose:
            sys.stdout.write('Downloaded {0} records from {1} layers/tables to {2}'
                             .format(matching_records, len(layers), data_path))
    else:
        if verbose:
            sys.stdout.write('No data to download at this time')
    sys.stdout.flush()


if __name__ == '__main__':
    args = import_track.get_cl_args(__doc__)
    sys.exit(main(**args))