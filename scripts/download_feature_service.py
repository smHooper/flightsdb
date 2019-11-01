import os, sys
import requests
import time
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


RESULT_TIMEOUT = 900 # seconds in 15 minutes
CHECK_STATUS_INTERVAL = 5


def check_http_error(attempted_action, response):
    try:
        response.raise_for_status()
    except Exception as e:
        raise requests.HTTPError('failed to {action} because {error}'.format(action=attempted_action, error=e))

    # status codes are often valid even if there was an error, so check the response json
    response_json = response.json()
    if 'error' in response_json:
        raise requests.HTTPError('failed to {action} because {error}'.format(action=attempted_action, error=response_json['error']['details']))


def download_data(portal_url, service_url, client_id, client_secret, ssl_cert, out_dir, last_poll_time=None):

    # Get token for REST API
    token_params = {'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'client_credentials'}
    token_response = requests.get(portal_url + '/sharing/rest/oauth2/token/', params=token_params, verify=ssl_cert)
    check_http_error('get token', token_response)
    token = token_response.json()['access_token']

    # Get feature service info
    info_response = requests.get(service_url, params={'f': 'json', 'token': token}, verify=ssl_cert)
    check_http_error('get service info', info_response)
    service_info = info_response.json()

    # Submit POST request to get data
    # Make sure that the layerQueries parameter is given with 'includeRelated' = true. Otherwise, related records will
    #   not be included
    layers = [layer_info['id'] for layer_info in service_info['layers']]
    layer_queries = {}
    for layer_id in layers:
        str_id = str(layer_id)
        layer_queries[str_id] = {
            'includeRelated': True,
            'queryOption': 'none'
        }
        if last_poll_time:
            layer_queries[str_id]['queryOption'] = 'useFilter'
            layer_queries[str_id]['where'] = "CreationDate > TIMESTAMP '%s'" % last_poll_time

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
        'async': True,
        'syncModel': 'none'#,
        #'layerQueries': json.dumps(layer_queries)
    }
    replica_response = requests.post('%s/createReplica' % service_url, params=create_replica_params, verify=ssl_cert)
    check_http_error('create replica', replica_response)
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
            if i + CHECK_STATUS_INTERVAL >= RESULT_TIMEOUT: # timeout exceeded
                raise requests.exceptions.ConnectTimeout(
                    'Asynchronous query exceeded RESULT_TIMEOUT of %.1f minutes' % (RESULT_TIMEOUT/60.0)
                )
            time.sleep(CHECK_STATUS_INTERVAL)

    # Write the result to disk
    result_response = requests.get(result_url, params={'f': 'json', 'token': token}, verify=ssl_cert)
    check_http_error('get createReplica result', result_response)
    service_name = service_info['serviceDescription']
    sqlite_path = os.path.join(out_dir, '{0}_{1}.db'.format(service_name, datetime.now().strftime('%Y%m%d-%H%M%S')))
    with open(sqlite_path, 'wb') as f:
        f.write(result_response.content)

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
                attachment_path = os.path.join(attachments_dir, '{name}_{id}{ext}'
                                               .format(name=name, id=attachment_id, ext=extension))
                with open(attachment_path, 'wb') as attachment_pointer:
                    attachment_pointer.write(row.DATA)

                # write a JSON file with some metadata so the attachment can be related back to this DB file
                row_dict = row.drop('DATA').to_dict() # drop 'DATA' field because it contains non-serializable bytes
                row_dict['sqlite_path'] = sqlite_path
                json_path = attachment_path.rstrip(extension) + '.json'
                with open(json_path, 'w') as json_pointer:
                    json.dump(row_dict, json_pointer, indent=4)


if __name__ == '__main__':
    sys.exit(download_data(*sys.argv[1:]))