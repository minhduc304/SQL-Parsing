import http.client
import mimetypes
from codecs import encode
import json


def sqlflow_request(sql_stmt, gg_auth_code='115302095083483541585'):
    conn = http.client.HTTPSConnection("api.gudusoft.com")
    dataList = []
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=userId;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("google-oauth2|{}".format(gg_auth_code)))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=sqltext;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode(sql_stmt))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=dbvendor;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("dbvoracle"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=showRelationType;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("fdd"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=dataflowOfAggregateFunction;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("direct"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=ignoreRecordSet;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("false"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=ignoreFunction;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("true"))
    dataList.append(encode('--'+boundary+'--'))
    dataList.append(encode(''))
    body = b'\r\n'.join(dataList)
    payload = body
    headers = {
    'Authorization': 'Token eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJndWR1c29mdCIsImV4cCI6MTkzNjg4NjQwMCwiaWF0IjoxNjIxNTI2NDAwfQ.1HBR-8Gv5xlpsI-uLY3p90cpFs9h01yBXPQ2L30MkUY',
    'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
    }
    conn.request("POST", "/gspLive_backend/sqlflow/generation/sqlflow/graph", payload, headers)
    res = conn.getresponse()
    data = res.read()

    return data.decode("utf-8")
   
def extracl_response_sqlflow(string_data_input, schema_parsing):
    objects_found = []
    dict_res = json.loads(string_data_input)
    
    for x in dict_res['data']['sqlflow']['dbobjs']:
        li_cols = None
        if 'schema' not in x.keys():
            if 'columns' in x.keys():
                li_cols = [y['name'] for y in x['columns']]
            objects_found.append([x['type'], schema_parsing, x['name'], li_cols, 'Found'])
        else:
            if 'columns' in x.keys():
                li_cols = [y['name'] for y in x['columns']]
            objects_found.append([x['type'], x['schema'], x['name'], li_cols, 'Checked'])
    
    return objects_found

