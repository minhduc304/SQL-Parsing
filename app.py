from concurrent.futures import process
from distutils.command.config import config
from distutils.log import debug
import re
import string
from unittest import skip
from flask import Flask, jsonify, request, render_template
from scr.readobj import ParsingSQLOwnersAndObjects
from scr.sql_request import *

app = Flask(__name__)


@app.route('/apiv1', methods=['POST'])
def single_string():
    a = ParsingSQLOwnersAndObjects()
    sql_string = request.args.get('sql-string')
    return jsonify(a.single_sql(sql_string))

@app.route('/apiv2', methods=['POST'])
def sqlflow_():
    req_json = request.json
    sql_string = req_json['sql-string']
    string_input = sqlflow_request(sql_string)
    schema_parsing = req_json['schema-parsing']
    return jsonify(extracl_response_sqlflow(string_input, schema_parsing))
    

if __name__ == "__main__":
    app.run(debug=True)


