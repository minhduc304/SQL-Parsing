from concurrent.futures import process
from distutils.log import debug
from unittest import skip
from flask import Flask, jsonify, request, render_template
from scr.readobj import ParsingSQLOwnersAndObjects
gg_auth_code = '115302095083483541585'


app = Flask(__name__)


@app.route('/sql_flow')
def request_():
    # a = ParsingSQLOwnersAndObjects()
    # sql_string = request.args.get('sql-string')
    # return jsonify(a.single_sql(sql_string))
    
    gg_auth_code = request.args.get('auth-code')
    sql_string = request.args.get('sql-string')
    return jsonify(sqlflow_request(gg_auth_code, sql_string))

    #return '''<h1>{}</h1>'''.format(sqlflow_request(gg_auth_code, sql_string))





if __name__ == "__main__":
    app.run(debug=True)


