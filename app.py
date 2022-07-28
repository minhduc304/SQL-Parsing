from concurrent.futures import process
from distutils.log import debug
from flask import Flask, jsonify, request, render_template
from scr.readobj import ParsingSQLOwnersAndObjects


app = Flask(__name__)

# @app.route("/")
# def my_form():
#     return render_template('upload.html')


@app.route('/upload/', methods=['GET','POST'])
def my_form_post():
   
    a = ParsingSQLOwnersAndObjects()
    #args = request.args
    text = request.json.get('sql_string')
    #a.find_objects_in_sql_stmt()
    return jsonify(a.single_sql(text))
    #return processed_text




    

if __name__ == "__main__":
    app.run(debug=True)

