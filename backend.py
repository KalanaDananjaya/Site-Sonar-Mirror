from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from processes import search_results
from expression_evaluator import parse_boolean_expression

app = Flask(__name__)
CORS(app)

@app.route('/search_site', methods=['POST','GET'])
def search_box():
    if request.method == 'POST':
        req = request.json['SearchFormInput']
        queries = req['SearchFields']
        equation = req['Equation']
        site_id = req['SiteId']
        parse_boolean_expression(equation,len(queries))
        total_nodes,coverage,supported = search_results(query,site_id)
        result = {
            'total_nodes':total_nodes,
            'supported': supported,
            'coverage':coverage
        }
        return jsonify (result)
    if request.method == 'GET':
        return render_template('index.html',init='True')

if __name__ == "__main__":
    app.run(debug=True)