from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from processes import search_results

app = Flask(__name__)
CORS(app)

@app.route('/search_site', methods=['POST','GET'])
def search_box():
    if request.method == 'POST':
        req = request.json['SearchQuery']
        query = req['Query']
        site_id = req['SiteId']
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