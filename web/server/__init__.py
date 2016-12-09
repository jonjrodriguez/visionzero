from flask import Flask, jsonify, redirect, render_template, request, url_for, Response

app = Flask(__name__)
app.config.from_object('config')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/download/<query>')
def download(query):
    sql = get_impala_query(query)

    if not sql:
        return jsonify(error=404, text=str("Not Found")), 404

    res = query_db(sql)
    csv = save_csv(res)

    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=%s.csv" % query}
    )

@app.route('/api/<query>')
def api(query):
    realtime = request.args.get('realtime', False)
    sql = get_impala_query(query, realtime)

    if not sql:
        return jsonify(error=404, text=str("Not Found")), 404

    res = query_db(sql)

    return jsonify(query=sql, results=res)

@app.errorhandler(404)
def page_not_found(e):
    return render_template('index.html')

from server.database import query_db
from server.utils import save_csv
from server.impala_queries import get_impala_query
