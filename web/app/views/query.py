from app import app
from app.database import query_db
from app.utils import save_csv
from app.impala_queries import get_impala_query
from flask import flash, redirect, request, render_template, url_for, Response

@app.route('/')
def index():
    return render_template('results.html')

@app.route('/results')
def results():
    query = request.args.get('q')

    if query:
        query = query.strip()

    sql = get_impala_query(query)

    if not sql:
        flash('Please select a valid query', 'error')
        return redirect(url_for('index'))

    res = query_db(sql)
    csv = save_csv(res)

    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=%s.csv" % query}
    )
