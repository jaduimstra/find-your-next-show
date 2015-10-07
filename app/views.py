import pymysql as mdb
import datetime
from flask import render_template, request

from app import app
import graph_analytics as ga

@app.route('/')
@app.route('/index')
def index():
    return render_template("base.html")

@app.route('/output')
def output():
    cities = {"San Francisco":"CA",
              "New York":"NY",
              "Seattle":"WA"}
    city = request.args.get('Venue_city')
    date = request.args.get('Date')
    try:
        d = datetime.datetime.strptime(date, '%Y-%m-%d')
        if d > datetime.datetime(2015, 9, 30):
            out_date = d.strftime('%Y-%m-%d')
        else:
            return render_template("date_error.html")
    except ValueError:
        return render_template("date_error.html")
    j_graph = ga.generate_json_graph(out_date, cities[city])
    return render_template("output.html", jsonfile = j_graph, 
                           in_date=out_date, city=city)

