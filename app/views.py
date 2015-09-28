from flask import render_template, request
import pymysql as mdb
from app import app
from a_Model import ModelIt

con = mdb.connect('localhost', 'root', '', 'hearshowdb2', charset='utf8') # host, user, pwd, db

@app.route('/')
@app.route('/index')
def index():
    return render_template("input.html")
    '''
    user = {'nickname': 'Miguel'} # fake user
    return render_template("index.html",
        title = 'Home',
        user = user)
    '''
'''
@app.route('/db')
def cities_page():
    with db:
        cur = db.cursor()
        cur.execute("SELECT Name FROM City LIMIT 15;")
        query_results = cur.fetchall()
    cities = ""
    for result in query_results:
        cities += result[0]
        cities += "<br>"
    return cities

@app.route('/db_fancy')
def cities_page_fancy():
    with db:
        cur = db.cursor()
        cur.execute("""SELECT Name, CountryCode,
            Population FROM City ORDER BY Population LIMIT 15;""")
        query_results = cur.fetchall()
    cities = [dict(name=result[0], country=result[1], population=result[2]) for \
              result in query_results]
    return render_template('cities.html', cities=cities)
'''

@app.route('/input')
def cities_input():
    return render_template("input.html")

@app.route('/viz')
def viz():
    event_graph = 'static/js/json/sf_sept15.json'
    return render_template("viz.html", jsonfile = event_graph)

@app.route('/output')
def cities_output():
    # pull 'ID' from input field and store it
    city = request.args.get('Venue_city')
    date = request.args.get('Dates')
    #print city
    artist_plot = "static/images/sf_20150925_20150927_2.png"
    artist = 'Arty'

    with con:
        cur = con.cursor()
        # just selct the city for the world_innodb that the use inputs
        cur.execute("SELECT event_url FROM Events WHERE event_datetime "
            "BETWEEN '2015-09-25 01:00:00' AND '2015-09-27 01:00:00' AND "
            "venue_city = '{0}' AND artist_name = '{1}';".format(city, artist))
        query_results = cur.fetchall()
    if len(query_results) != 0:
        event_url = str([result[0] for result in query_results][0])
    else:
        #cities = [dict(name='Not in db', country='NA', population='NA')]
        artist = ''
        artist_plot = ''
        event_url = 'Event not in database. Please try again.'

    return render_template("output.html",
                           artist_plot=artist_plot,
                           artist=artist,
                           event_url=event_url)
