import pymysql as mdb
import pyapi
import re

bit = pyapi.Pyapi('bandsintown')

con = mdb.connect('localhost', 'root', '', 'hearshowdb2') # host, user, pwd, db

def drop_table(table):
    with con:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(table))

def create_events_table():
    with con:
        cur = con.cursor()
        sql = ("CREATE TABLE IF NOT EXISTS Events "
               "(id INT PRIMARY KEY AUTO_INCREMENT," 
                          "artist_mbid TEXT,"
                          "artist_name TEXT,"
                          "artist_bit_url TEXT,"
                          "event_bit_id INT,"
                          "event_datetime DATETIME,"
                          "event_sale_datetime DATETIME,"
                          "tix_status TEXT,"
                          "tix_url TEXT,"
                          "event_url TEXT,"
                          "venue_bit_id INT,"
                          "venue_city TEXT,"
                          "venue_country TEXT,"
                          "venue_lat FLOAT,"
                          "venue_long FLOAT,"
                          "venue_name TEXT,"
                          "venue_region TEXT,"
                          "venue_url TEXT)"
                          )
        #print sql
        cur.execute(sql)

def scrub(s):
    return re.sub('"', '', s)

def check_entry(event_id):
    with con:
        cur = con.cursor()
        cur.execute("SELECT id FROM Events WHERE event_bit_id = {0}".format(event_id))
        rows = cur.fetchall()
        if len(rows) == 0:
            return False
        if len(rows) == 1:
            return True

def insert_row(rowdict):
    keys = []
    results = []
    for key in rowdict:
        if rowdict[key] == None:
            pass
        else:
            keys.append(key)
            results.append(rowdict[key])
    print keys
    print results
    try:
        sql = """INSERT INTO Events ({0}) VALUES (\"{1}\")""".format(', '.join(keys), '", "'.join(results))
        print
        print sql
    except UnicodeEncodeError:
        return
    #t1 = time.clock()
    #c = apsw_conn.cursor()
    #c.execute(sql)
    #t2 = time.clock()
    #print '\nTime to enter XY row: '+str(round(t2-t1, 6))+' seconds'
    #self.pkeyid = 1000
    #self.pkeyid = apsw_conn.last_insert_rowid()
    with con:
        cur = con.cursor()
        cur.execute(sql)
    con.commit()

def db_write_all_pages(location='San Francisco,CA', date='all'):
    """Writes all new events to the Events TABLE

    BandInTown API details:
    Example URL
    http://api.bandsintown.com/events/search?date=2015-09-25%2C2015-09-27&per_page=100&api_key=HearYourNextShow&location=Boston%2CMA&page=4

    pyapi call:
        foo.get('events/search', location='Boston,MA', per_page=100, date='2015-09-25,2015-09-27', page=4)

        API description:
            http://www.bandsintown.com/api/1.0/requests#events-search

        API call returns:
            list of dicts with 1 dict per event
                each dict has
                    'artists' -- list of artists with a dict per artist
                        dict keys:
                            'mbid' -- str MusicBrainz ID
                            'name' -- str artist name
                            'url' -- str BandsInTown artist url
                    'datetime' -- str of format '2015-09-25T12:00:00'
                    'id' -- int BandInTown event id
                    'on_sale_datetime' -- ??
                    'ticket_status': 'unavailable'
                    'ticket_url': 'http://www.bandsintown.com/event/9923689/buy_tickets?came_from=233'
                    'url': 'http://www.bandsintown.com/event/9923689'
                    'venue': 
                        dict keys:
                            'city': 'Boston',
                            'country': 'United States',
                            'id': 2806580, BandsInTown venue id
                            'latitude': 42.3583333,  map coords
                            'longitude': -71.0602778, map coords
                            'name': "Reader's Park",
                            'region': 'MA',
                            'url': 'http://www.bandsintown.com/venue/2806580'
    """
    page = 1
    while True:
        bit_req = bit.get('events/search', location=location, per_page=100, date=date, page=page)
        print page, len(bit_req)
        row_dict ={}
        for event in bit_req:
            if not check_entry(event['id']):
                row_dict['event_bit_id'] = str(event['id'])
                row_dict['event_datetime'] = event['datetime']
                row_dict['event_sale_datetime'] = event['on_sale_datetime']
                row_dict['tix_status'] = event['ticket_status']
                row_dict['tix_url'] = event['ticket_url']
                row_dict['event_url'] = event['url']
                row_dict['venue_bit_id'] = str(event['venue']['id'])
                row_dict['venue_city'] = event['venue']['city']
                row_dict['venue_country'] = event['venue']['country']
                row_dict['venue_lat'] = str(event['venue']['latitude'])
                row_dict['venue_long'] = str(event['venue']['longitude'])
                row_dict['venue_name'] = scrub(event['venue']['name'])
                row_dict['venue_region'] = event['venue']['region']
                row_dict['venue_url'] = event['venue']['url']
                for artist in event['artists']:
                    row_dict['artist_mbid'] = artist['mbid']
                    # for some reason, some of the artist names have a " in them!!
                    row_dict['artist_name'] = scrub(artist['name'])
                    row_dict['artist_bit_url'] = artist['url']
                    print row_dict
                    print
                    insert_row(row_dict)
        page += 1
        #if page > 1:
        if len(bit_req) < 100:
            break

def main():
    create_events_table()
    db_write_all_pages(location= 'Seattle,WA', date = 'upcoming')


if __name__ == '__main__':
    main()
