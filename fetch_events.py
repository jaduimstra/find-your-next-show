import pickle
import pymysql as mdb
import pyapi
import re

bit = pyapi.Pyapi('bandsintown')

with open('con_cred.pik', 'r') as f:
    cred = pickle.load(f)
con = mdb.connect(cred[0], cred[1], cred[2], cred[3])

def scrub(s):
    return re.sub('"', '', s)

def check_entry(sql):
    with con:
        cur = con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) == 0:
            return 0
        else:
            for row in rows:
                return row[0]

def gen_key_value(rowdict):
    # Generate column and values strings for SQL inserts
    # from a dict of k=column, v=value
    keys = []
    results = []
    for key in rowdict:
        if rowdict[key] == None:
            pass
        else:
            keys.append(key)
            results.append(rowdict[key])
    return ', '.join(keys), '", "'.join(results)

def insert_event(a_dict, e_dict, v_dict):
    # Insert the artist and venue info and 
    fk_id = {}
    for d in [(a_dict, 'Artist', 'artist_bit_name'),
              (v_dict, 'Venue', 'venue_bit_id')]:
        try:
            row_id = check_entry("SELECT id FROM {0} WHERE {1} = \"{2}\""
                    .format(d[1], d[2], d[0][d[2]]))
        except UnicodeEncodeError:
            break
        if row_id == 0:
            columns, values = gen_key_value(d[0])
            try:
                sql = ("""INSERT INTO {0} ({1}) VALUES (\"{2}\")"""
                       .format(d[1], columns, values))
                print
                print sql
            except UnicodeEncodeError:
                break
            with con:
                cur = con.cursor()
                cur.execute(sql)
                cur.execute('SELECT LAST_INSERT_ID()')
                rows = cur.fetchall()
                for row in rows:
                    fk_id[d[1]] = row[0]
        else:
            fk_id[d[1]] = row_id
    #Now insert the Event using the fk_ids from the Artist and Venue tables
    e_col, e_val = gen_key_value(e_dict)
    for fk in fk_id:
        e_col = e_col + ', {0}_fk'.format(fk.lower())
        e_val = e_val + '", \"{0}'.format(fk_id[fk])
    try:
        sql = ("""INSERT INTO Event ({0}) VALUES (\"{1}\")"""
               .format(e_col, e_val))
        print
        print sql
    except UnicodeEncodeError:
        return
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
        a_dict ={}
        e_dict ={}
        v_dict ={}
        for event in bit_req:
            #if not check_entry(event['id']):
                e_dict['event_bit_id'] = str(event['id'])
                e_dict['event_datetime'] = event['datetime']
                e_dict['event_sale_datetime'] = event['on_sale_datetime']
                e_dict['tix_status'] = event['ticket_status']
                e_dict['tix_url'] = event['ticket_url']
                e_dict['event_url'] = event['url']
                v_dict['venue_bit_id'] = str(event['venue']['id'])
                v_dict['venue_city'] = event['venue']['city']
                v_dict['venue_country'] = event['venue']['country']
                v_dict['venue_lat'] = str(event['venue']['latitude'])
                v_dict['venue_long'] = str(event['venue']['longitude'])
                v_dict['venue_name'] = scrub(event['venue']['name'])
                v_dict['venue_region'] = event['venue']['region']
                v_dict['venue_url'] = event['venue']['url']
                for artist in event['artists']:
                    a_dict['artist_bit_mbid'] = artist['mbid']
                    # for some reason, some of the artist names have a " in them!!
                    a_dict['artist_bit_name'] = scrub(artist['name'])
                    a_dict['artist_bit_url'] = artist['url']
                    print a_dict
                    print
                    insert_event(a_dict, e_dict, v_dict)
        page += 1
        #if page > 1:
        if len(bit_req) < 100:
            break

def main():
    #create_events_table()
    db_write_all_pages(location= 'San Francisco,CA', date = 'upcoming')


if __name__ == '__main__':
    main()
