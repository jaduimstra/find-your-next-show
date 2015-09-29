import os
import pymysql as mdb
import sqlalchemy as sa
import pandas as pd
import pickle
from pyechonest import artist as a
from pyechonest import util
from numpy import zeros
import pyapi
from pyapi import PyapiException

"""This script will pull the artists from the hearshowdb based on location and
event.  Then, using either MusicBrainz id (artist_mbid) or name, it will query
EchoNest to get a set of terms for the artists.  These terms will then be
stored as a sparse vector for each artist and the artists will be compared
against each other using cosine similarity
"""

db_connect_string = os.environ.get('DB_CREDENTIALS')

engine = sa.create_engine(db_connect_string)
#artist_info = {}

en = pyapi.Pyapi('echonest')

#def artists_df_lookup(date_start, date_stop, venue_region='CA'):
def artists_df_lookup(date_start, date_stop, venue_city='San Francisco'):
    """
    Args
        date_start: str of format YYYY-MM-DD HH:MM:SS
        date_end: str of format YYYY-MM-DD HH:MM:SS
        venue_region: str, currently either 'CA', 'WA' or 'NY'
        venue_city: str, 'San Francisco'
    """
    sql = ("SELECT artist_name, artist_mbid, event_datetime FROM Events "
        "WHERE event_datetime BETWEEN '{0}' AND '{1}' "
        "AND venue_city = '{2}' "
        #"ORDER BY event_datetime DESC, artist_mbid ASC;"
        .format(date_start, date_stop, venue_city))

    print sql
    # pd sql query requires a sqlalchemy engine
    return pd.read_sql_query(sql, engine)

def get_artist_info_pyechonest(df_artist):
    """Takes an Artist pyechonest object
    Arg: df_artist, a pandas dataframe

    Note: this function has a tendency to exceed the API limit
    """
    i = 0
    j = 0
    k = 0
    for index, row in df_artist.iterrows():
        artist = None
        try:
            artist = a.Artist('musicbrainz:artist:{0}'
                              .format(row['artist_mbid']))
            i += 1
            print '\n', 'mb =', i, '\n'
        except util.EchoNestAPIError:
            try:
                artist = a.Artist(row['artist_name'])
                j += 1
                print '\n', 'name =', j, '\n'
            except util.EchoNestAPIError:
                k += 1
                print '\n', 'unknown =', k, '\n'
        if artist:
            artist_info[artist.name] = {'familiarity': artist.familiarity,
                                        'hotttnesss': artist.hotttnesss,
                                        'terms': artist.terms}
            print 'familiarity: ', artist.familiarity 
    with open('artist_info.pik', 'w') as f:
        pickle.dump(artist_info, f)
    return artist_info

def get_all_terms():
    """Gets all of the terms used by Echonest to describe an artist
    Returns:
        all_terms: dict with k = str term name and v = consecutive integer
    """
    terms = en.get('artist/list_terms')
    all_terms = {}
    for i, term in enumerate(terms['terms']):
        all_terms[term['name']] = i
    return all_terms

def get_artist_info_pyapi(artist_mbid=None, artist_name=None):
    """Generates a single artist info dict
    Args:
        artist_mbid, str: MusicBrainz artist id
        artist_name, str: artist name
    Returns:
        success: bool, True if artist lookup was successful,
                False if artist lookup unsuccessful
        artist: dict of json artist info from Echonest
                has format:

    """
    i = False
    j = False
    k = False
    artist = None
    success = False
    bucket_list = ['terms',
                   'genre',
                   'familiarity',
                   'hotttnesss',
                   'songs']
    if artist_mbid != None:
        # use a try block here in case the mbid is not accessible
        # through Echonest
        try:
            artist = en.get('artist/profile', id='musicbrainz:artist:{0}'
                              .format(artist_mbid), bucket=bucket_list)
            i = True
            #print '\n', 'mb =', i, '\n'
        except PyapiException:
            pass
    else:
        # lookup artist on Echonest using the artist's name
        try:
            artist = en.get('artist/profile', name=artist_name,
                            bucket=bucket_list)
            j = True
            #print '\n', 'name =', j, '\n'
        except PyapiException:
            k = True
            #print '\n', 'unknown =', k, '\n'
    if artist:
        print artist
        #artist_info['name']
        success = True
        return success, artist
    else:
        return success, artist

def generate_all_artist_info(df_artists, all_terms):
    """generates and pickles a dict of all artists in 'df_artist' whose info
    could be found on Echonest. Formats in the following manner:


    """
    all_artist_info = {}
    for index, row in df_artists.iterrows():
        #if index < 5:
        result, artist_info =get_artist_info_pyapi(artist_mbid=row['artist_mbid'],
                                         artist_name=row['artist_name'])
        if result:    
            term_wt_vec = zeros(len(all_terms))
            term_fq_vec = zeros(len(all_terms))
            d = {}
            info = artist_info['artist']
            d['en_name'] = info['name']
            d['en_id'] = info['id']
            d['mbid'] = row['artist_mbid']
            d['familiarity'] = info['familiarity']
            d['hotttnesss'] = info['hotttnesss']
            d['genre'] = info['genres']
            for term in info['terms']:
                vec_idx = all_terms[term['name']]
                term_fq_vec[vec_idx] = term['frequency']
                term_wt_vec[vec_idx] = term['weight']
            d['term_fq'] = term_fq_vec
            d['term_wt'] = term_wt_vec
            d['songs'] = [song['title'] for song in info['songs']]
            all_artist_info[row['artist_name']] = d
    with open('all_artist_info2.pik', 'w') as f:
        pickle.dump(all_artist_info, f)
    return all_artist_info

def main():
    df_artists = artists_df_lookup('2015-09-25 01:00:00', '2015-09-27 01:00:00')
    all_terms = get_all_terms()
    all_info = generate_all_artist_info(df_artists, all_terms)
    return all_info, all_terms
    #artist_info = get_artist_info_pyapi(df_artist)

if __name__ == '__main__':
    all_info, all_terms = main()
