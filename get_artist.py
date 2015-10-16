import os
import pymysql as mdb
import sqlalchemy as sa
import pandas as pd
import cPickle
import json
import re
from sys import argv
from sklearn.cluster import KMeans
from pyechonest import artist as a
from pyechonest import util
from numpy import asarray
import pyapi
from pyapi import PyapiException

"""This script will pull the artists from the hearshowdb based on location and
event.  Then, using either MusicBrainz id (artist_mbid) or name, it will query
EchoNest to get a set of terms for the artists.  These terms will then be
stored as a sparse vector for each artist and the artists will be compared
against each other using cosine similarity
"""    

def scrub(s):
    return re.sub('"', '', s)

def get_all_terms(en):
    """Gets all of the terms used by Echonest to describe an artist
    Returns:
        all_terms: dict with k = str term name and v = consecutive integer
    """
    try:
        with open('all_terms.pik', 'r') as f:
            all_terms = cPickle.load(f)
    except IOError:
        terms = en.get('artist/list_terms')
        all_terms = {}
        for i, term in enumerate(terms['terms']):
            all_terms[term['name']] = i
        with open('all_terms.pik', 'w') as f:
            cPickle.dump(all_terms, f)
    return all_terms

def get_artist_info_pyapi(en, artist_mbid=None, artist_name=None):
    """Generates a single artist info dict from Echonest
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
            success = True
            return success, artist
        except PyapiException:
            pass
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

def get_kmeans():
    with open('kmeans_2015104.pik') as f:
        return cPickle.load(f)

def calc_cluster(nparray, model):
    cluster = model.predict(nparray)
    # set '0' cluster to '10' for plotting
    if cluster[0] == 0:
        return 10
    else:
        return cluster[0]


def get_all_artist_en_info(all_terms, engine, en, k_model, a_id):
    """
    """
    sql = ("SELECT id, artist_bit_name, artist_bit_mbid FROM Artist "
        "WHERE artist_en_id is NULL "
        "AND id > %s;")
        #2740, skip 5531,5532,5753, 7647, 7651 
        #7737, 9854, 10693, 12353 (genre), 6189 (% in artist name)
        #WHERE artist_en_id IS NULL")
    # pd sql query requires a sqlalchemy engine
    df_artists = pd.read_sql_query(sql, engine, params=[a_id])
    for index, row in df_artists.iterrows():
        #if index < 6:    
        result, artist_info =get_artist_info_pyapi(en,
                                     artist_mbid=row['artist_bit_mbid'],
                                     artist_name=row['artist_bit_name'])
        if result:    
            term_fq_vec = [0 for i in xrange(len(all_terms))]
            term_wt_vec = [0 for i in xrange(len(all_terms))]
            info = artist_info['artist']
            d = {}
            # some artist entries began with a \
            if info['name'][0] == '\\':
                d['artist_en_name'] = scrub(info['name'][1:])
            else:
                d['artist_en_name'] = scrub(info['name'])
            d['artist_en_id'] = info['id']
            d['en_familiarity'] = info['familiarity']
            d['en_hotttnesss'] = info['hotttnesss']
            #if len(info['genres']) != 0:
                #d['en_genre'] = info['genres'] #change at id 5531
            for term in info['terms']:
                # in case a term is not in the all_terms
                try:
                    idx = all_terms[term['name']]
                    term_fq_vec[idx] = term['frequency']
                    term_wt_vec[idx] = term['weight']
                except IndexError:
                    continue
            d['en_term_fq'] = json.dumps(term_fq_vec)
            d['en_term_wt'] = json.dumps(term_wt_vec)
            #d['en_songs'] = [scrub(song['title']) for song in info['songs']]
            d['k_ten'] = calc_cluster(asarray(term_wt_vec), k_model)
            print d['en_term_wt']
            try:
                col_val = ['{0} = "{1}"'.format(col, d[col]) for col in d]
            except UnicodeEncodeError:
                continue
            update_sql = (("UPDATE Artist SET %s WHERE id = \"%s\"") % (
                          ', '.join(col_val), row['id']))
            #print update_sql
            with engine.connect() as con:
                    con.execute(update_sql)


def main(a_id):
    db_connect_string = os.environ.get('DB_CREDENTIALS')
    engine = sa.create_engine(db_connect_string)
    en = pyapi.Pyapi('echonest')
    all_terms = get_all_terms(en)
    k_model = get_kmeans()
    get_all_artist_en_info(all_terms, engine, en, k_model, a_id)

if __name__ == '__main__':
    main(argv[1])
