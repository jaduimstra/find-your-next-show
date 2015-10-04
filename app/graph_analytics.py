# Here's the SQL query for this run:
# 
# SELECT artist_name, artist_mbid, event_datetime FROM Events WHERE event_datetime BETWEEN '2015-09-25 01:00:00' AND '2015-09-27 01:00:00' AND venue_city = 'San Francisco'
import pickle
import json
import pandas as pd
import numpy as np
import networkx as nx
from networkx.readwrite import json_graph
import os
import sqlalchemy as sa

db_connect_string = os.environ.get('DB_CREDENTIALS')
engine = sa.create_engine(db_connect_string)

with open('all_terms.pik', 'r') as f:
    # BE CAREFUL WITH the pickle file, it's consecutive except for
    # skipping 127 since that entry appeared twice in the
    # EchoNest API
    all_terms = pickle.load(f)
term_key = sorted([(all_terms[term], term) for term in all_terms])

def ordered_terms(w, term_key=term_key):
    term_name = []
    term_value = []
    for idx, elem in enumerate(w):
        if elem > 0:
            # hack to get around the fact that the data were encoded without
            # a 127 index in `all_terms`
            if idx < 127:
                term_name.append(term_key[idx][1])
            else:
                term_name.append(term_key[idx-1][1])
            term_value.append(elem)
    t = sorted(zip(term_value, term_name), reverse=True)
    ordered_names = [item[1] for item in t]
    ordered_values = [item[0] for item in t]
    return [ordered_names, ordered_values]

def db_lookup(event_date='2015-10-06', venue_region='CA'):
    sql2 = ("SELECT artist_en_name, en_term_wt FROM Artist "
          "WHERE id < 500 AND artist_en_name IS NOT NULL")

    sql = ("SELECT a.`artist_en_name`, a.`en_familiarity`, a.`en_hotttnesss`, "
        "a.`en_term_fq`, a.`en_term_wt`, a.`en_songs`, a.`en_genre`, "
        "a.`itunes_id`, a.`itunes_art_url`, a.`itunes_prev_url`, a.`itunes_prim_genre`, "
        "Venue.`venue_city`, Venue.`venue_name`, Event.`event_datetime`, "
        "Event.`event_bit_id`, Event.`event_url` "
        "FROM `Event` "
        "INNER JOIN `Artist` AS a ON Event.`artist_fk` = a.`id` "
        "INNER JOIN `Venue` ON Event.`venue_fk` = Venue.`id` "
        "WHERE a.artist_en_name IS NOT NULL "
        "AND Event.`event_datetime` LIKE %s " #need to add extra%
        "AND Venue.`venue_region` = %s;")
    # need to add extra% to escape the % wildcard used to select all times for a
    # desired date for the datetime query
    df = pd.read_sql_query(sql, engine, params=(event_date+'%%', venue_region),
                           index_col = 'artist_en_name')
    df.en_term_wt = df.en_term_wt.map(lambda w: np.asarray(json.loads(w)))
    df2 = df[df.en_term_wt.map(lambda w: sum(w)) != 0]
    # Create column of normalized term weight vectors
    df2.loc[:,'norm_wt'] = df2.en_term_wt.map(lambda w: w/np.sqrt(np.dot(w, w)))
    df2.loc[:,'term_names'] = df2.en_term_wt.map(lambda w: ordered_terms(w)[0])
    df2.loc[:,'term_values'] = df2.en_term_wt.map(lambda w: ordered_terms(w)[1])
    return df2

def graph_from_df(df, min_wt=0.3):
    """
    Create graph from weights in df
    see: http://stackoverflow.com/questions/21207872/construct-networkx-graph-from-pandas-dataframe
    Args:
        df: pandas DataFrame, each row represents an artist.  Contains the
            weight vectors for each Artist in a np array in column 'norm_wt'
        min_wt: float, the mininum value for and edge to be kept
    Returns
        g: networkx graph with nodes label with the Artist name and edges
            defined using the min_wt argument
    
    How it works:
    Generate similarity matrix
    We get the projection of each vector on all the others by doing
    df(transpose) multiplied by df to get a square matrix with dimension 
    equal to the number of artists contained in the df
    """
    norm_matrix = np.vstack(df['norm_wt'].values)
    wt_matrix = np.dot(norm_matrix, np.transpose(norm_matrix))
    wt_df = pd.DataFrame(data=wt_matrix, columns=df.index, index=df.index)
    # get rid of edges less than 0.3 and self edges and fill those with zeros
    g = nx.from_numpy_matrix(wt_df[(wt_df > min_wt) & (wt_df < .9999)]
                             .fillna(0).values)
    g = nx.relabel_nodes(g, dict(enumerate(wt_df.index)))
    degree = g.degree()
    to_keep = [n for n in degree if degree[n] > 0]
    to_delete = list(set(degree)-set(to_keep))
    df = df.drop(to_delete)
    g_sub = g.subgraph(to_keep)
    return g_sub, df

def graph_add_attributes(g, df):
    nx.set_node_attributes(g, 'label', {n:n for n in g.nodes()})
    nx.set_node_attributes(g, 'label', {n:n for n in g.nodes()})
    attr = [('familiarity', 'en_familiarity'),
            ('hotttnesss', 'en_hotttnesss'),
            ('art_url','itunes_art_url'),
            ('prim_genre', 'itunes_prim_genre'),
            ('prev_url', 'itunes_prev_url'),
            ('city', 'venue_city'),
            ('venue_name', 'venue_name'),
            #('event_datetime', 'event_datetime'),
            ('event_url', 'event_url'),
            ('event_id', 'event_bit_id'),
            ('term_names', 'term_names'),
            ('term_values', 'term_values')]
    for a in attr:
        a_dict = dict(zip(df.index, df[a[1]]))
        nx.set_node_attributes(g, a[0], a_dict)
    return g

def save_json(g, filename):
    jsonfile_path = 'app/static/js/json/{0}.json'.format(filename)
    d = json_graph.node_link_data(g)
    with open(jsonfile_path, 'w') as f:
        json.dump(d, f)
    return jsonfile_path[4:]

def generate_json_graph(event_date='2015-10-07', venue_region='NY'):
    df = db_lookup(event_date, venue_region)
    g, df2 = graph_from_df(df) 
    g_attr = graph_add_attributes(g, df2)
    filename = event_date + '_' + venue_region
    return save_json(g_attr, filename)


if __name__ == '__main__':
    j_file = generate_json_graph(event_date='2015-10-07', venue_region='CA')

