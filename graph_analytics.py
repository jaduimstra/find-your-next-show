# Here's the SQL query for this run:
# 
# SELECT artist_name, artist_mbid, event_datetime FROM Events WHERE event_datetime BETWEEN '2015-09-25 01:00:00' AND '2015-09-27 01:00:00' AND venue_city = 'San Francisco'

get_ipython().magic(u'matplotlib inline')
import pickle
import pandas as pd
import numpy as np
import networkx as nx

all_info, all_terms = get_ipython().magic(u'run get_artist.py')
TODO: load above from a pickle

with open('df_sim_wt2.pik', 'w') as f:
    pickle.dump(df_sim_wt, f)

# Create df of normalized term weight vectors
# each `artist` in `all_info` has a ndarray for term weights, w, and term frequency, f.  We need to extract these arrays, normalize them via w / sqrt(w(dot)w) and then put them in a data frame.  The code below does that.

term_wt_norm_df = pd.DataFrame(data=[])
for artist in all_info:
    wt_vec = all_info[artist]['term_wt']
    # get rid of artist who were in Echonest but have no terms
    if wt_vec.sum() != 0:
        term_wt_norm_df[artist] = wt_vec/np.sqrt(np.dot(wt_vec, wt_vec))


# ## Generate similarity matrix
# We get the projection of each vector on all the others by doing df(transpose) multiplied by df to get a square matrix with dimension equal to the number of artists contained in the `all_info` data.

sim_wt_matrix = np.dot(np.transpose(term_wt_norm_df.values), term_wt_norm_df.values)



# ### Check diagonal for normalization
# Diagonal values should all be 1 unless the vector was all zeros and then it's `nan`

sim_wt_matrix.diagonal()

# this doesn't work
#import unittest
#t = TestCase()
#t.assertEqual(sim_wt_matrix.diagonal().sum(),len(sim_wt_matrix.diagonal()))

df_sim_wt = pd.DataFrame(data=sim_wt_matrix, index=term_wt_norm_df.columns, columns=term_wt_norm_df.columns)




# ## Create graph from weight similarity df
# see: http://stackoverflow.com/questions/21207872/construct-networkx-graph-from-pandas-dataframe

def graph_from_df(df):
    g = nx.from_numpy_matrix(df.values)
    g = nx.relabel_nodes(g, dict(enumerate(df.columns)))
    #nx.write_gml(g, "g.gml")
    #nx.write_gexf(g, "g.exf", encoding='utf-8', prettyprint=True, version='1.1draft')
    #nx.draw(g)
    print 'edges = {0}'.format(g.number_of_edges())
    print 'nodes = {0}'.format(g.number_of_nodes())
    return g

# get rid of edges less than 0.3 and self edges and fill those with zeros
dfw2 = df_sim_wt[(df_sim_wt > 0.3) & (df_sim_wt < .9999)].fillna(0)
g_dfw2 = graph_from_df(dfw2)
degree = g_dfw2.degree()
to_keep = [n for n in degree if degree[n] > 2]
g_sub_dfw2 = g_dfw2.subgraph(to_keep)
nx.draw(g_sub_dfw2)

print 'edges = {0}'.format(g_sub_dfw2.number_of_edges())
print 'nodes = {0}'.format(g_sub_dfw2.number_of_nodes())
#nx.write_gml(g_sub_dfw2, "g2.gml")



