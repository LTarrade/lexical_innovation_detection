import pandas as pd
import argparse
import logging
import ujson
import snap
import sys

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("./log/network_var.log")
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

# args
parser = argparse.ArgumentParser()
parser.add_argument("--path_edges", type=str, help="Path to the file contains all the links between the users of the corpus.")
parser.add_argument("--path_idUsers", type=str, help="Path to json files containing the id and corresponding users.")
parser.add_argument("--path_out", type=str, help="Path to the directory containing the output.")

args = parser.parse_args()

path_edges = args.path_edges
path_idUsers = args.path_idUsers
path_out = args.path_out

logger.info("path_edges : "+path_edges+" ; path_idUsers : "+path_idUsers+" ; path_out : "+path_out)

idUsers = ujson.load(open(path_idUsers))

logger.info("Loading the user graph.")
graph = snap.LoadEdgeList(snap.TNGraph, path_edges, 0, 1, '\t')
logger.info("Loading the user graph - ended.")

logger.info("Recovery of the in degree of each node.")
inDegree_byUserId = {}
InDegV = graph.GetNodeInDegV()
for i,item in enumerate(InDegV):
    inDegree_byUserId[str(item.GetVal1())] = item.GetVal2()
ujson.dump(inDegree_byUserId, open(path_out+"07_inDegree.json", "w"))
logger.info("Recovery of the in degree of each node - ended.")

logger.info("Recovery of the out degree of each node.")
outDegree_byUserId = {}
OutDegV = graph.GetNodeOutDegV()
for i,item in enumerate(OutDegV):
    outDegree_byUserId[str(item.GetVal1())] = item.GetVal2()
ujson.dump(outDegree_byUserId, open(path_out+"07_outDegree.json", "w"))
logger.info("Recovery of the out degree of each node - ended.")

logger.info("Recovery of the clustering coefficient of each node.")
clusterCoef_byUserId = {}
NIdCCfH = graph.GetNodeClustCfAll()
for item in NIdCCfH:
    clusterCoef_byUserId[str(item)] = NIdCCfH[item]
ujson.dump(clusterCoef_byUserId, open(path_out+"07_clusterCoef.json", "w"))
logger.info("Recovery of the clustering coefficient of each node - ended.")

logger.info("Recovery of the pageRank score of each node.")
pageRank_byUserId = {}
PRankH = graph.GetPageRank()
for item in PRankH:
    pageRank_byUserId[str(item)]=PRankH[item]
ujson.dump(pageRank_byUserId, open(path_out+"07_pageRank.json", "w"))
logger.info("Recovery of the pageRank score of each node - ended.")

df = pd.DataFrame(index=idUsers.keys())

df["in_degree"] = df.index.map(inDegree_byUserId)
df["out_degree"] = df.index.map(outDegree_byUserId)

df = df.fillna(0)

df["clusterCoef"] = df.index.map(clusterCoef_byUserId)
df["pageRank"] = df.index.map(pageRank_byUserId)

logger.info("Saving results in "+path_out+"df_usersNetwork.csv")
df.to_csv(path_out+"df_usersNetwork.csv")