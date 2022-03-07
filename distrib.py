from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd 
import numpy as np
import argparse
import scipy
import ujson

parser = argparse.ArgumentParser()
parser.add_argument("--path_df", type=str, help="path to the dataframe containing informations about the buzzes and changes")
parser.add_argument("--path_users", type=str, help="path to the dataframe containing the values of the network variables for each user")
parser.add_argument("--path_out", type=str, help="Path to the directory containing the output")
parser.add_argument("--path_idByForm", type=str, help="path to the json file containing the identifiers of words")
parser.add_argument("--path_idByUser", type=str, help="path to the json file containing for each id the corresponding user")
parser.add_argument("--path_usersByMonth", type=str, help="path to the json file containing the users by month and by word")
args = parser.parse_args()

path_df = args.path_df
path_users = args.path_users
path_out = args.path_out
path_idByForm = args.path_idByForm
path_idByUser = args.path_idByUser
path_usersByMonth = args.path_usersByMonth

df_users = pd.read_csv(path_users, index_col=0)
df_users.index = df_users.index.astype(str)
df_forms = pd.read_csv(path_df, index_col=0)
idByUser = ujson.load(open(path_idByUser))
idByUser_inv = {str(v):str(k) for k,v in idByUser.items()}
idByForm = ujson.load(open(path_idByForm))
users_byMonth_byForm = ujson.load(open(path_usersByMonth))

plt.rcParams['axes.axisbelow'] = True

# For each word, we recover the different phases of diffusion of the word and the users who used this word for the first time during these phases.
usersByPhase_byCat = {"change":{"innovation":set(), "propagation":set(), "fixation":set()}, "buzz":{"innovation":set(), "propagation":set(), "fixation":set()}}
usersNetwork = set([str(idByUser[u]) for u in df_users.index])

for i,form in enumerate(df_forms.index) : 
				
	cat = df_forms.loc[form,"type"]
		
	month = df_forms.loc[form,"period"].split(" - ")[0]
	months = [month]
	for i in range(60) :
		month = (datetime.strptime(month,"%Y-%m")+relativedelta(months=1)).strftime("%Y-%m")
		months.append(month)
	months = sorted(months)
	
	beginProp = df_forms.loc[form,"propagation_start"]
	endProp = df_forms.loc[form,"propagation_end_ex"]
	
	innovation_months = months[:beginProp]
	propagation_months = months[beginProp:endProp]
	fixation_months = months[endProp:]
	phases = {"innovation":innovation_months, "propagation":propagation_months, "fixation":fixation_months}
	
	previous_users = set()
	
	for p in ["innovation", "propagation", "fixation"] :
		users = set()
		for month in phases[p] : 
			if month in users_byMonth_byForm[str(idByForm[form])] :
				for u in users_byMonth_byForm[str(idByForm[form])][month] : 
					if u in usersNetwork and u not in previous_users :
						usersByPhase_byCat[cat][p].add(str(idByUser_inv[u]))
						previous_users.add(u)	


def plot_dist(var,lim=[]) : 
	
	dic = {}
	
	# glob
	var_allUsers = np.array(df_users[var].tolist())
	var_allUsers = var_allUsers[~np.isnan(var_allUsers)]
	
	# change
	var_changeUsers_innovation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["change"]["innovation"])][var].tolist())
	var_changeUsers_propagation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["change"]["propagation"])][var].tolist())
	var_changeUsers_fixation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["change"]["fixation"])][var].tolist())
	var_changeUsers_innovation = var_changeUsers_innovation[~np.isnan(var_changeUsers_innovation)]
	var_changeUsers_propagation = var_changeUsers_propagation[~np.isnan(var_changeUsers_propagation)]
	var_changeUsers_fixation = var_changeUsers_fixation[~np.isnan(var_changeUsers_fixation)]
	
	# buzz
	var_buzzUsers_innovation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["buzz"]["innovation"])][var].tolist())
	var_buzzUsers_propagation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["buzz"]["propagation"])][var].tolist())
	var_buzzUsers_fixation = np.array(df_users[df_users.index.isin(usersByPhase_byCat["buzz"]["fixation"])][var].tolist())
	var_buzzUsers_innovation = var_buzzUsers_innovation[~np.isnan(var_buzzUsers_innovation)]
	var_buzzUsers_propagation = var_buzzUsers_propagation[~np.isnan(var_buzzUsers_propagation)]
	var_buzzUsers_fixation = var_buzzUsers_fixation[~np.isnan(var_buzzUsers_fixation)]
	
	dic = {"var_allUsers":list(var_allUsers), "var_changeUsers_innovation":list(var_changeUsers_innovation), "var_changeUsers_propagation":list(var_changeUsers_propagation), "var_changeUsers_fixation":list(var_changeUsers_fixation), "var_buzzUsers_innovation":list(var_buzzUsers_innovation), "var_buzzUsers_propagation":list(var_buzzUsers_propagation), "var_buzzUsers_fixation":list(var_buzzUsers_fixation)}
	
	fig = plt.figure(figsize=[18,9])

	plt.title(str(var)+" of change and buzz users by period")
	plt.grid(axis="y")
	sns.boxplot(data=[var_buzzUsers_fixation, var_buzzUsers_propagation, var_buzzUsers_innovation, var_allUsers, var_changeUsers_innovation, var_changeUsers_propagation, var_changeUsers_fixation], palette=["#084C61", "#177E89", "#A3C5E1", "#FFC857", "#A3C5E1", "#177E89", "#084C61"], zorder=10, notch=True)
	plt.xticks([0,1,2,3,4,5,6], ["BUZZ\nfixation", "BUZZ\npropagation", "BUZZ\ninnovation", "GLOBAL", "CHANGE\ninnovation", "CHANGE\npropagation", "CHANGE\nfixation"])
	if len(lim)!=0 : 
		plt.ylim(lim)

	plt.savefig(path_out+var+".png")
	
	plt.show()
	
	# tests stat
	statistical_tests = {}
	statistical_tests["Changes"]=scipy.stats.kruskal(var_changeUsers_innovation,var_changeUsers_propagation,var_changeUsers_fixation)
	statistical_tests["Buzzes"]=scipy.stats.kruskal(var_buzzUsers_innovation,var_buzzUsers_propagation,var_buzzUsers_fixation)
	statistical_tests["Changes innovation - Changes propagation"]=scipy.stats.mannwhitneyu(var_changeUsers_innovation, var_changeUsers_propagation)
	statistical_tests["Changes propagation - Changes fixation"]=scipy.stats.mannwhitneyu(var_changeUsers_propagation, var_changeUsers_fixation)
	statistical_tests["Buzzes innovation - Buzzes propagation"]=scipy.stats.mannwhitneyu(var_buzzUsers_innovation, var_buzzUsers_propagation)
	statistical_tests["Buzzes propagation - Buzzes fixation"]=scipy.stats.mannwhitneyu(var_buzzUsers_propagation, var_buzzUsers_fixation)
	statistical_tests["Changes innovation - Buzzes innovation"]=scipy.stats.mannwhitneyu(var_buzzUsers_innovation, var_changeUsers_innovation)
	statistical_tests["Changes propagation - Buzzes propagation"]=scipy.stats.mannwhitneyu(var_buzzUsers_propagation, var_changeUsers_propagation)
	statistical_tests["Changes fixation - Buzzes fixation"]=scipy.stats.mannwhitneyu(var_buzzUsers_fixation, var_changeUsers_fixation)
	
	return (var, dic, statistical_tests)

def compare_withRandomSample(var, period, cat, lim=[]) : 
	
	real_data = np.array(df_users[df_users.index.isin(usersByPhase_byCat[cat][period])][var].tolist())
	real_data = real_data[~np.isnan(real_data)]
	
	nb = len(real_data)
	
	data = np.array(df_users[var].tolist()) 
	data = data[~np.isnan(data)]
	
	q25 = []
	q75 = []
	med = []
	
	for i in range(1000) :
		sample = np.random.choice(a=data, size=nb, replace=False)
		med.append(np.median(sample))
		q25.append(np.quantile(sample, 0.25))
		q75.append(np.quantile(sample, 0.75))
	
	med_min = np.quantile(med, 0.025)
	med_max = np.quantile(med, 0.975)
	q25_min = np.quantile(q25, 0.025)
	q25_max = np.quantile(q25, 0.975)
	q75_min = np.quantile(q75, 0.025)
	q75_max = np.quantile(q75, 0.975)
	
	plt.title(str(var)+"\n"+str(cat)+" - "+str(period))
	plt.grid(axis="y")
	sns.boxplot(data=[real_data], palette=["#FFC857"], zorder=10, notch=True)
	plt.xticks([0], labels=[str(nb)])
	
	plt.vlines(x=0, ymin=med_min, ymax=med_max, linewidth=10, color="#084C61", label="95% confidence interval")
	plt.vlines(x=0, ymin=q25_min, ymax=q25_max, linewidth=10, color="#084C61")
	plt.vlines(x=0, ymin=q75_min, ymax=q75_max, linewidth=10, color="#084C61")

	plt.rcParams['axes.axisbelow'] = True
	if len(lim)!=0 : 
		plt.ylim(lim)
	
	plt.legend()


r = plot_dist("pageRank",lim=[0,0.0000005])
ujson.dump(r[1], open(path_out+"08_"+r[0]+"_values.json", "w"))
#ujson.dump(r[2], open(path_out+"08_"+r[0]+"_stats.json", "w"))

r = plot_dist("clusterCoef",lim=[0,0.4])
ujson.dump(r[1], open(path_out+"08_"+r[0]+"_values.json", "w"))
#ujson.dump(r[2], open(path_out+"08_"+r[0]+"_stats.json", "w"))