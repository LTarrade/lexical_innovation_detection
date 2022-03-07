from datetime import datetime, timedelta
from collections import OrderedDict
from multiprocessing import Pool,cpu_count
import dateutil.relativedelta
import pandas as pd
import argparse
import logging
import ujson
import glob
import sys
import re
import os

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("./log/newForms_retrieval.log")
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

# args
parser = argparse.ArgumentParser()
parser.add_argument("--path_tokenizedTweets", type=str, help="Path to the directory of tokenized tweets")
parser.add_argument("--path_out", type=str, help="Path to the directory containing the output")
parser.add_argument("--path_idByForm", type=str, help="path to the json file containing the identifiers of words")
parser.add_argument("--first_month", type=str, help="First month from which new forms are researched (format : yyyy-mm) - included")
parser.add_argument("--last_month", type=str, help="Last month from which new forms are researched (format : yyyy-mm) - excluded")
parser.add_argument("--period_duration", type=int, help="Duration (number of months) of the period for which we want to recover the percentage of use of the forms", default=60)
args = parser.parse_args()

path_tokenizedTweets = args.path_tokenizedTweets
path_idByForm = args.path_idByForm
path_out = args.path_out
first_month = args.first_month
last_month = args.last_month
period_duration = args.period_duration

logger.info("Path of tokenized tweets : "+path_tokenizedTweets+" ; path out : "+path_out+" ; first month of the covered period : "+first_month+" ; last month (excluded) : "+last_month+" ; period duration : "+str(period_duration)+" months")


# from a file of tokenized tweets, retrieve the number of occurrences of each token, excluding mentions and urls. 
def nbOcc_retrieval(f) :

	occForms = {}

	fileName = os.path.basename(f).split("_tokenized")[0]

	logger.info("retrieval of the number of occurrences of each token for the tweets contained in the file %s."%fileName)
	month = os.path.basename(f)[:7]

	file = open(f)

	for line in file :

		tweet = ujson.loads(line.rstrip())
		
		nb_sent = tweet["nb_sent"]

		for sent in range(1,nb_sent+1) : 
			for token in tweet["tokenization"]["sentence_"+str(sent)]["tokens"] : 
				form = token.lower()
				if not form.startswith("@") and not form.startswith("http") and not form.startswith("www.") :
					id_form = corres_forms[form]
					if id_form not in occForms : 
						occForms[id_form]=0
					occForms[id_form]+=1

	logger.info("retrieval of the number of occurrences of each token for the tweets contained in the file %s - ended."%fileName)

	return (month, occForms)

# For a given month, retrieves the tokens which had no occurrence in the year preceding the observed month.  
def retrieve_newTokens(month) : 

	newForms = set()

	logger.info("Retrieves the new tokens of month "+month)

	# we recover the tokens used during one year before
	previous_tokens = set()
	previous_year = sorted([(datetime.strptime(month, "%Y-%m")-dateutil.relativedelta.relativedelta(months=i+1)).strftime(r"%Y-%m") for i in range(12)])
	for m in previous_year :
		for form in occForms_byMonth[m] : 
			previous_tokens.add(form)

	for form in occForms_byMonth_more100[month] : 
		if form not in previous_tokens : 
			newForms.add(form)

	logger.info("Retrieves the new tokens of month "+month+" - ended.")

	return (month, newForms)


# For a given month, finds all the users having used the tokens considered as new, and all the users who tweeted this month.
def retrieve_usersByForm(month) : 

	dic = {"usersByForm":{}, "users":set(), "occByForm":{}}
	
	files_to_treat = sorted(glob.glob(path_tokenizedTweets+month+"*"))

	logger.info("Retrieval of users of new tokens and users of month "+month+", "+str(len(files_to_treat))+" files to be processed.")
	
	for file in files_to_treat : 
		f = open(file)
		for line in f : 
			tweet = ujson.loads(line.rstrip())
			user_id = tweet["user_id"]
			dic["users"].add(user_id)
			tokens = []
			
			for sentence in tweet["tokenization"] : 
				tokens += [token.lower() for token in tweet["tokenization"][sentence]["tokens"]]
			
			for token in tokens : 

				if not token.startswith("@") and not token.startswith("http") and not token.startswith("www.") :
					
					id_form = corres_forms[token]
					
					if id_form in newForms_infos and month in newForms_infos[id_form]["period"] :
						
						if id_form not in dic["occByForm"] :
							dic["usersByForm"][id_form]=set()
							dic["occByForm"][id_form]=0

						dic["usersByForm"][id_form].add(user_id)  
						dic["occByForm"][id_form]+=1

	logger.info("Retrieval of users of new forms and users of month "+month+" - ended.")

	return (month,dic)


logger.info("Retrieval of the number of occurrences of each token (lowercase - excluding mentions and urls), by month and in the corpus.")

filesToTreat = glob.glob(path_tokenizedTweets+"*")
corres_forms = ujson.load(open(path_idByForm))
corres_forms = {k:str(v) for k,v in corres_forms.items()}
corres_forms_inv = {str(v):k for k,v in corres_forms.items()}

try :
	pool = Pool(processes=cpu_count()-2)
	result = pool.map(nbOcc_retrieval, filesToTreat)
finally:
	pool.close()
	pool.join()

occForms = {}
occForms_byMonth = {}

for i,r in enumerate(result) :
	
	month = r[0]
	occByForm = r[1]

	if month not in occForms_byMonth : 
		occForms_byMonth[month] = {}

	for form in occByForm : 
		if form not in occForms : 
			occForms[form] = 0 
		if form not in occForms_byMonth[month] : 
			occForms_byMonth[month][form]=0

		occForms[form]+=occByForm[form]
		occForms_byMonth[month][form]+=occByForm[form]

logger.info("Save of results in "+path_out)
ujson.dump(occForms, open(path_out+"00_occForms.json", "w"))
ujson.dump(occForms_byMonth, open(path_out+"01_occForms_byMonth.json", "w"))

#occForms = ujson.load(open(path_out+"00_occForms.json"))
#occForms_byMonth = ujson.load(open(path_out+"01_occForms_byMonth.json"))

logger.info("Retrieval of the number of occurrences of each token (lowercase - excluding mentions and urls), by month and in the corpus - ended.")


# we keep only the tokens that have more than 100 occurrences in the whole corpus
formsMore100 = set([f for f in occForms if occForms[f]>100])
occForms_byMonth_more100 = {}
for month in occForms_byMonth : 
	occForms_byMonth_more100[month] = {}
	for form in occForms_byMonth[month] : 
		if form in formsMore100 : 
			occForms_byMonth_more100[month][form]=occForms_byMonth[month][form]

# we recover the months to be covered
# "months" contains the months for which we want to recover the new forms
first_month = datetime.strptime(first_month, "%Y-%m")
last_month = datetime.strptime(last_month, "%Y-%m")
months = list(OrderedDict(((first_month + timedelta(_)).strftime(r"%Y-%m"), None) for _ in range((last_month - first_month).days)).keys())

months_toCheck = sorted(months+[(datetime.strptime(months[-1], "%Y-%m")+dateutil.relativedelta.relativedelta(months=i+1)).strftime(r"%Y-%m") for i in range(period_duration)])

logger.info("Retrieval of tokens occurred between "+first_month.strftime(r"%Y-%m")+" and "+last_month.strftime(r"%Y-%m")+" (excluded)")

try :
	pool = Pool(processes=cpu_count()-2)
	result = pool.map(retrieve_newTokens, months)
finally:
	pool.close()
	pool.join()

newForms_byMonth = {}
newForms_infos = {}
for r in sorted(result) : 
	newForms_byMonth[r[0]]=r[1]
	for id_form in newForms_byMonth[r[0]] : 
		if id_form not in newForms_infos :
			newForms_infos[id_form] = {"form":corres_forms_inv[id_form],"nbOcc":occForms[id_form], "nbOcc_period":0, "period":months_toCheck[months_toCheck.index(r[0]):months_toCheck.index(r[0])+period_duration]}

logger.info("Retrieval of tokens occurred between "+first_month.strftime(r"%Y-%m")+" and "+last_month.strftime(r"%Y-%m")+" (excluded) - ended.")


logger.info("Retrieval of users by new forms and users for each month.")
try :
	pool = Pool(processes=cpu_count()-2)
	result = pool.map(retrieve_usersByForm, months_toCheck)
finally:
	pool.close()
	pool.join()

usersByMonth = {}
usersByMonth_byForm = {}
usersByMonth_byForm_temp = {}

for r in result : 
	month = r[0]

	usersByForm = r[1]["usersByForm"]
	users = r[1]["users"]
	nbOcc = r[1]["occByForm"]

	usersByMonth[month] = list(users)

	for id_form in usersByForm : 
		newForms_infos[id_form]["nbOcc_period"]+=nbOcc[id_form]
		if id_form not in usersByMonth_byForm : 
			usersByMonth_byForm[id_form] = {}
		usersByMonth_byForm[id_form][month] = list(usersByForm[id_form])

del result 

logger.info("save of results in "+path_out)
ujson.dump(usersByMonth, open(path_out+"02_usersByMonth.json", "w"))
ujson.dump(usersByMonth_byForm, open(path_out+"03_usersByMonth_byForm.json", "w"))
ujson.dump(newForms_infos, open(path_out+"04_newForms_infos.json", "w"))

logger.info("Retrieval of users by new forms and users for each month - ended.")

logger.info("Saving informations concerning each new tokens")
# saving usage rates and information by new token
dic_forms = {}
for j,form in enumerate(newForms_infos) :
	dic_forms[form]={}
	nbUsers = set()
	for i,month in enumerate(sorted(newForms_infos[form]["period"])) : 
		if month not in usersByMonth_byForm[form] : 
			rate = 0
		else : 
			rate=len(usersByMonth_byForm[form][month])/len(usersByMonth[month])
			for user in usersByMonth_byForm[form][month] : 
				nbUsers.add(user)
		dic_forms[form][i] = rate
	dic_forms[form]["period"] = newForms_infos[form]["period"][0]+" - "+newForms_infos[form]["period"][-1]
	dic_forms[form]["form"] = newForms_infos[form]["form"]
	dic_forms[form]["nbOcc"] = newForms_infos[form]["nbOcc"]
	dic_forms[form]["nbOcc_period"] = newForms_infos[form]["nbOcc_period"]
	dic_forms[form]["nbUsers_period"] = len(nbUsers)

df = pd.DataFrame.from_dict(dic_forms,orient="index")
df_rel = df.iloc[:,:-5].copy()
for i,form in enumerate(df.index) :
	for col in df_rel.columns : 
		df_rel.loc[form,col]=df.loc[form,col]/df.iloc[i,:-5].sum()

logger.info("Saving informations concerning each new tokens - ended.")

df.to_csv(path_out+"df_newTokensAlign.csv")
df_rel.to_csv(path_out+"df_newTokensAlign_rel.csv")

logger.info("Informations about new tokens saved in "+path_out+"df_newTokensAlign.csv and "+path_out+"df_newTokensAlign_rel.csv")