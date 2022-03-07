from lmfit.models import StepModel, ConstantModel, LognormalModel
from scipy.misc import derivative
from multiprocessing import Pool
from functools import partial
import pandas as pd
import numpy as np
from math import *
import argparse
import logging
import math

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("./log/selection_categorization.log")
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

# args
parser = argparse.ArgumentParser()
parser.add_argument("--path_df", type=str, help="path to the dataframe containing the new forms and their usage rate per month")
parser.add_argument("--path_dfRel", type=str, help="path to the dataframe containing the new forms and their relative usage rate per month")
parser.add_argument("--path_out", type=str, help="Path to the directory containing the output")
args = parser.parse_args()

path_df = args.path_df
path_dfRel = args.path_dfRel
path_out = args.path_out

logger.info("path_df : "+path_df+" ; path_dfRel : "+path_dfRel+" ; path_out : "+path_out)

logger.info("Fitting the curves for the new words to the two reference functions: logistic and logNormal.")

df_5years = pd.read_csv(path_df, index_col=0)
df_5years_rel = pd.read_csv(path_dfRel, index_col=0)
df_5years_rel = df_5years_rel.set_index(df_5years.form)
df_5years = df_5years.set_index("form")

df_final = df_5years.copy()

# We add for all forms a first month at 0.
df_5years = df_5years.rename(columns={str(e):str(int(e)+1) for e in range(60)})
df_5years.insert(loc=0,column="0",value=0.0)
df_5years_rel = df_5years_rel.rename(columns={str(e):str(int(e)+1) for e in range(60)})
df_5years_rel.insert(loc=0,column="0",value=0.0)

# We divide tokens according to whether they are hashtags, words, symbols or other.
hashtags = df_5years[df_5years.index.str.startswith("#")]
symb = df_5years[df_5years.index.str.contains(r"^\W+$")]
words = df_5years[df_5years.index.str.contains(r"^(\w|-|')+$")]
others = df_5years[(~df_5years.index.isin(hashtags)) & (~df_5years.index.isin(symb)) & (~df_5years.index.isin(words))]

logger.info(str(len(words))+" words (any sequence of alpha-numeric characters, which may contain an apostrophe or a dash).")

# curve fitting function from a reference function (logNormal or logistic)
logNormal_model = LognormalModel()
logistic_model=StepModel(form='logistic')
def fit(form, log=False) : 
    
    x = [i for i in range(61)]
    y=df_5years_rel.loc[form].rolling(window=3, min_periods=0).mean()
    
    if log :

        params=logistic_model.guess(y, x=x)
        model = logistic_model 
    
    else :
        params=logNormal_model.guess(y, x=x)
        params.add("sigma", value=2)
        model = logNormal_model 
    
    result = model.fit(y, params, x=x)

    if log : 

        return {'form':form,
        'sigma':result.params['sigma'].value,
        'sigma_err':result.params['sigma'].stderr,
        'center':result.params['center'].value,
        'center_err':result.params['center'].stderr,
        'amplitude':result.params['amplitude'].value,
        'amplitude_er':result.params['amplitude'].stderr,    
        'redchi':result.redchi,
        'chisqr':result.chisqr}

    else : 
        maxPoint = np.where(result.best_fit==np.max(result.best_fit))[0][0]
        return {'form':form,
        'sigma':result.params['sigma'].value,
        'sigma_err':result.params['sigma'].stderr,
        'center':result.params['center'].value,
        'center_err':result.params['center'].stderr,
        'amplitude':result.params['amplitude'].value,
        'amplitude_er':result.params['amplitude'].stderr,
        'height':result.params['height'].value,
        'height_err':result.params['height'].stderr,
        'fwhm':result.params['fwhm'].value,
        'fwhm_err':result.params['fwhm'].stderr,
        'redchi':result.redchi,
        'chisqr':result.chisqr,
        'maxPoint':maxPoint}

# return the diffusion phases of a word
def phases_delimitation(form, log=False) : 
    
    x = [i for i in range(61)]
    y=df_5years_rel.loc[form].rolling(window=3, min_periods=0).mean()
    
    if log :

        params=logistic_model.guess(y, x=x)
        model = logistic_model 
    
    else :
        params=logNormal_model.guess(y, x=x)
        params.add("sigma", value=2)
        model = logNormal_model 
    
    result = model.fit(y, params, x=x)

    # detection of diffusion phases
    def f(x) : 
        ampl = result.params['amplitude'].value
        sigma = result.params['sigma'].value
        center = result.params['center'].value 
        if log : 
            return ampl*(1-(1/(1+math.exp((x-center)/sigma))))
        else : 
            return (ampl/(sigma*math.sqrt(2*math.pi)))*((math.exp(-((math.log(x)-center)**2/(2*sigma**2))))/x)

    maxPoint = np.where(result.best_fit==np.max(result.best_fit))[0][0]

    values_deriv_3 = []
    if log : 
        for x2 in range(0,61) : 
            values_deriv_3.append(derivative(f, x2, n=3, order=5, dx=1))
    else : 
        values_deriv_3=[0]
        for x2 in range(1,61) : 
            values_deriv_3.append(derivative(f, x2, n=3, order=5, dx=0.1))

    periods = {"innovation":(min(x), values_deriv_3.index(max(values_deriv_3[:values_deriv_3.index(min(values_deriv_3))]))), 
    "propagation":(values_deriv_3.index(max(values_deriv_3[:values_deriv_3.index(min(values_deriv_3))])), values_deriv_3.index(max(values_deriv_3[values_deriv_3.index(min(values_deriv_3)):]))),
    "fixation":(values_deriv_3.index(max(values_deriv_3[values_deriv_3.index(min(values_deriv_3)):])), max(x))}

    return (form,periods)

pool = Pool()

# We retrieve the results of the curve fitting for the two reference functions: logistic and logNormal, for all new tokens identified as words
lmfit_results = pool.map(partial(fit, log=True), words.index.tolist())
df_log = pd.DataFrame.from_records(lmfit_results, index="form")
df_log.to_csv(path_out+"05_lmfit_logistic_words.csv")

lmfit_results = pool.map(partial(fit, log=False), words.index.tolist())
df_logNorm= pd.DataFrame.from_records(lmfit_results, index="form")
df_logNorm.to_csv(path_out+"05_lmfit_logNorm_words.csv")

logger.info("Fitting the curves for the new words to the two reference functions: logistic and logNormal - ended.")
logger.info("Saving the results of these fits in the files"+path_out+"05_lmfit_logistic_words.csv and "+path_out+"05_lmfit_logNorm_words.csv.")

logger.info("Categorization of words as change or buzz.")
# We only select words used by at least 200 users.
df = df_5years[df_5years.nbUsers_period>=200]
df_log_min200 = df_log[df_log.index.isin(df.index)]
df_logNorm_min200 = df_logNorm[df_logNorm.index.isin(df.index)]

# Sorting on the output parameters of the logistic and lognormal curve fit
logNorm_select = df_logNorm_min200[(df_logNorm_min200.fwhm>=4) & (df_logNorm_min200.fwhm<=40) & (df_logNorm_min200.redchi<=0.00005) & (df_logNorm_min200.amplitude<=1.1) & (df_logNorm_min200.maxPoint>=21) & (df_logNorm_min200.maxPoint<=46) & (((df_logNorm_min200.center<=3.6) & (df_logNorm_min200.sigma<=0.65)) | ((df_logNorm_min200.center>3.6) & (df_logNorm_min200.center<=3.8) & (df_logNorm_min200.sigma<=0.35)) | ((df_logNorm_min200.center>3.8) & (df_logNorm_min200.sigma<=0.15)))]
logistic_select = df_log_min200[(((df_log_min200.center>=16) & (df_log_min200.center<=31) & (df_log_min200.sigma<=8)) | ((df_log_min200.center>31) & (df_log_min200.center<=46) & (df_log_min200.sigma<=7))) & (df_log_min200.redchi<0.00005) & (df_log_min200.amplitude>0.02) & (df_log_min200.center_err<5)]

logger.info("Categorization of words as change or buzz - ended.")

# to treat the possible cases where a word is classified in both categories
for form in logistic_select.index : 
    if form in logNorm_select.index : 
        if logistic_select.loc[form,"redchi"]<logNorm_select.loc[form,"redchi"] :
            logNorm_select = logNorm_select.drop(form) 
        else : 
            logistic_select = logistic_select.drop(form) 

logger.info(str(len(logNorm_select))+" words classified as buzzes, "+str(len(logistic_select))+" words classified as changes.")
logNorm_select.to_csv(path_out+"06_logNorm_select.csv")
logistic_select.to_csv(path_out+"06_logistic_select.csv")

logger.info("Results saving in "+path_out+"06_logistic_select.csv and "+path_out+"06_logistic_select.csv.")

logger.info("Retrieval of the diffusion phases of each word.")
# We recover for each buzz and change its diffusion phases (the beginning and end of the propagation period).
logisticForms = set(logistic_select.index.tolist())
logNormForms = set(logNorm_select.index.tolist())

df_final = df_final[(df_final.index.isin(logisticForms) | df_final.index.isin(logNormForms))]

results = pool.map(partial(phases_delimitation, log=True), logistic_select.index.tolist())
dic_start = {}
dic_end = {}
dic_type = {}
for r in results : 
    dic_start[r[0]] = r[1]["propagation"][0]-1
    dic_end[r[0]] = r[1]["propagation"][1]-1
    dic_type[r[0]] = "change"

results = pool.map(partial(phases_delimitation, log=False), logNorm_select.index.tolist())
for r in results : 
    dic_start[r[0]] = r[1]["propagation"][0]-1
    dic_end[r[0]] = r[1]["propagation"][1]-1
    dic_type[r[0]] = "buzz"

df_final['propagation_start'] = df_final.index.map(dic_start)
df_final['propagation_end_ex'] = df_final.index.map(dic_end)
df_final['type'] = df_final.index.map(dic_type)

logger.info("Retrieval of the diffusion phases of each word - ended.")

df_final_rel = df_5years_rel[df_5years_rel.index.isin(df_final.index)]
df_final.to_csv(path_out+"df_changeAndBuzz_5yearsAlign.csv")
df_final_rel.to_csv(path_out+"df_changeAndBuzz_5yearsAlign_rel.csv")

logger.info("Results saving in "+path_out+"df_changeAndBuzz_5yearsAlign.csv")

