from lmfit.models import StepModel, ConstantModel, LognormalModel
from dateutil.relativedelta import relativedelta
from scipy.misc import derivative
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import numpy as np
import argparse
import ujson

# args
parser = argparse.ArgumentParser()
parser.add_argument("--path_df", type=str, help="path to the dataframe containing the buzzes and changes and their usage rate per month")
parser.add_argument("--path_dfRel", type=str, help="path to the dataframe containing the buzzes and changes and their relative usage rate per month")
parser.add_argument("--path_out", type=str, help="Path to the directory containing the output")
parser.add_argument("--path_idByForm", type=str, help="path to the json file containing the identifiers of words")

args = parser.parse_args()

path_df = args.path_df
path_dfRel = args.path_dfRel
path_out = args.path_out
path_idByForm = args.path_idByForm

idByForm = ujson.load(open(path_idByForm))

df_forms = pd.read_csv(path_df, index_col=0)
df_forms_rel = pd.read_csv(path_dfRel, index_col=0)

df_forms = df_forms.rename(columns={str(e):str(int(e)+1) for e in range(60)})
df_forms.insert(loc=0,column="0",value=0.0)
df_forms_rel = df_forms_rel.rename(columns={str(e):str(int(e)+1) for e in range(60)})
df_forms_rel.insert(loc=0,column="0",value=0.0)

df_forms_justRate = df_forms.iloc[:,:-7]

logNormal_model = LognormalModel()
logistic_model=StepModel(form='logistic')

def best_fit(form, log=False) : 
    
    x = [i for i in range(61)]
    y=df_forms_justRate.loc[form].rolling(window=3, min_periods=0).mean()
    
    if log :

        params=logistic_model.guess(y, x=x)
        model = logistic_model 
    
    else :
        params=logNormal_model.guess(y, x=x)
        params.add("sigma", value=2)
        model = logNormal_model 
    
    result = model.fit(y, params, x=x)
    
    return result.best_fit

df_forms_changes = df_forms[df_forms.type=="change"].sort_values("nbUsers_period", ascending=False)
df_forms_buzzes = df_forms[df_forms.type=="buzz"].sort_values("nbUsers_period", ascending=False)

def plot_words(df, log=False, name="img") : 
    
    plt.figure(figsize=[40,4*round((len(df)+1)/4)])

    for i,form in enumerate(df.index) :
        
        if i==len(df)-1 : 
            plt.tight_layout()
            plt.show()
            
            plt.savefig(path_out+name+".png")
            break

        idForm = str(idByForm[form])

        month = (datetime.strptime(df_forms.loc[form,"period"].split(" - ")[0],"%Y-%m")-relativedelta(months=1)).strftime("%Y-%m")
        months = [month]
        for j in range(60) :
            month = (datetime.strptime(month,"%Y-%m")+relativedelta(months=1)).strftime("%Y-%m")
            months.append(month)

        beg_propag = df_forms.loc[form,"propagation_start"]+1
        end_propag = df_forms.loc[form,"propagation_end_ex"]+1

        ratesOfUse = df_forms_justRate.loc[form].rolling(window=3, min_periods=0).mean()
        #ratesOfUse = df_forms_rel.loc[form].rolling(window=3, min_periods=0).mean()
        if log : 
            fit = best_fit(form, log=True)
        else : 
            fit = best_fit(form, log=False)

        plt.subplot(round((len(df)+1)/4),4,i+1)

        plt.title(form, fontsize=12)

        plt.axvspan(0, beg_propag, color="#686963", alpha=0.1)
        plt.axvspan(beg_propag, end_propag, color="#686963", alpha=0.2)
        plt.axvspan(end_propag, 60, color="#686963", alpha=0.3)

        plt.plot(fit, label="best_fit", linewidth=4, color="#8AA29E")
        plt.plot(ratesOfUse, label="relative rate of use", linewidth=4, color="#3D5467")

        plt.xlim(0,60)
        plt.xticks(np.arange(0,61,2),[m for i,m in enumerate(months) if i%2==0], rotation=60)

        if i==0 :
            plt.legend()

plot_words(df_forms_changes, log=True, name="changes")
plot_words(df_forms_buzzes, log=False, name="buzzes")
