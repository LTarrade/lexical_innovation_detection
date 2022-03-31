# lexical_innovation_detection

This repository contains the scripts to reproduce the method of detecting and categorising lexical innovations described in *Detecting and categorising lexical innovations in a corpus of tweets*, as well as those used for the validation of the method. 

1. newForms_retrieval.py

	Script to retrieve the tokens that have appeared in a given period of time as well as their usage rate per month and other various information. 

	- **Input** : 

		- *--path_tokenizedTweets* : Path to the directory of tokenized tweets. The files containing the tweets must be named with the date on which the tweets they contain were produced in yyyy-mm format.
		- *--path_out* : Path to the directory that will contain the output. 
		- *--corresId* : Path to the file containing (json format) the corresponding id of each token (to save space when storing results). 
		- *--first_month* : First month from which new forms are researched (format : yyyy-mm) - included.
		- *--last_month* : Last month from which new forms are researched (format : yyyy-mm) - excluded.
		- *--period_duration* : Duration (number of months) of the period for which we want to recover the percentage of use of the forms.

	- **Output** : 

		- *df_newTokensAlign.csv* : dataframe containing the tokens that appeared in the selected period with their usage rates per month, information on the date of appearance, the number of occurrences and the number of users for each token. A version of this dataframe with relative usage rates is also created.
		- In parallel, various information are saved such as: the number of occurrences per form (overall and per month), the users who tweeted each month, and the users who used each of the forms identified as new each month.   


2. selection_categorization.py

	Script to identify words corresponding to changes or buzzes using a method of fitting to two reference curves: logistic and lognormal, as well as to detect the three phases of diffusion of each of these words.

	- **Input** : 

		- *--path_df* : Path to the dataframe containing the tokens that appeared in the selected period with their usage rates per month. 
		- *--path_dfRel* : Path to the dataframe containing the tokens that appeared in the selected period with their relative usage rates per month. 
		- *--path_out* : Path to the directory that will contain the output. 
		
	- **Output** : 

		- *df_changeAndBuzz_5yearsAlign.csv* : dataframe containing only the words that have been identified as buzz or change, with their three phases of diffusion.A version of this dataframe with relative usage rates is also created.<br/>*In our case, an additional manual sorting was required to remove all named entities from this dataframe. It's this final version that is available in "./data/".*

3. network_var.py 

	Script that, from the users' network, retrieves for each node (user) its incoming and outgoing degree, as well as its pageRank score and its clustering coefficient. 

	- **Input** : 

		- *--path_edges* : Path to the file contains all the ties between the users of the corpus. 
		The file should be in the following format:  <br/>
			\# Directed graph: \<name.ext\><br/>\# Nodes: \<nb_nodes\> Edges: \<nb_edges\><br/>\# source    target<br/>\<id_user\>  \<id_user\><br/>\<id_user\>  \<id_user\><br/>\<id_user\>  \<id_user\><br/>...<br/>\<id_user\>  \<id_user\> <br/>
			And source and target must be seprated by a tabulation. 

		- *--path_idUsers* : Path to json files containing the user IDs and corresponding users (for anonymisation).
		- *--path_out* : Path to the directory that will contain the output. 

	- **Output** : 

		- *df_usersNetwork.csv* : dataframe containing the values of the network variables for each users.

4. plot_fit.py 

	script that produces graphs representing for each type of word (buzz and change) the usage rate per month, the curve fitted to the reference function, and the three diffusion periods. 

	- **Input** : 

		- *--path_df* : Path to the dataframe containing the tokens that appeared in the selected period with their usage rates per month. 
		- *--path_dfRel* : Path to the dataframe containing the tokens that appeared in the selected period with their relative usage rates per month. 
		- *--path_out* : Path to the directory that will contain the output. 
		- *--path_idByForm* : Path to the json file containing the identifiers of words.
		
	- **Output** : 

		- *changes.png* and *buzzes.png* : graphs of each word identified as change or buzz. 

5. distrib.py 

	script that produces graphs representing the distributions of the network variables of the users of the buzzes and changes at the different phases of diffusion, as well as that of the users of the whole corpus. 

	- **Input** : 

		- *--path_df* : Path to the dataframe containing the tokens that appeared in the selected period with their usage rates per month. 
		- *--path_users* : Path to the dataframe containing the values of the network variables for each user.
		- *--path_out* : Path to the directory that will contain the output. 
		- *--path_idByForm* : Path to the json file containing the identifiers of words.
		- *--path_idByUser* : Path to the json file containing the identifiers of users.
		- *--path_usersByMonth* : path to the json file containing the users by month and by word.

	- **Output** : 

		- graphs representing the distributions of the network variables, and two json files containing respectively the values of the different distributions and results of the statistical tests.
