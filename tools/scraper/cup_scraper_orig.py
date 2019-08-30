#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from pandas.compat import u

import numpy as np
import os.path
import pandas as pd
import re
import requests
import time


"""
ahv http://docs.python-guide.org/en/latest/scenarios/scrape/
# deze gasten gebruikt worldfootball.net ecm BeautifulSoup4
1. https://gist.github.com/gfleetwood/15442d9a34cb63cce5df1feede84296b
2. https://gist.github.com/samkellett/62bb6fe13faaf9f6a1f6 <-- werkt!! ik moet ook iets met
# test to get fixtures from 1 webpage
# webpage = http://www.worldfootball.net/teams/afc-ajax/2018/3/
"""

"""
explenation script:
1. loop trough all play_rounds of a competition. Each round returns:
	- play date
	- home team name
	- away team name
	- score
	- url of the specific play_round
2. inside the loop we open de score url for more information. This returns:
	- home team manager name
	- away team manager name
	- home team starting 11 player names
	- away team starting 11 player names
3. multiple panda dataframes are created. After each round all this information is
   appended to a resulting dataframe. In the end this df is exported to a csv file.


note:
1. 	this script only loops trough all games of 1 competition of 1 year.
	user needs to predefine these (hardcoded) below
2. 	We import time since we scrape multiple webpages (html) using python for-loops
	we do not want to get banned, so after each request we sleep 4 seconds
3.	scraping eredivsie seasons 2000 to 2018 will take 6.5 hours with sleep = 4sec
4.	scraping eredivsie seasons 2000 to 2018 will take 4.8 hours with sleep = 3sec
5.	scraping premier league seasons 2000 to 2018 will take 8.0 hours with sleep = 4sec
6.	scraping premier league 2000 to 2018 will take 6.0 hours with sleep = 3sec
"""

nr_secondes_sleep = int(3)

# only these seasons available
season_list = [
    "-2017/",
    "-2016/",
    "-2015/",
    "-2014/",
    "-2013/",
    "-2012/",
    "-2011/",
    "-2010/",
    "-2009/",
    "-2008/",
]

# handmatig: update de url voor elk seizoen en voor elke competitie
competition = str("fra-trophee-des-champions")
csv_file_name = "all_games_" + competition + ".csv"

# per season 1 tbl
tbl = None
# all seasons in 1 result_df
result_df = pd.DataFrame([])

for season in season_list:
    if season in ["-2013/", "-2012/", "-2011/", "-2010/", "-2009/", "-2008/"]:
        competition = "fra-supercup"

    url = "http://www.worldfootball.net/all_matches/%s%s" % (competition, season)

    print("start retrieving all fixtures for season %s" % (season))
    # url = "http://www.worldfootball.net/all_matches/eng-fa-cup-2018-2019/
    # url = "http://www.worldfootball.net/all_matches/dfb-pokal-2000-2001/"
    # url = "http://www.worldfootball.net/all_matches/esp-copa-del-rey-2017-2018/"
    # url = "http://www.worldfootball.net/all_matches/fra-coupe-de-la-ligue-2011-2012/
    html = requests.get(url).content
    soup = BeautifulSoup(html, "lxml")
    all_games_html = soup.find_all("table")
    time.sleep(nr_secondes_sleep)

    tbl = None
    tbl = pd.read_html(str(all_games_html))[1].iloc[:, [0, 1, 2, 4, 5]]

    # set columnname
    tbl.columns = ["date", "time", "home", "away", "score"]

    # de eerste 21 rijen van tbl zien er zo uit:
    #                date   time               home                  away
    # 0      1. pre-round    NaN                NaN                   NaN
    # 1        19/08/2017  13:30    VVOG Harderwijk             VV Gemert
    # 2               NaN  13:30          Ter Leede             DHC Delft
    # 3               NaN  13:30      VV Buitenpost                   ZSV
    # 4               NaN  13:30         VV Capelle           RKSV Nuenen
    # 5               NaN  14:00          ACV Assen          USV Hercules
    # 6               NaN  14:00          VV Dongen    VV DOVO Veenendaal
    # 7               NaN  16:00  AFC Ajax Zaterdag       VV OJC Rosmalen
    # 8               NaN  16:00     Sparta Nijkerk      VPV Purmersteijn
    # 9               NaN  16:00            ADO '20         SV Spakenburg
    # 10              NaN  16:30         Chevremont             AVV Swift
    # 11              NaN  17:00   RKSV Groene Ster      SVV Scheveningen
    # 12              NaN  17:00    BVV/Caterpillar          VV Staphorst
    # 13              NaN  17:00          JVC Cuijk            SDC Putten
    # 14              NaN  18:00                EVV        Olde Veste '54
    # 15     2. pre-round    NaN                NaN                   NaN
    # 16       19/08/2017  13:30             DVS'33            Quick Boys
    # 17              NaN  14:00   ASWH Ido-Ambacht              HSV Hoek
    # 18              NaN  17:00        Achilles'29          OFC Oostzaan
    # 19              NaN  18:00                TEC   VV Rijnsburgse Boys
    # 20       20/08/2017  13:00    Koninklijke HFC       RKVV Westlandia
    # 21              NaN  13:30   Amsterdamsche FC           ASV De Dijk

    # fill all NaN values of column date with the "last known" date row above
    tbl["date"].fillna(method="pad", inplace=True)

    # # nan is a pain in the ass so we replace nans in column time with 9999
    # tbl['time'].fillna(9999, inplace=True)
    #
    # new column 'round_text'
    tbl["round_text"] = np.nan
    #
    # daar waar niet een '/' in tbl.date daar update we column round_text met
    # date (# bijv met '1 pre-round')
    # hint: you can use the invert (~) operator (which acts like a not for
    # boolean data) like: new_df = df[~df["col"].str.contains(word)]
    tbl.loc[~tbl.date.str.contains("/"), "round_text"] = tbl["date"]

    # # fill all NaN values of round_text with the "last known" date row above
    tbl["round_text"].fillna(method="pad", inplace=True)

    # new column 'play_round'
    tbl["play_round"] = np.nan

    # loop door column time, elke keer als time = 9999, dan play_round updaten
    play_round = int(1)
    for index, row in tbl.iterrows():
        if "/" not in row.date:
            tbl.set_value(index, "play_round", play_round)
            play_round += int(1)

    # fill all NaN values of play_round with the "last known" date row above
    tbl["play_round"].fillna(method="pad", inplace=True)

    # weggooien rijen
    tbl = tbl[tbl.date.str.contains("/")]

    # weggooien column
    tbl.drop(columns=["time"], inplace=True)

    # reset index of the dataframe since we deleted some rows
    tbl.reset_index()
    # You can use the invert (~) operator (which acts like a not for boolean data):
    # new_df = df[~df["col"].str.contains(word)]

    # the tbl['score'] looks now like:
    # '1:3 (1:1, 1:1, 1:3) aet'
    # '0:1 (0:1)'
    # '3:2 (0:0, 0:0, 0:0) pso'
    # from this we wil retrieve the score at 45 minutes, 90 minutes, 105
    # minutes, 120 minutes, and final score.

    # new column 'round_text'
    tbl["score_45"] = ""
    tbl["score_90"] = ""
    tbl["score_105"] = ""
    tbl["score_120"] = ""
    tbl["aet"] = False
    tbl["pso"] = False

    for index, row in tbl.iterrows():
        score = row["score"]

        if "aet" in score:
            tbl.set_value(index, "aet", True)
        elif "pso" in score:
            tbl.set_value(index, "pso", True)

        inside_brackets = score[score.find("(") + 1 : score.find(")")]
        count_elements = int(inside_brackets.count(",") + 1)

        for cnt in range(count_elements):
            if cnt == 0:
                score_45 = inside_brackets.split(" ")[0].replace(",", "")
                tbl.set_value(index, "score_45", score_45)
                # print 'score_45= ' + score_45
            elif cnt == 1:
                score_90 = inside_brackets.split(" ")[1].replace(",", "")
                tbl.set_value(index, "score_90", score_90)
                # print 'score_90= ' + score_90
            elif cnt == 2:
                score_105 = inside_brackets.split(" ")[2].replace(",", "")
                tbl.set_value(index, "score_105", score_105)
                # print 'score_105= ' + score_105
            elif cnt == 3:
                score_120 = inside_brackets.split(" ")[3].replace(",", "")
                tbl.set_value(index, "score_120", score_120)
                # print 'score_120= ' + score_120

    # fill in final score
    tbl["score"] = tbl["score"].map(lambda x: x.split(" ")[0].split("n")[1])

    # fill in home & away goals
    tbl["home_goals"], tbl["away_goals"] = tbl["score"].str.split(":", 1).str

    # new column 'season'
    tbl["season"] = season

    # add game url to dataframe
    url2 = []
    all_links = [
        a["href"] for a in soup.find_all("a", href=True) if "report" in a["href"]
    ]
    for link in all_links:
        url_long = "http://www.worldfootball.net{}".format(link)
        url2.append(url_long)

    # new column 'season'
    tbl["url"] = "no_url_exists"

    # apperantly, not all games do have an url (in the website), so we need
    # to determine which games do and which games do not have an url (to get
    # detailed information)

    # to avoid we split strings into this e.g. ['VfL', 'Osnabr', 'xfcck']
    # we transfer the tbl column to a list and then work with unicodes
    # so we get in the end [u'VfL', u'Osnabr', u'ck']
    tbl["home"].fillna("DitWasEenNaNWaarde", inplace=True)
    home_to_list = tbl["home"].tolist()
    unicode_home_to_list = pd.Series(map(u, home_to_list))

    # the same for away teamnames
    tbl["away"].fillna("DitWasEenNaNWaarde", inplace=True)
    away_to_list = tbl["away"].tolist()
    unicode_away_to_list = pd.Series(map(u, away_to_list))

    # reset index of the dataframe since we deleted some rows
    tbl.reset_index(inplace=True)

    # first attempt
    for index, row in tbl.iterrows():
        find_home = re.findall(r"[\w']+", unicode_home_to_list[index])
        try:
            split_home = re.split("['-]", find_home)
        except:
            split_home = find_home

        find_away = re.findall(r"[\w']+", unicode_away_to_list[index])
        try:
            split_away = re.split("['-]", find_away)
        except:
            split_away = find_away
        sorted_split_home = sorted(split_home, key=len)
        sorted_split_away = sorted(split_away, key=len)
        # take the longest string and remove possible apostrophe
        longest_home = sorted_split_home[-1].replace("'", "").lower()
        longest_away = sorted_split_away[-1].replace("'", "").lower()
        look_up_strings = [longest_home, longest_away]
        for link in url2:
            if all(item in link for item in look_up_strings):
                tbl.set_value(index, "url", link)
                # remove link from url2 so it cannot be allocated twice
                url2.remove(link)
                break

    # second attempt
    for index, row in tbl.iterrows():
        if row["url"] == "no_url_exists":
            find_home = re.findall(r"[\w']+", unicode_home_to_list[index])
            try:
                split_home = re.split("['-]", find_home)
            except:
                split_home = find_home

            find_away = re.findall(r"[\w']+", unicode_away_to_list[index])
            try:
                split_away = re.split("['-]", find_away)
            except:
                split_away = find_away
            sorted_split_home = sorted(split_home, key=len)
            sorted_split_away = sorted(split_away, key=len)

            # take the 2nd-longest string and remove possible apostrophe
            try:
                longest_home = sorted_split_home[-2].replace("'", "").lower()
            # take the longest
            except:
                longest_home = sorted_split_home[-1].replace("'", "").lower()

            # take the 2nd-longest string and remove possible apostrophe
            try:
                longest_away = sorted_split_away[-2].replace("'", "").lower()
            # take the longest
            except:
                longest_away = sorted_split_away[-1].replace("'", "").lower()

            look_up_strings = [longest_home, longest_away]
            for link in url2:
                if all(item in link for item in look_up_strings):
                    tbl.set_value(index, "url", link)
                    # remove link from url2 so it cannot be allocated twice
                    url2.remove(link)
                    break
                else:
                    print(str(look_up_strings) + " not in url2")

    result_df = result_df.append(tbl, ignore_index=True)

# add game details to dataframe
# empty all lists
df_home_managers = []
df_away_managers = []
df_home_team_starting_11 = []
df_away_team_starting_11 = []

for counter, url_item in enumerate(result_df["url"]):
    if url_item == "no_url_exists":
        print("no_url_exists")
        home_manager = "no_url_exists"
        away_manager = "no_url_exists"
        home_team_starting_11 = ["no_url_exists"]
        away_team_starting_11 = ["no_url_exists"]
    else:
        print(
            "start retrieving specific gamedetails for counter %i url_item %s"
            % (counter, url_item)
        )
        soup = BeautifulSoup(requests.get(url_item).content, "lxml")
        specific_game_html = soup.find_all("table")
        time.sleep(nr_secondes_sleep)

        # managers
        home_managers = []
        away_managers = []

        managers = pd.read_html(str(specific_game_html))[-2]
        try:
            home_manager = managers.iloc[0, 0].split(":")[1].lstrip()
        except:
            home_manager = ""
        try:
            away_manager = managers.iloc[0, 1].split(":")[1].lstrip()
        except:
            away_manager = ""
        # teamsheets
        home_team_starting_11 = []
        away_team_starting_11 = []

        # selecteer de starting_11 en verwijder \n

        try:
            home_team_html = pd.read_html(str(specific_game_html))[5]
            home_team = home_team_html.iloc[0:11, 1].replace(r"\\n", " ", regex=True)
            for st in home_team:
                # selecteer alleen de varchar from string (not numbers and ')
                player = " ".join(re.findall("[a-zA-Z]+", st))
                home_team_starting_11.append(player)
        except:
            home_team_starting_11 = [""]

        try:
            away_team_html = pd.read_html(str(specific_game_html))[6]
            away_team = away_team_html.iloc[0:11, 1].replace(r"\\n", " ", regex=True)
            for st in away_team:
                # selecteer alleen de varchar from string (not numbers and ')
                player = " ".join(re.findall("[a-zA-Z]+", st))
                away_team_starting_11.append(player)
        except:
            away_team_starting_11 = [""]

    df_home_managers.append(home_manager)
    df_away_managers.append(away_manager)
    df_home_team_starting_11.append(home_team_starting_11)
    df_away_team_starting_11.append(away_team_starting_11)

# add managers to dataframe
print("add managers and players to dataframe")

result_df["home_manager"] = df_home_managers
result_df["away_manager"] = df_away_managers

# add teamsheets to dataframe
result_df["home_sheet"] = df_home_team_starting_11
result_df["away_sheet"] = df_away_team_starting_11

# remove all '\n\ from the dataframe
result_df = result_df.replace(r"\\n", " ", regex=True)
result_df = result_df.replace(r"\t", " ", regex=True)

# append data to resulting dataframe
# ignore_index = True, otherwise the existing rows will be overwritten
# result_df = result_df.append(tbl, ignore_index=True)


# write dataframe to a csv
abs_path = "/home/renier.kramer/Documents/werk/Machine_learning/soccer_predition/data/"
path = abs_path + csv_file_name
# check if file already exists
if os.path.isfile(path) == False:
    result_df.to_csv(path, sep="\t", encoding="utf-8")
else:
    print("csv file " + csv_file_name + " already exists")
