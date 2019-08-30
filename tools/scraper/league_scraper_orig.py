#!/usr/bin/python3
# -*- coding: utf-8 -*-


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

from bs4 import BeautifulSoup
from pandas.compat import u

import os.path
import pandas as pd
import re
import requests
import time


season_list = [
    # '-2017-2018-spieltag/',
    # '-2016-2017-spieltag/',
    # '-2015-2016-spieltag/',
    # '-2014-2015-spieltag/',
    # '-2013-2014-spieltag/',
    # '-2012-2013-spieltag/',
    # '-2011-2012-spieltag/',
    # '-2010-2011-spieltag/',
    # '-2009-2010-spieltag/',
    "-2008-2009-spieltag/",
    "-2007-2008-spieltag/",
    "-2006-2007-spieltag/",
    "-2005-2006-spieltag/",
    "-2004-2005-spieltag/",
    "-2003-2004-spieltag/",
    "-2002-2003-spieltag/",
    "-2001-2002-spieltag/",
    "-2000-2001-spieltag/",
]

# --------------------------------------------
# --------------------------------------------
# --------------------------------------------
# handmatig: update de url voor elk seizoen en voor elke competitie
competition = str("eng-league-two")

# season = str('-2016-2017-spieltag/')
# csv_file_name = 'ned_eredivisie_2016_2017'
nr_secondes_sleep = int(3)
# total_round nederland altijd 34
total_rounds = int(46)
# --------------------------------------------
# --------------------------------------------
# --------------------------------------------

for season in season_list:

    # slash verwijderen van season anders error met wegschrijven naar csv
    csv_file_name = "eng-league-two" + season.replace("/", "")
    result_df = pd.DataFrame([])

    # de eerste 13 vd 42 rounds hebben geen doorklik link op de uitslag (dus ook geen opstelling etc)
    for round_nr in range(1, total_rounds + 1):
        print(
            "start retrieving all fixtures for round_nr %i season %s"
            % (round_nr, season)
        )

        # empty all lists
        tbl = None
        df_home_managers = []
        df_away_managers = []
        df_home_team_starting_11 = []
        df_away_team_starting_11 = []

        # http://www.worldfootball.net/schedule/esp-segunda-division-2017-2018-spieltag/38/
        url = "http://www.worldfootball.net/schedule/%s%s%i/" % (
            competition,
            season,
            round_nr,
        )

        # request html
        page = requests.get(url)

        # Parse html using BeautifulSoup
        soup = BeautifulSoup(page.content, "lxml")

        all_games_html = soup.find_all("table")
        time.sleep(nr_secondes_sleep)

        # create panda dataframe
        try:
            tbl = pd.read_html(str(all_games_html))[1].iloc[:, [0, 2, 4, 5]]
        except:
            pass

        # set columnname
        tbl.columns = ["date", "home", "away", "score"]

        # fill in play_round, data, score, home&away goals
        tbl["play_round"] = round_nr
        tbl["date"] = tbl["date"].ffill()
        tbl["score"] = tbl["score"].map(lambda x: x.split(" ")[0])
        tbl["home_goals"], tbl["away_goals"] = tbl["score"].str.split(":", 1).str

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

        for counter, url_item in enumerate(tbl["url"]):
            print(
                "start retrieving specific gamedetails for game_nr %i round_nr %i season %s"
                % (counter, round_nr, season)
            )

            try:
                soup = BeautifulSoup(requests.get(url_item).content, "lxml")
                specific_game_html = soup.find_all("table")
                time.sleep(nr_secondes_sleep)
            except:
                pass

            # managers
            home_managers = []
            away_managers = []

            if url_item == "no_url_exists":
                home_manager = ""
                away_manager = ""
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            elif (
                url_item
                == "http://www.worldfootball.net/report/serie-a-2012-2013-cagliari-calcio-as-roma/"
            ):
                home_manager = "Massimo Ficcadenti"
                away_manager = "Zdenek Zeman"
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            elif (
                url_item
                == "http://www.worldfootball.net/report/league-one-2008-2009-carlisle-united-colchester-united/"
            ):
                home_manager = "Greg Abbott"
                away_manager = "Paul Lambert"
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            elif (
                url_item
                == "http://www.worldfootball.net/report/premier-league-2004-2005-manchester-city-bolton-wanderers/"
            ):
                home_manager = "Carlos Ríos"
                away_manager = "Paco Herrera"
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            elif (
                url_item
                == "http://www.worldfootball.net/report/ligue-1-2004-2005-fc-istres-fc-sochaux/"
            ):
                home_manager = "???"
                away_manager = "Guy Lacombe"
                home_team_starting_11 = [
                    "niet bekend",
                    "Abdoulaye Faye",
                    "Noureddine Kacemi",
                    "Philippe Delaye",
                    "Moussa N Diaye",
                    "Adel Chedli",
                    "Laurent Courtois",
                    "Ibrahima Bakayoko",
                    "Steven Pelé",
                    "Laurent Mohellebi",
                    "Amor Kehiha",
                ]
                away_team_starting_11 = [
                    "niet bekend",
                    "niet bekend",
                    "Teddy Richert",
                    "Ibrahima Tall",
                    "Sylvain Monsoreau",
                    "Lionel Potillon",
                    "Romain Pitau",
                    "Francileudo Dos Santos",
                    "Jaouad Zairi",
                    "Wilson Oruma",
                    "Jérémy Ménez",
                ]
            elif (
                url_item
                == "http://www.worldfootball.net/report/segunda-division-2011-2012-fc-cartagena-celta-vigo/"
            ):
                home_manager = "Olivier Pantaloni"
                away_manager = "Cédric Daury"
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            elif (
                url_item
                == "http://www.worldfootball.net/report/ligue-2-2009-2010-ac-ajaccio-le-havre-ac/"
            ):
                home_manager = "Olivier Pantaloni"
                away_manager = "Cédric Daury"
                home_team_starting_11 = [""]
                away_team_starting_11 = [""]
            else:
                oa_managers_html = pd.read_html(str(specific_game_html))

                home_manager = ""
                away_manager = ""
                for element in oa_managers_html:
                    if "Manager" in str(element):
                        home_manager = element.iloc[0, 0].split(":")[1].lstrip()
                        away_manager = element.iloc[0, 1].split(":")[1].lstrip()
                        break

                # managers = pd.read_html(str(specific_game_html))[-2]
                # home_manager = managers.iloc[0, 0].split(':')[1].lstrip()
                # away_manager = managers.iloc[0, 1].split(':')[1].lstrip()

                # teamsheets
                home_team_starting_11 = []
                away_team_starting_11 = []

                # home players
                try:
                    home_team_html = pd.read_html(str(specific_game_html))[5]
                    # selecteer de starting_11 en verwijder \n
                    home_team = home_team_html.iloc[0:11, 1].replace(
                        r"\\n", " ", regex=True
                    )
                except Exception as e:
                    print("no home players available ?")
                    home_team = ""
                for st in home_team:
                    try:
                        # selecteer alleen de varchar from string (not numbers and ')
                        player = " ".join(re.findall("[a-zA-Z]+", st))
                    except TypeError:
                        player = "unknown"
                    home_team_starting_11.append(player)

                # away players
                try:
                    away_team_html = pd.read_html(str(specific_game_html))[6]
                    # selecteer de starting_11 en verwijder \n
                    away_team = away_team_html.iloc[0:11, 1].replace(
                        r"\\n", " ", regex=True
                    )
                except Exception as e:
                    print("no away players available ?")
                    away_team = ""
                for st in away_team:
                    try:
                        # selecteer alleen de varchar from string (not numbers and ')
                        player = " ".join(re.findall("[a-zA-Z]+", st))
                    except TypeError:
                        player = "unknown"
                    away_team_starting_11.append(player)

            df_home_managers.append(home_manager)
            df_away_managers.append(away_manager)
            df_home_team_starting_11.append(home_team_starting_11)
            df_away_team_starting_11.append(away_team_starting_11)

        print("add managers and players to dataframe for round_nr %i" % (round_nr))

        # add managers to dataframe
        tbl["home_manager"] = df_home_managers
        tbl["away_manager"] = df_away_managers

        # add teamsheets to dataframe
        tbl["home_sheet"] = df_home_team_starting_11
        tbl["away_sheet"] = df_away_team_starting_11

        # remove all '\n\ from the dataframe
        tbl = tbl.replace(r"\\n", " ", regex=True)

        # append data to resulting dataframe
        # ignore_index = True, otherwise the existing rows will be overwritten
        result_df = result_df.append(tbl, ignore_index=True)

    # write dataframe to a csv
    abs_path = (
        "/home/renier.kramer/Documents/werk/Machine_learning/soccer_predition/data/"
    )
    # csv_file_name --> staat helemaal bovenaan
    path = abs_path + csv_file_name + ".csv"
    # check if file already exists
    if os.path.isfile(path) == False:
        print("exporting to csv file " + csv_file_name)
        result_df.to_csv(path, sep="\t", encoding="utf-8")
    else:
        print("csv file" + csv_file_name + "already exists")
