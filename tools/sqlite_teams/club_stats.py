from tools.constants import ALL_GAMEID_COUNTRY_MAPPING
from tools.constants import ALL_GAMEID_NAME_MAPPING
from tools.constants import GAMENAME_ID_LEAGUE
from tools.constants import TABLE_NAME_ALL_GAMES_WITH_IDS
from tools.constants import TABLE_NAME_LONG_TERM_STATS
from tools.utils import compress_df
from tools.utils import df_to_sqlite_table
from tools.utils import sqlite_table_to_df

import logging
import math


log = logging.getLogger(__name__)

"""
# leauge
# "eng": {
#   1: "premier_league",
#   2: "championship",
#   3: "league_one",
#   4: "league_two"},

# cup
# "eng": {
#   1: "fa_cup",
#   2: "league_cup",
#   3: "community_shield",
"""


class TeamStatsShortTerm:
    pass


class TeamStatsLongTerm:
    """
    ultimate dataframe setup for AI with columns:
    date    home    away    home_mood     away_mood     home_expectancy     away_expectancy

    ------------
    EXPECTANCY
    ------------
    expectancy of team is a function of:
    - cup and league: team_rating --> 1000 starting points etc on team basis
    - cup and league: players_rating --> 1000 starting points etc on player basis

    ------------
    MOOD
    ------------
    mood of the team (coach, players, public opinion, etc) is a function of:
    - 1. league: current avg_points (per game) compared to long term avg_points (per game) in previous years
    - 2. league: current avg_gf (per game) compared to long term avg_gf (per game) in previous years
    - 3. league: current avg_ga (per game) compared to long term avg_ga (per game) in previous years
    - 4. league: current place compared to long term place (average in previous years)
    - 5. cup: nr of cups (times cup_coefficient) in which the team is currently active compared to nr cups (at same previous years)

    NOTE_1: what to do with teams promoting/relegate ??
        if milan plays 2004: serie_a, 2005: serie_a, 2006: serie_b, and in 2007: serie_a <-- how do calc avg_points_per_game?
        lets do it per league, so we compare 2007 with 2005 and 2004
    NOTE_2: If team plays for first time in league and it just relegated:
                - then we expect it to promote (we assume in top2 places)
                - we ignore avg_points, avg_gf, and avg_ga
    NOTE_3: If team plays for first time in league and it just promoted:
                - then we expect it to avoid relegation (we assume above bottom2 places)
                - we ignore avg_points, avg_gf, and avg_ga

    plan of attack:
    # - Determine bottom 2 places per season per game_name_id < -- To get this we need to know
    #       nr of teams per game_name_id per season
    # - Calc points per team_id per season per game_name_id
    # - Calc gf per team_id per season per game_name_id
    # - Calc ga per team_id per season per game_name_id

    # we need a "long_term" table like this
    team_id  season  game_name_id  nr_games_home  nr_games_away     points  gf  ga
          1       1             1            1.0            NaN          6   2   0
          1       2             2            2.0            NaN          1   1   5
          2       1             1            3.0            1.0          0   3   2
          2       2             2            2.0            1.0          2   1   1
          3       1             1            2.0            2.0          1   4   3
          3       2             2            1.0            2.0          1   2   1
          4       2             2            1.0            3.0          3   8   1
          4       1             1            NaN            3.0          6   1   2

    # and we need to update TABLE_NAME_ALL_GAMES_WITH_IDS with columns:
    tot_games       <-- per team_id per season per game_name_id
    tot_points      <-- per team_id per season per game_name_id
    tot_gf          <-- per team_id per season per game_name_id
    tot_ga          <-- per team_id per season per game_name_id

    # 2017-08-08	Coventry City	Blackburn Rovers	1:3	1	3	http://www.worldfootball.net/report/league-cup-2017-2018-1-runde-coventry-city-blackburn-rovers/	Mark Robins	Tony Mowbray	[Liam O Brien, Jack Grimmer, Rod McDonald, Dominic Hyam, Chris Camwell, Jodi Jones, Devon Kelly Evans, Jordan Shipley, Ben Stevenson, Maxime Biamou, Duckens Nazon]	[David Raya, Ryan Nyambe, Derrick Williams, Charlie Mulgrew, Elliott Ward, Paul Caddis, Richard Smallwood, Liam Feeney, Corry Evans, Elliott Bennett, Dominic Samuel]	1. Round	1	1:2				0	0	2017	cup	league_cup	eng	all_games_eng_league_cup.csv	136	70	16	2	4
    # 2009-02-24	Coventry City	Blackburn Rovers	1:0	1	0	http://www.worldfootball.net/report/fa-cup-2008-2009-achtelfinale-coventry-city-blackburn-rovers/	Chris Coleman	Sam Allardyce	[Keiren Westwood, Stephen Wright, Danny Fox, Elliott Ward, Ben Turner, Michael Doyle, Aron Gunnarsson, Jordan Henderson, Leon Best, Freddy Eastwood, Clinton Morrison]	[Jason Brown, Christopher Samba, Zurab Khizanishvili, Aaron Mokoena, Danny Simpson, Keith Treacy, Martin Olsson, Tugay Kerimo u flu, Benny McCarthy, Carlos Villanueva, Jason Roberts]	24/02/2009	5	0:0				0	0	2008	cup	fa_cup	eng	all_games_eng_fa_cup.csv	136	70	15	2	4
    # 2012-10-03	Nottingham Forest	Blackburn Rovers	0:0	0	0	http://www.worldfootball.net/report/championship-2012-2013-nottingham-forest-blackburn-rovers/	Sean O'Driscoll	Eric Black	[Lee Camp, Dan Harding, Danny Collins, Daniel Ayala, Simon Gillett, Chris Cohen, Lewis McGugan, Andy Reid, Greg Halford, Henri Lansbury, Billy Sharp]	[Paul Robinson, Bradley Orr, Martin Olsson, Ga xebl Givet, Scott Dann, Grant Hanley, Jason Lowe, Mauro F xf rmica, Danny Murphy, Jordan Rhodes, Nuno Gomes]									2012	league	championship	eng	eng_championship_2012_2013_spieltag.csv	279	70	2	1	4
    # 2014-01-18	Nottingham Forest	Blackburn Rovers	4:1	4	1	http://www.worldfootball.net/report/championship-2013-2014-nottingham-forest-blackburn-rovers/	Billy Davies	Gary Bowyer	[Karl Darlow, Dan Harding, Danny Collins, Gonzalo Jara, Henri Lansbury, Andy Reid, Greg Halford, David Vaughan, Jamie Mackie, Jamie Paterson, Simon Cox]	[Simon Eastwood, Tommy Spurr, Grant Hanley, Scott Dann, Adam Henley, Jason Lowe, Ben Marshall, Lee Williamson, Chris Taylor, Tom Cairney, Rudy Gestede]									2013	league	championship	eng	eng_championship_2013_2014_spieltag.csv	279	70	2	1	4
    # 2014-10-25	Nottingham Forest	Blackburn Rovers	1:3	1	3	http://www.worldfootball.net/report/championship-2014-2015-nottingham-forest-blackburn-rovers/	Stuart Pearce	Gary Bowyer	[Karl Darlow, Dan Harding, Michael Mancienne, Kelvin Wilson, Jack Hunt, Michail Antonio, David Vaughan, Chris Burke, Robert Tesche, Matty Fryatt, Britt Assombalonga]	[Jason Steele, Grant Hanley, Alex Baptiste, Shane Duffy, Tom Cairney, Ben Marshall, Marcus Olsson, Lee Williamson, Ryan Tunnicliffe, Corry Evans, Jordan Rhodes]									2014	league	championship	eng	eng_championship_2014_2015_spieltag.csv	279	70	2	1	4


    """

    def __init__(self):
        pass

    def get_json_data(self):
        my_url = "https://www.worldfootball.net/report/ligue-1-2006-2007-as-monaco-osc-lille/"

        new_url = "https://securepubads.g.doubleclick.net/gampad/ads?gdfp_req=1&pvsid=324009158373288&correlator=3197424626384405&output=ldjh&callback=googletag.impl.pubads.callbackProxy1&impl=fif&eid=21062452%2C21063637%2C21063965&vrg=2019081501&guci=1.2.0.0.2.2.0.0&plat=1%3A32776%2C2%3A16809992%2C8%3A134250504&sc=1&sfv=1-0-35&ecs=20190823&iu=%2F53015287%2Fworldfootball.net_d_728x90_1&sz=728x90&cookie_enabled=1&bc=31&abxe=1&lmt=1566557988&dt=1566557988034&dlt=1566557987294&idt=671&frm=20&biw=1838&bih=981&oid=3&adx=-12245933&ady=-12245933&adk=1295409359&uci=1&ifi=1&u_tz=120&u_his=3&u_h=1080&u_w=1920&u_ah=1053&u_aw=1853&u_cd=24&u_nplug=3&u_nmime=4&u_sd=1&flash=0&url=https%3A%2F%2Fwww.worldfootball.net%2Freport%2Fligue-1-2006-2007-as-monaco-osc-lille%2F&dssz=32&icsg=554&std=0&vis=1&dmc=8&scr_x=0&scr_y=0&psz=0x0&msz=0x0&blev=0.96&bisch=1&ga_vid=1688681370.1566552801&ga_sid=1566557988&ga_hid=1798167071&fws=128&ohw=0"

        import requests

        # shots_url = 'http://stats.nba.com/stats/playerdashptshotlog?' + \
        #             'DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&' + \
        #             'Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&' + \
        #             'PlayerID=202322&Season=2014-15&SeasonSegment=&' + \
        #             'SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision='

        # request the URL and parse the JSON
        response = requests.get(new_url)
        response.raise_for_status()  # raise exception if invalid response
        blaaaat = response.json()

        shots = response.json()["resultSets"][0]["rowSet"]

        response = requests.get(my_url)
        data = response.json()

        import urllib.request
        import json

        with urllib.request.urlopen(my_url) as url:
            data = json.loads(url.read().decode())
            print(data)

    def get_nr_games(self, df_games):
        """Calculate nr games (home and away games separated) per team_id, season,
        game_name_id. This is done for league as well as cup.

        Example:
        raw_data = {
            "date": pd.to_datetime(
                [
                    "01-01-2011",
                    "02-01-2011",
                    "03-01-2011",
                    "04-01-2011",
                    "05-01-2011",
                    "06-01-2011",
                    "07-01-2011",
                    "08-01-2011",
                    "09-01-2011",
                    "10-01-2011",
                    "11-01-2011",
                    "12-01-2011",
                ]
            ),
            "home_id": [1, 2, 2, 3, 3, 2, 1, 2, 4, 3, 1, 2],
            "away_id": [3, 4, 2, 4, 4, 3, 3, 4, 2, 4, 4, 3],
            "season": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2],
            "game_name_id": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2],
        }

        df = pd.DataFrame(
            raw_data, columns=["date", "home_id", "away_id", "season", "game_name_id"]
        )
        
        This input should return df_nr_games:
        #    team_id  season  game_name_id  nr_games_home  nr_games_away  nr_games_total
        # 0        1       1             1            1.0            NaN  1.0
        # 1        1       2             2            2.0            NaN  2.0
        # 2        2       1             1            3.0            1.0  3.0
        # 3        2       2             2            2.0            1.0  3.0
        # 4        3       1             1            2.0            2.0  4.0
        # 5        3       2             2            1.0            2.0  3.0
        # 6        4       2             2            1.0            3.0  4.0
        # 7        4       1             1            NaN            3.0  3.0
        """

        for col in ["home_id", "away_id", "season", "game_name_id"]:
            assert col in df_games.columns

        home_nr_games = df_games.groupby(["home_id", "season", "game_name_id"])
        home_nr_games = home_nr_games.size().reset_index(name="nr_games")
        home_nr_games.rename(index=str, columns={"home_id": "team_id"}, inplace=True)

        away_nr_games = df_games.groupby(["away_id", "season", "game_name_id"])
        away_nr_games = away_nr_games.size().reset_index(name="nr_games")
        away_nr_games.rename(index=str, columns={"away_id": "team_id"}, inplace=True)

        df_nr_games = home_nr_games.merge(
            away_nr_games,
            on=["team_id", "season", "game_name_id"],
            how="outer",
            suffixes=("_home", "_away"),
        )

        # sum two columns. Nan (is possible in cup matches!!) becomes zero
        df_nr_games["nr_games_total"] = df_nr_games["nr_games_home"].fillna(
            0
        ) + df_nr_games["nr_games_away"].fillna(0)
        assert not df_nr_games["nr_games_total"].hasnans

        # Check if nr_games do make sense: the nr of league games in 1 season should
        # be equal for all teams
        # 1. get only league games
        df_league_games = df_nr_games[
            df_nr_games["game_name_id"].isin(GAMENAME_ID_LEAGUE)
        ]
        df_group_by = df_league_games.groupby(
            ["season", "game_name_id", "nr_games_total"]
        )
        df_too_few_games = df_group_by.size().reset_index(name="nr_teams")
        df_too_few_games.sort_values(
            by=["game_name_id", "season"], ascending=True, inplace=True
        )

        # Now, lets calc how many games are in the group_by (season and game_name_id)
        df_too_few_games["nr_teams_total"] = df_too_few_games.groupby(
            ["season", "game_name_id"]
        )["nr_teams"].transform("sum")
        # calc expected number of games
        df_too_few_games["exp_nr_games"] = (df_too_few_games["nr_teams_total"] - 1) * 2
        df_too_few_games.reset_index(inplace=True, drop=True)

        # df_too_few_games output below:
        # Seasons 2000, 2001, 2002, and 2003 are okay for game_name_id=1.
        # Season 2004 and 2005 are not okay! In 2004:
        # 2 teams played 36 games, 2 teams played 37 games, 16 teams played 38 games
        """
        season  game_name_id  nr_games_total nr_teams  nr_teams_total  exp_nr_games
        2000.0             1              38       20              20            38
        2001.0             1              38       20              20            38
        2002.0             1              38       20              20            38
        2003.0             1              38       20              20            38
        
        2004.0             1              36        2              20            38
        2004.0             1              37        2              20            38
        2004.0             1              38       16              20            38
        
        2005.0             1              35        2              20            38
        2005.0             1              36        3              20            38
        2005.0             1              37        8              20            38
        2005.0             1              38        7              20            38
        
        2006.0             1              34        1              20            38 
        """

        # get sum of the number of missing games.
        df_too_few_games["missing_games"] = (
            df_too_few_games["exp_nr_games"] - df_too_few_games["nr_games_total"]
        ) * df_too_few_games["nr_teams"]

        # calc how many games are missing in the group_by (season and game_name_id)
        df_too_few_games["missing_games_total"] = df_too_few_games.groupby(
            ["season", "game_name_id"]
        )["missing_games"].transform("sum")
        assert all(df_too_few_games["missing_games_total"] % 2 == 0)

        # total_missing_games = sum(df_too_few_games["missing_games"])
        # print("total_missing_games:" + str(total_missing_games))

        columns = ["season", "game_name_id", "exp_nr_games", "missing_games_total"]
        df_log = df_too_few_games[columns].copy()
        df_log.drop_duplicates(
            subset=["season", "game_name_id"], keep="first", inplace=True
        )
        nr_teams = df_log["exp_nr_games"] / 2 + 1
        # calc total nr games of whole competition
        df_log["exp_nr_games"] = (nr_teams * (nr_teams + 1)) / 2  # binomial coefficient
        # only select rows when missing_games_total > 0
        df_log = df_log[df_log["missing_games_total"] > 0]
        # total_nr_games = (nr_teams * (nr_teams+1)) / 2
        df_log["season"] = df_log["season"].astype(int)
        df_log["missing_games_total"] = df_log["missing_games_total"].astype(int)
        # convert game_name_id to string

        df_log["country"] = df_log["game_name_id"]
        df_log["game_name_id"] = df_log["game_name_id"].replace(ALL_GAMEID_NAME_MAPPING)
        df_log["country"] = df_log["country"].replace(ALL_GAMEID_COUNTRY_MAPPING)
        df_log.to_csv(
            path_or_buf="/work/data/missing_games.txt",
            header=True,
            index=None,
            sep=",",
            mode="w",
        )
        # 2) Then we get de specific id of teams that played to few games

    def test(self, nr_teams):
        """
        Factorial, but with addition <-- is called bionimal coefficient
        :param nr_teams:
        :return: total_nr_games of whole competition
        """
        total_nr_games = (nr_teams * (nr_teams + 1)) / 2
        return total_nr_games

        #
        # df_sum_games["joinn"] = df_sum_games["season"].map(str) + df_sum_games[
        #     "game_name_id"
        # ].map(str)
        # df_too_few_games["joinn"] = df_too_few_games["season"].map(
        #     str
        # ) + df_too_few_games["game_name_id"].map(str)
        #
        # df_too_few_games.set_index(["season", "game_name_id"], inplace=True)
        # df_sum_games.set_index(["season", "game_name_id"], inplace=True)
        # df_too_few_games.update(df_sum_games)
        # df_too_few_games.reset_index(inplace=True)
        #
        # df_too_few_games["nr_expec_games"] = df_sum_games

        """
        >> > df1.set_index('Code', inplace=True)
        >> > df1.update(df2.set_index('Code'))
        >> > df1.reset_index()  # to recover the initial structure
        """

        # df_too_few_games["nr_expec_games"] = df_too_few_games.groupby(
        #     ["season", "game_name_id"]
        # ).sum()
        #
        # df_too_few_games.sum(label="nr_teams", axis=0)

        # to get the ids of those teams (who played too few games) we do the following:
        # 1) determine how many games should be played.
        # aggregations = {"nr_games_total": "max", "nr_teams": ["max", "count"]}
        # columns = ["season", "game_name_id"]
        # df_expect_max_games = df_too_few_games.groupby(columns).agg(aggregations)
        # df_expect_max_games.sort_values(by=["game_name_id", "season"], inplace=True)
        # df_expect_max_games.reset_index(level=columns, inplace=True)
        #      season game_name_id nr_games_total nr_teams
        #                                     max      max count
        # 0    2000.0            1           38.0       20     1
        # 1    2001.0            1           38.0       20     1
        # 2    2002.0            1           38.0       20     1
        # 3    2003.0            1           38.0       20     1
        # 4    2004.0            1           38.0       16     3
        # 5    2005.0            1           38.0        8     4
        # we see that in seasons 2000, 2001, 2002 and 2003 that count(nr_teams) is 1:
        # there is only 1 group of (20) teams which all played 38 games. However, in
        # season 2004, 3 groups exists which played different nr of games. The
        # majority (16 teams) played 38 games.

        # f_first_last_gamedates.reset_index(level=columns, inplace=True, drop=False)

        # (df_obj_count_bracket > 1).sum(axis=1)

        #         aggregations = {
        #             "home_points": ["sum", "mean"],
        #             "home_goals": ["sum"],
        #             "away_goals": ["sum"],  # goals against in home games
        #         }
        #         columns = ["home_id", "season", "game_name_id"]
        #         home_stats = df_games.groupby(columns).agg(aggregations)

        # df_nr_games
        league_game_ids = GAMENAME_ID_LEAGUE

        return df_nr_games

    def get_home_points_gf_ga(self, df_games):
        """
        :return:
        #        team_id  season  game_name_id  points_sum  points_mean  gf  ga
        # 0            0  2014.0            10        36.0     2.571429  24  11
        """
        aggregations = {
            "home_points": ["sum", "mean"],
            "home_goals": ["sum"],
            "away_goals": ["sum"],  # goals against in home games
        }
        columns = ["home_id", "season", "game_name_id"]
        home_stats = df_games.groupby(columns).agg(aggregations)

        # home_stats has a multi-level column index
        # instead op df.columns = df.columns.drop_level() we do list-comprehension
        #    A     B
        #    x  y  y
        # 0  1  2  8
        # 1  3  4  9
        # Dropping the top level would leave two columns with the index 'y'.
        # That can be avoided by joining the names with the list comprehension:
        # # df.columns = ['_'.join(col) for col in df.columns]
        #     A_x A_y B_y
        # 0   1   2   8
        # 1   3   4   9
        home_stats.columns = ["_".join(col) for col in home_stats.columns]
        home_stats.reset_index(level=columns, inplace=True, drop=False)
        home_stats.rename(
            index=str,
            columns={
                "home_id": "team_id",
                "home_points_sum": "points_sum",
                "home_points_mean": "points_mean",
                "home_goals_sum": "gf",
                "away_goals_sum": "ga",
            },
            inplace=True,
        )
        return home_stats

    def get_away_points_gf_ga(self, df_games):
        """See get_home_points_gf_ga """
        aggregations = {
            "away_points": ["sum", "mean"],
            "away_goals": ["sum"],
            "home_goals": ["sum"],  # goals against in away games
        }
        columns = ["away_id", "season", "game_name_id"]
        away_stats = df_games.groupby(columns).agg(aggregations)
        away_stats.columns = ["_".join(col) for col in away_stats.columns]
        away_stats.reset_index(level=columns, inplace=True, drop=False)
        away_stats.rename(
            index=str,
            columns={
                "away_id": "team_id",
                "away_points_sum": "points_sum",
                "away_points_mean": "points_mean",
                "away_goals_sum": "gf",
                "home_goals_sum": "ga",
            },
            inplace=True,
        )
        return away_stats

    def update_with_points_gf_ga(self, df_nr_games, df_games):
        """Update df_nr_games with column 'points', 'gf', 'ga' """
        for col in ["home_id", "away_id", "season", "game_name_id"]:
            assert col in df_games.columns
        for col in [
            "team_id",
            "season",
            "game_name_id",
            "nr_games_home",
            "nr_games_away",
        ]:
            assert col in df_nr_games.columns

        # first add two tmp columns
        df_games.loc[df_games["home_goals"] > df_games["away_goals"], "home_points"] = 3
        df_games.loc[df_games["home_goals"] > df_games["away_goals"], "away_points"] = 0
        df_games.loc[
            df_games["home_goals"] == df_games["away_goals"], "home_points"
        ] = 1
        df_games.loc[
            df_games["home_goals"] == df_games["away_goals"], "away_points"
        ] = 1
        df_games.loc[df_games["home_goals"] < df_games["away_goals"], "home_points"] = 3
        df_games.loc[df_games["home_goals"] < df_games["away_goals"], "away_points"] = 0

        home_stats = self.get_home_points_gf_ga(df_games)
        away_stats = self.get_away_points_gf_ga(df_games)

        df_stats = home_stats.merge(
            away_stats,
            how="outer",
            on=["team_id", "season", "game_name_id"],
            suffixes=("_home", "_away"),
        )

        # add nr_games_home', 'nr_games_away' to stats
        df_nr_games_stats = df_nr_games.merge(
            df_stats,
            how="outer",
            on=["team_id", "season", "game_name_id"],
            suffixes=("_home", "_away"),
        )

        df_nr_games_stats = compress_df(df_nr_games_stats)
        return df_nr_games_stats

    def update_with_nr_teams(self, df_nr_games_stats):
        """Add 1 column 'nr_teams' to df. This is the total number of teams that particpated
        in that season in that game_name_id
        :return: df
        """
        aggregations = {"team_id": "count"}  # count nr teams of teams particpating

        columns = ["season", "game_name_id"]
        df_count_teams = df_nr_games_stats.groupby(columns).agg(aggregations)
        df_count_teams.reset_index(level=columns, inplace=True)
        df_count_teams.rename(index=str, columns={"team_id": "nr_teams"}, inplace=True)

        df_nr_games_stats_nr_games = df_nr_games_stats.merge(
            df_count_teams, how="outer", on=["season", "game_name_id"]
        )
        return df_nr_games_stats_nr_games

    def update_with_first_last_dates(self, df_nr_games_stats, df_games):
        """
        raw_data = {
            "date": pd.to_datetime(
                [
                    "01-01-2011",
                    "02-01-2011",
                    "03-01-2011",
                    "04-01-2011",
                    "05-01-2011",
                    "06-01-2011",
                    "07-01-2011",
                    "08-01-2011",
                    "09-01-2011",
                    "10-01-2011",
                    "11-01-2011",
                    "12-01-2011",
                ]
            ),
            "home_id": [1, 2, 2, 3, 3, 2, 1, 2, 4, 3, 1, 2],
            "away_id": [3, 4, 2, 4, 4, 3, 3, 4, 2, 4, 4, 3],
            "season": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2],
            "game_name_id": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2],
        }

        columns = ["date", "home_id", "away_id", "season", "game_name_id"]
        df = pd.DataFrame(raw_data, columns=columns)

        This input should return df_first_last_gamedates:
        #                           date            game_name_id
        #                          first       last        count
        # season game_name_id
        # 1      1            2011-01-01 2011-06-01            6
        # 2      2            2011-07-01 2011-12-01            6
        """
        aggregations = {
            "date": ["first", "last"],  # get first and last gamedate per group
            "game_name_id": "count",  # find the number of game_name_id entries
        }

        columns = ["season", "game_name_id"]
        df_first_last_gamedates = df_games.groupby(columns).agg(aggregations)
        df_first_last_gamedates.columns = [
            "_".join(col) for col in df_first_last_gamedates.columns
        ]
        df_first_last_gamedates.reset_index(level=columns, inplace=True, drop=False)
        df_first_last_gamedates.rename(
            index=str, columns={"game_name_id_count": "nr_games"}, inplace=True
        )
        df_merge = df_nr_games_stats.merge(
            df_first_last_gamedates, how="inner", on=["season", "game_name_id"]
        )
        return df_merge

    def run(self):
        self.get_json_data()
        df_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_GAMES_WITH_IDS)
        df_nr_games = self.get_nr_games(df_games)
        self.checker.check_nr_games(df_nr_games)
        df_long_term_stats = self.update_with_points_gf_ga(df_nr_games, df_games)
        df_long_term_stats = self.update_with_nr_teams(df_long_term_stats)
        df_long_term_stats = self.update_with_first_last_dates(
            df_long_term_stats, df_games
        )
        df_to_sqlite_table(
            df_long_term_stats,
            table_name=TABLE_NAME_LONG_TERM_STATS,
            if_exists="replace",
        )


class TeamStats:
    def __init__(self, team_id, team_name, country):
        self.team_id = team_id
        self.team_name = team_name
        self.country = country

    def get_competition_per_season(self):
        """
        :return: {
        2000: premier_league,
        2001: championship,
        2002: premier_league,
        } """

    def get_cups_per_season(self):
        """
        :return: {
        2000: ['fa_cup', 'league_cup', 'community_shield', 'champions_league_qual', etc
        2001: ['fa_cup', 'league_cup', 'community_shield', 'champions_league_qual', etc
        }
        """
        pass

    def get_score_per_season_per_cup(self):
        """
        :return:
        2000: { 'fa_cup': 50,
                'league_cup': 60,
                'community_shield': 10,
                'champions_league_qual: 100,
                'champions_league': 62},
        2000: { 'fa_cup': 50,
                'league_cup': 60,
                'community_shield': 10,
                'champions_league_qual: 100,
                'champions_league': 62,
                'playoff_championship': 100}
        """

    def run(self):
        self.get_competition_per_season()
