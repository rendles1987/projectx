from tools.constants import TABLE_NAME_ALL_GAMES_WITH_IDS
from tools.constants import TABLE_NAME_LONG_TERM_STATS
from tools.utils import sqlite_table_to_df, compress_df, df_to_sqlite_table

import logging
import pandas as pd


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
    mood of the team (coach, players, public opion, etc) is a function of:
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

    def get_nr_games(self, df_games):
        """Get nr games (home and away games seperated) per team_id, season, game_name_id"""

        """
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
        #    team_id  season  game_name_id  nr_games_home  nr_games_away
        # 0        1       1             1            1.0            NaN
        # 1        1       2             2            2.0            NaN
        # 2        2       1             1            3.0            1.0
        # 3        2       2             2            2.0            1.0
        # 4        3       1             1            2.0            2.0
        # 5        3       2             2            1.0            2.0
        # 6        4       2             2            1.0            3.0
        # 7        4       1             1            NaN            3.0
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
        home_stats.columns = ['_'.join(col) for col in home_stats.columns]
        home_stats.reset_index(level=columns, inplace=True, drop=False)
        home_stats.rename(index=str, columns={"home_id": "team_id",
                                              "home_points_sum": "points_sum",
                                              "home_points_mean": "points_mean",
                                              "home_goals_sum": "gf",
                                              "away_goals_sum": "ga"
                                              }, inplace=True)
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
        away_stats.columns = ['_'.join(col) for col in away_stats.columns]
        away_stats.reset_index(level=columns, inplace=True, drop=False)
        away_stats.rename(index=str, columns={"away_id": "team_id",
                                              "away_points_sum": "points_sum",
                                              "away_points_mean": "points_mean",
                                              "away_goals_sum": "gf",
                                              "home_goals_sum": "ga"
                                              }, inplace=True)
        return away_stats

    def update_with_points_gf_ga(self, df_nr_games, df_games):
        """Update df_nr_games with column 'points', 'gf', 'ga' """
        for col in ["home_id", "away_id", "season", "game_name_id"]:
            assert col in df_games.columns
        for col in ["team_id", "season", "game_name_id", "nr_games_home", "nr_games_away"]:
            assert col in df_nr_games.columns

        # first add two tmp columns
        df_games.loc[df_games['home_goals'] > df_games['away_goals'], 'home_points'] = 3
        df_games.loc[df_games['home_goals'] > df_games['away_goals'], 'away_points'] = 0
        df_games.loc[df_games['home_goals'] == df_games['away_goals'], 'home_points'] = 1
        df_games.loc[df_games['home_goals'] == df_games['away_goals'], 'away_points'] = 1
        df_games.loc[df_games['home_goals'] < df_games['away_goals'], 'home_points'] = 3
        df_games.loc[df_games['home_goals'] < df_games['away_goals'], 'away_points'] = 0

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
        aggregations = {
            "team_id": "count"  # count nr teams of teams particpating
        }

        columns = ["season", "game_name_id"]
        df_count_teams = df_nr_games_stats.groupby(columns).agg(aggregations)
        df_count_teams.reset_index(level=columns, inplace=True)
        df_count_teams.rename(index=str, columns={"team_id": "nr_teams"}, inplace=True)


        df_nr_games_stats_nr_games = df_nr_games_stats.merge(
            df_count_teams,
            how="outer",
            on=["season", "game_name_id"],
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
        df_first_last_gamedates.columns = ['_'.join(col) for col in df_first_last_gamedates.columns]
        df_first_last_gamedates.reset_index(level=columns, inplace=True, drop=False)
        df_first_last_gamedates.rename(index=str, columns={"game_name_id_count": "nr_games"}, inplace=True)
        df_merge = df_nr_games_stats.merge(
            df_first_last_gamedates,
            how="inner",
            on=["season", "game_name_id"])
        return df_merge

    def run(self):
        df_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_GAMES_WITH_IDS)
        df_nr_games = self.get_nr_games(df_games)
        df_long_term_stats = self.update_with_points_gf_ga(df_nr_games, df_games)
        df_long_term_stats = self.update_with_nr_teams(df_long_term_stats)
        df_long_term_stats = self.update_with_first_last_dates(df_long_term_stats, df_games)
        df_to_sqlite_table(df_long_term_stats, table_name=TABLE_NAME_LONG_TERM_STATS, if_exists='replace')


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
