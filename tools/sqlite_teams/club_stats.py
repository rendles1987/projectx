from tools.constants import TABLE_NAME_ALL_GAMES_WITH_IDS
from tools.constants import TABLE_NAME_ALL_TEAMS
from tools.utils import sqlite_table_to_df

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


class ManageTeamStats:
    def __init__(self):
        self.df_teams = sqlite_table_to_df(table_name=TABLE_NAME_ALL_TEAMS)
        self.df_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_GAMES_WITH_IDS)

    def get_nr_games(self, df):
        """Get nr games (home and away games seperated) per team_id, season, game_name_id
        :param df:
        :return: df
        """

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
            assert col in df.columns

        home_nr_games = df.groupby(["home_id", "season", "game_name_id"])
        home_nr_games = home_nr_games.size().reset_index(name="nr_games")
        home_nr_games.rename(index=str, columns={"home_id": "team_id"}, inplace=True)

        away_nr_games = df.groupby(["away_id", "season", "game_name_id"])
        away_nr_games = away_nr_games.size().reset_index(name="nr_games")
        away_nr_games.rename(index=str, columns={"away_id": "team_id"}, inplace=True)

        df_nr_games = home_nr_games.merge(
            away_nr_games,
            on=["team_id", "season", "game_name_id"],
            how="outer",
            suffixes=("_home", "_away"),
        )

        return df_nr_games

    def get_first_last_gamedates(self, df):
        """

        :param df:
        :return:
        """

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
        df_first_last_gamedates = df.groupby(columns).agg(aggregations)
        df_first_last_gamedates.reset_index(level=columns)
        return df_first_last_gamedates

    def run(self):
        df_nr_games = self.get_nr_games(self.df_games)
        df_first_last_gamedates = self.get_first_last_gamedates(self.df_games)
        print("hoi")


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
