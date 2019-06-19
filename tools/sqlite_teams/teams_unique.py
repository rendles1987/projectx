from tools.csv_merger.csv_merger import SQLITE_FULL_PATH
import numpy as np
import os
import logging
import sqlite3
import pandas as pd

log = logging.getLogger(__name__)


class TeamsUnique:
    def __init__(self):
        pass

    def open_sqlite_connection(self):
        assert os.path.isfile(SQLITE_FULL_PATH)
        log.debug(f"connect to sqlite {SQLITE_FULL_PATH}")
        self.connex = sqlite3.connect(SQLITE_FULL_PATH)

    def close_sqlite_connection(self):
        self.connex.close()

    def get_home_away_country(self, game_type=None, international=False):
        # This 'cur' object lets us actually send messages to our DB and receive results
        assert game_type in ["cup", "league"]
        if international:
            query = """
                    SELECT
                        home,
                        away,
                        country
                    FROM
                        {}
                    WHERE
                        country == 'eur';
                        """.format(
                game_type
            )
        else:
            query = """
                    SELECT
                        home,
                        away,
                        country
                    FROM
                        {}
                    WHERE
                        country != 'eur';
                        """.format(
                game_type
            )
        df = pd.read_sql_query(query, self.connex)
        return df

    def get_unique_team_country(self):
        """
        self.get_unique_teams() returns df with columns 'team' + 'country'
        :return:
        """
        df_cup_eur = self.get_home_away_country(game_type="cup", international=True)
        df_cup_eur = self.__get_unique_team_country(df_cup_eur)

        df_cup_non_eur = self.get_home_away_country(
            game_type="cup", international=False
        )
        df_cup_non_eur = self.__get_unique_team_country(df_cup_non_eur)

        df_league = self.get_home_away_country(game_type="league", international=False)
        df_league = self.__get_unique_team_country(df_league)

        # for teams in df_leauge we know from which country they are
        # for teams in unique_df_cup_non_eur we know from which country they are
        # However, for teams in df_cup_eur we might not know (in not in former two)
        # Find out if those teams exist
        teams_unknown_country = df_cup_eur[
            (~df_cup_eur["team"].isin(df_cup_non_eur["team"]))
            & (~df_cup_eur["team"].isin(df_league["team"]))
        ]

        teams_known_country = pd.concat([df_cup_non_eur, df_league])
        teams_known_country.drop_duplicates(inplace=True)

        unique_team_country = pd.concat([teams_known_country, teams_unknown_country])
        return unique_team_country

    def __get_unique_team_country(self, df):
        assert "home" in df.columns
        assert "away" in df.columns
        assert "country" in df.columns

        # method 1)
        all_teams = pd.unique(df[["home", "away"]].values.ravel("K"))
        all_teams.sort()

        # method 2)
        column_names = ["team", "country"]
        df_team_country = df[["home", "country"]]
        df_team_country.columns = column_names
        tmp_df = df[["away", "country"]]
        tmp_df.columns = column_names
        df_team_country = pd.concat([df_team_country, tmp_df])
        df_team_country.drop_duplicates(inplace=True)
        df_team_country.sort_values(by=["team"], inplace=True)

        # results of both methods should be the same
        if not len(all_teams) == len(df_team_country):
            not_found = np.setdiff1d(all_teams, df_team_country["team"].to_numpy())
            if len(not_found) > 0:
                raise AssertionError(
                    f"not found {not_found}: results of both methods should be the same"
                )
            not_found = np.setdiff1d(df_team_country["team"].to_numpy(), all_teams)
            if len(not_found) > 0:
                raise AssertionError(
                    f"not found {not_found}: results of both methods should be the same"
                )

        return df_team_country

    def check_uniqueness(sefl, df_unique_team_country):
        len_orig = len(df_unique_team_country)
        len_unique = len(df_unique_team_country["team"].unique())
        assert len_orig == len_unique

    def check_fuzzy_wuzzy(self, df_unique_team_country):
        # http://jonathansoma.com/lede/algorithms-2017/classes/fuzziness-matplotlib/fuzzing-matching-in-pandas-with-fuzzywuzzy/
        # fuzz is used to compare TWO strings
        from fuzzywuzzy import fuzz
        # process is used to compare a string to MULTIPLE other strings
        from fuzzywuzzy import process
        print("hoi")
        pass

    def run(self):
        self.open_sqlite_connection()
        df_unique_team_country = self.get_unique_team_country()
        self.check_uniqueness(df_unique_team_country)
        self.check_fuzzy_wuzzy(df_unique_team_country)
        print("hoi")
