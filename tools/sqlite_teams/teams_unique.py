from tools.csv_merger.csv_merger import SQLITE_FULL_PATH
import numpy as np
import os
import logging
import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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
        unique_team_country.reset_index(drop=True, inplace=True)

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

    def check_uniqueness(self):
        len_orig = len(self.df_unique_team_country)
        len_unique = len(self.df_unique_team_country["team"].unique())
        assert len_orig == len_unique

    def get_processed_teams(self, teams):
        processed_teams = []
        for team in teams:
            if team:
                processed_team = fuzz._process_and_sort(team, True, True)
                processed_teams.append({"processed": processed_team, "team": team})
        processed_teams.sort(key=lambda x: len(x["processed"]))
        return processed_teams

    def find_matching_teams(self, processed_teams, upper_bound=0.85):
        """Takes about 78 seconds for 2201 teams
        https://codereview.stackexchange.com/questions/193567/string-similarity-
        using-fuzzywuzzy-on-big-data
        """
        matching_teams = []
        for idx, team in enumerate(processed_teams):
            length_team = len(team["processed"])
            matcher = fuzz.SequenceMatcher(None, team["processed"])
            # we only compare team with teams that have longer string length (so from
            # index idx+1 until end of processed_teams)
            for idx2 in range(idx + 1, len(processed_teams)):
                team2 = processed_teams[idx2]
                length_team2 = len(team2["processed"])
                # before we actually calc ratio with fuzz we compare string length
                # minimal 85% of
                if 2 * length_team / (length_team + length_team2) < upper_bound:
                    break
                matcher.set_seq2(team2["processed"])
                if (
                    matcher.quick_ratio() >= upper_bound
                    and matcher.ratio() >= upper_bound
                ):  # should also try without quick_ratio() check
                    log.warning(
                        f'found matching teams: {team["team"]}, {team2["team"]}'
                    )
                    matching_teams.append((team["team"], team2["team"]))
        return matching_teams

    def find_teams_alike(self):
        teams = self.df_unique_team_country["team"].to_list()
        processed_teams = self.get_processed_teams(teams)
        matching_teams = self.find_matching_teams(processed_teams)
        # TODO: hier geeindigt: zal ik matching_teams nog aan aan land en competitie
        #  koppelen ofzo ?? of nog sorteren op ratio?

    def run(self):
        self.open_sqlite_connection()
        self.df_unique_team_country = self.get_unique_team_country()
        self.check_uniqueness()
        self.find_teams_alike()
