from fuzzywuzzy import fuzz
from tools.constants import ALL_GAMENAME_ID_MAPPING
from tools.constants import COUNTRY_ID_MAPPING
from tools.constants import GAMETYPE_ID_MAPPING
from tools.constants import SQLITE_FULL_PATH
from tools.constants import TABLE_NAME_ALL_GAMES
from tools.constants import TABLE_NAME_ALL_GAMES_WITH_IDS
from tools.constants import TABLE_NAME_ALL_TEAMS
from tools.utils import df_to_sqlite_table
from tools.utils import sqlite_table_to_df

import logging
import numpy as np
import pandas as pd
import sqlite3


log = logging.getLogger(__name__)


class TeamsUnique:
    def __init__(self):
        self._df_unique_team_country = None

    def get_unique_team_country(self):
        """
        self.get_unique_teams() returns df with columns 'team' + 'country'
        :return:
        """
        df_all_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_GAMES)

        mask = (df_all_games["game_type"] == "cup") & (df_all_games["country"] == "eur")
        df_cup_eur = df_all_games[mask]
        df_cup_eur = self.__get_unique_team_country(df_cup_eur)

        mask = (df_all_games["game_type"] != "cup") & (df_all_games["country"] != "eur")
        df_cup_non_eur = df_all_games[mask]
        df_cup_non_eur = self.__get_unique_team_country(df_cup_non_eur)

        mask = df_all_games["game_type"] == "league"
        df_league = df_all_games[mask]
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

    def check_uniqueness(self, df_unique_team_country):
        len_orig = len(df_unique_team_country)
        len_unique = len(df_unique_team_country["team"].unique())
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

    def find_matching_teamnames(self, df_unique_team_country):
        teams = df_unique_team_country["team"].to_list()
        processed_teams = self.get_processed_teams(teams)

        # matching_teams = self.find_matching_teams(processed_teams)
        matching_teams = [
            ("ZSV", "AZSV"),
            ("FC Lisse", "Ulisses FC"),
            ("Lorca FC", "FC Flora"),
            ("Parma AC", "Parma FC"),
            ("CA Bastia", "SC Bastia"),
            ("Feyenoord", "Feyenoord AV"),
            ("Piense SC", "SC Praiense"),
            ("York City", "Cork City"),
            ("FK Astana", "FK Astana-64"),
            ("AC Venezia", "Venezia FC"),
            ("AC Venezia", "SSC Venezia"),
            ("AZ Alkmaar", "AZ Alkmaar (J)"),
            ("Burnley FC", "Barnsley FC"),
            ("CD Ourense", "GD Sourense"),
            ("FC Jumilla", "Jumilla CF"),
            ("FC Messina", "ACR Messina"),
            ("FC Treviso", "Treviso FBC"),
            ("Granada CF", "Granada 74 CF"),
            ("Hertha BSC", "Hertha BSC II"),
            ("Quick Boys", "Quick Boys II"),
            ("Venezia FC", "SSC Venezia"),
            ("VfL Bochum", "VfL Bochum II"),
            ("Wrexham FC", "Wrexham AFC"),
            ("Randers FC", "Rangers FC"),
            ("CF Palencia", "Valencia CF"),
            ("FC St. Pauli", "FC St. Pauli II"),
            ("GFC Ajaccio", "GFCO Ajaccio"),
            ("Hannover 96", "Hannover 96 II"),
            ("Lusitano GC", "Lusitano FCV"),
            ("SC Freiburg", "SC Freiburg II"),
            ("TeBe Berlin", "TeBe Berlin II"),
            ("1. FC K\\xf6ln", "1. FC K\\xf6ln II"),
            ("CD Alcal\\xe1", "RSD Alcal\\xe1"),
            ("FC Barcelona", "FC Barcelona B"),
            ("RKC Waalwijk", "RKC Waalwijk (J)"),
            ("Wimbledon FC", "AFC Wimbledon"),
            ("Hibernian FC", "Hibernians FC"),
            ("CSV Apeldoorn", "WSV Apeldoorn"),
            ("Calcio Chieri", "Chieti Calcio"),
            ("De Graafschap", "De Graafschap (J)"),
            ("FC Schalke 04", "FC Schalke 04 II"),
            ("HVV Hollandia", "HVV Hollandia II"),
            ("Hansa Rostock", "Hansa Rostock II"),
            ("Hereford Town", "Hednesford Town"),
            ("Spezia Calcio", "Pomezia Calcio"),
            ("VfB Stuttgart", "VfB Stuttgart II"),
            ("VfL Wolfsburg", "VfL Wolfsburg II"),
            ("Villarreal CF", "Villarreal CF B"),
            ("Werder Bremen", "Werder Bremen II"),
            ("1. FSV Mainz 05", "1. FSV Mainz 05 II"),
            ("Chesham United", "Evesham United"),
            ("Cosenza Calcio", "Vicenza Calcio"),
            ("Ford United FC", "AFC Telford United"),
            ("Perugia Calcio", "AC Perugia Calcio"),
            ("Vicenza Calcio", "Piacenza Calcio"),
            ("HB T\\xf3rshavn", "B36 T\\xf3rshavn"),
            ("Athletic Bilbao", "Athletic Bilbao B"),
            ("CD San Fernando", "San Fernando CD"),
            ("Energie Cottbus", "Energie Cottbus II"),
            ("Jahn Regensburg", "Jahn Regensburg II"),
            ("KR Reykjav\\xedk", "Fram Reykjav\\xedk"),
            ("KR Reykjav\\xedk", "Fylkir Reykjav\\xedk"),
            ("Lausanne Sports", "FC Lausanne-Sport"),
            ("Alemannia Aachen", "Alemannia Aachen II"),
            ("Bayer Leverkusen", "Bayer Leverkusen II"),
            ("Bayern M\\xfcnchen", "Bayern M\\xfcnchen II"),
            ("Manchester United", "United of Manchester"),
            ("SD Logro\\xf1\\xe9s", "UD Logro\\xf1\\xe9s"),
            ("Rot-Wei\\xdf Erfurt", "Rot-Wei\\xdf Erfurt II"),
            ("Uni\\xe3o Torreense", "Uni\\xe3o Torcatense"),
            ("1. FC Saarbr\\xfccken", "1. FC Saarbr\\xfccken II"),
            ("Schwarz-Wei\\xdf Essen", "Schwarz-Wei\\xdf Rehden"),
            ("CS Universitatea Craiova", "FC Universitatea Craiova"),
        ]
        return matching_teams

    def add_id_column(self, df_unique_team_country):
        # add an 'id' column
        df_unique_team_country = df_unique_team_country.reset_index(drop=True)
        df_unique_team_country["id"] = df_unique_team_country.index
        return df_unique_team_country

    def save_to_sqlite(self, df_unique_team_country):
        # Opens file if exists, else creates file
        log.debug(f"connect to {SQLITE_FULL_PATH}")
        connex = sqlite3.connect(SQLITE_FULL_PATH)
        df_unique_team_country.to_sql(
            name=TABLE_NAME_ALL_TEAMS, con=connex, if_exists="replace", index=False
        )
        connex.close()

    def run(self):
        # 1
        df_unique_team_country = self.get_unique_team_country()
        # 2
        self.check_uniqueness(df_unique_team_country)
        # 3 TODO: do something with matching_teamnames ??
        matching_teamnames = self.find_matching_teamnames(df_unique_team_country)
        # 4
        df_unique_team_country = self.add_id_column(df_unique_team_country)
        # 5
        self.save_to_sqlite(df_unique_team_country)


class UpdateGamesWithIds:
    def __init__(self):
        pass

    def all_games_with_team_ids(self, df_all_games, df_all_teams):
        # home merge team id (2 new columns are added ('team' and 'id)
        df_home = df_all_games.merge(
            df_all_teams[["team", "id"]], left_on="home", right_on="team"
        )
        df_home.drop(columns=["team"], inplace=True)
        df_home.rename(index=str, columns={"id": "home_id"}, inplace=True)

        # away merge team id (2 new columns are added ('team' and 'id)
        df_home_away = df_home.merge(
            df_all_teams[["team", "id"]], left_on="away", right_on="team"
        )
        df_home_away.drop(columns=["team"], inplace=True)
        df_home_away.rename(index=str, columns={"id": "away_id"}, inplace=True)

        # do some checks
        orig_count_unique = df_all_teams["team"].nunique()
        new_count_unique = pd.unique(
            df_home_away[["home_id", "away_id"]].values.ravel("K")
        ).size
        assert new_count_unique == orig_count_unique
        assert not df_home_away["home_id"].hasnans
        assert not df_home_away["away_id"].hasnans

        return df_home_away

    def all_games_with_game_ids(self, df):
        """ add columns with ints, we do not drop any columns """
        assert "game_name" in df.columns
        assert "game_type" in df.columns
        assert "country" in df.columns
        df["game_name_id"] = df["game_name"].replace(ALL_GAMENAME_ID_MAPPING)
        df["game_type_id"] = df["game_type"].replace(GAMETYPE_ID_MAPPING)
        df["game_country_id"] = df["country"].replace(COUNTRY_ID_MAPPING)
        return df

    def convert_dtypes_to_int(self, df):
        """method does two things:
        1.  Convert dyptes to int for columns we want """
        existing_int_columns = df.select_dtypes(include=["int"]).columns
        expected_int_columns = pd.Index(
            [
                "home_goals",
                "away_goals",
                "play_round",
                "season",
                "home_id",
                "away_id",
                "game_name_id",
                "game_type_id",
                "game_country_id",
            ]
        )

        not_yet_int_col = expected_int_columns.difference(existing_int_columns)
        if not_yet_int_col.size > 0:
            for col in not_yet_int_col:
                # we can only convert to int if column not hasnans (else float)
                if not df[col].hasnans:
                    df[col] = df[col].astype(int)
        return df

    def compress_df(self, df):
        """Downcast integer and float dyptes to smallest (aim is to compress sqlite)
        Downcast from int64 to uint8 (if possible) and from float64 to float32 """
        search_types = ["integer", "float"]
        for search_type in search_types:
            existing_type_columns = df.select_dtypes(include=[search_type]).columns
            for col in existing_type_columns:
                df[col] = pd.to_numeric(df[col], downcast=search_type)
        return df

    def replace_table_all_games(self, df_games_with_team_game_id):
        # Opens file if exists, else creates file
        log.debug(f"connect to {SQLITE_FULL_PATH}")
        connex = sqlite3.connect(SQLITE_FULL_PATH)
        df_games_with_team_game_id.to_sql(
            name=TABLE_NAME_ALL_GAMES_WITH_IDS,
            con=connex,
            if_exists="replace",
            index=False,
        )
        connex.close()

    def run(self):
        """ we update table with home team id, away team id, """
        df_all_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_GAMES)
        df_all_teams = sqlite_table_to_df(table_name=TABLE_NAME_ALL_TEAMS)
        df_games_team_id = self.all_games_with_team_ids(df_all_games, df_all_teams)
        df_games_team_game_id = self.all_games_with_game_ids(df_games_team_id)
        df_convert = self.convert_dtypes_to_int(df_games_team_game_id)
        df_compress = self.compress_df(df_convert)
        df_to_sqlite_table(
            df_compress, table_name=TABLE_NAME_ALL_GAMES_WITH_IDS, if_exists="replace"
        )
