import pandas as pd
import time
import string
from tools.utils import string_to_unicode, is_panda_df_empty, df_to_sqlite_table
import os
from tools.constants import TAB_DELIMETER, SQLITE_TABLE_NAMES_UNICODE
import logging
import numpy as np
from tools.utils import df_to_sqlite_table, sqlite_table_to_df


log = logging.getLogger(__name__)

nr_secondes_sleep = 1


class UnicodeScraperController:
    def __init__(self, csv_type, full_path):
        self.csv_type = csv_type
        self.csv_file_full_path = full_path
        self._dataframe = None
        self._expected_csv_columns = None
        self._existing_csv_columns = None
        self._df_existing_sqlite_rows = None

    @property
    def csv_file_dir(self):
        assert os.path.isfile(self.csv_file_full_path)
        assert os.path.isdir(os.path.dirname(self.csv_file_full_path))
        return os.path.dirname(self.csv_file_full_path)  # '/work/data/raw_data/league/

    @property
    def csv_file_name_with_extension(self):
        return self.csv_file_full_path.split("/")[-1]  # 'xx.csv'

    @property
    def csv_file_name_without_extension(self):
        return (self.csv_file_full_path.split("/")[-1]).split(".")[0]  # 'xx'

    @property
    def dataframe(self):
        if not is_panda_df_empty(self._dataframe):
            return self._dataframe
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep=TAB_DELIMETER)
        return self._dataframe

    @property
    def df_existing_sqlite_rows(self):
        if not is_panda_df_empty(self._df_existing_sqlite_rows):
            return self._df_existing_sqlite_rows
        self._df_existing_sqlite_rows = sqlite_table_to_df(
            table_name=SQLITE_TABLE_NAMES_UNICODE
        )
        return self._df_existing_sqlite_rows
        # if is_panda_df_empty(self._df_existing_sqlite_rows):
        #     return None
        # return self._df_existing_sqlite_rows

    def row_already_in_sqlite(self, row):
        """Check if row [home, away, date] already exists in the sqlite """
        df = self.df_existing_sqlite_rows
        return (
            (df.home == row.home) & (df.away == row.away) & (df.date == row.date)
        ).any()

    def create_empty_df(self):
        columns = [
            "date",
            "home",
            "away",
            "url",
            "game_type",
            "game_name",
            "country",
            "source_file",
            "home_manager",
            "away_manager",
            "home_sheet",
            "away_sheet",
        ]
        df = pd.DataFrame().reindex_like(self.dataframe[columns])
        # set these column to dtype str and empty them
        columns = [
            "home_manager",
            "away_manager",
            "home_sheet",
            "away_sheet",
            "start_time",
            "home_subs",
            "away_subs",
        ]
        for col in columns:
            df[col] = df["home_manager"].astype(str)
            df[col] = np.NaN
        return df

    def run(self):

        try:
            for index, row in self.dataframe.iterrows():
                if self.row_already_in_sqlite(row):
                    # log.info(f"this row is already in sqlite {row.url}")
                    continue
                df = self.create_empty_df()
                df["date"][index] = row["date"]
                df["home"][index] = row["home"]
                df["away"][index] = row["away"]
                df["url"][index] = row["url"]
                df["game_type"][index] = row["game_type"]
                df["game_name"][index] = row["game_name"]
                df["country"][index] = row["country"]
                df["source_file"][index] = row["source_file"]
                if str(row["url"]) == "nan":
                    df["home_manager"][index] = np.nan
                    df["away_manager"][index] = np.nan
                    df["home_sheet"][index] = np.nan
                    df["away_sheet"][index] = np.nan
                    df["home_subs"][index] = np.nan
                    df["away_subs"][index] = np.nan
                    df["start_time"][index] = np.nan
                else:
                    scraper = UnicodeScraper(row["url"])
                    if not scraper.can_read_html():
                        continue
                    df["home_manager"][index] = scraper.home_manager
                    df["away_manager"][index] = scraper.away_manager
                    df["home_sheet"][index] = scraper.home_sheet
                    df["away_sheet"][index] = scraper.away_sheet
                    df["home_subs"][index] = scraper.home_subs
                    df["away_subs"][index] = scraper.away_subs
                    df["start_time"][index] = scraper.start_time

                # Drop rows if all empty cells
                df.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)

                # add row by row instead of whole csv
                if len(df) > 0:
                    df_to_sqlite_table(
                        df, table_name=SQLITE_TABLE_NAMES_UNICODE, if_exists="append"
                    )
        except JumpToNextRow:
            log.error(f"skipped row {index} of {self.csv_file_full_path}")


class JumpToNextRow(Exception):
    pass


class UnicodeScraper:
    def __init__(self, url):
        log.info(f"start scraping for url: {url}")
        self.url = url
        self._tables = None
        self._home_manager = None
        self._away_manager = None
        self._home_sheet = None
        self._home_subs = None
        self._away_sheet = None
        self._away_subs = None
        self._start_time = None

    def can_read_html(self):
        try:
            self._tables = pd.read_html(self.url)
            time.sleep(nr_secondes_sleep)
            return True
        except Exception as e:
            return False

    @property
    def tables(self):
        if not self._tables:
            try:
                self._tables = pd.read_html(self.url)
                time.sleep(nr_secondes_sleep)
                try:
                    # wtf... sometimes it works a second time (same url)
                    time.sleep(nr_secondes_sleep)
                    self._tables = pd.read_html(self.url)
                    time.sleep(nr_secondes_sleep)
                except Exception as e:
                    log.error(f"could not read_html for 2nd time: {self.url}")
                    log.error(str(e))
                    raise JumpToNextRow
            except Exception as e:
                log.error(f"could not read_html for 1st time: {self.url}")
                log.error(str(e))
                raise JumpToNextRow
        return self._tables

    @property
    def home_manager(self):
        if not self._home_manager:
            self.__get_managers()
        return self._home_manager

    @property
    def away_manager(self):
        if not self._away_manager:
            self.__get_managers()
        return self._away_manager

    @property
    def home_sheet(self):
        if not self._home_sheet:
            self.__get_sheets()
        return self._home_sheet

    @property
    def away_sheet(self):
        if not self._away_sheet:
            self.__get_sheets()
        return self._away_sheet

    @property
    def home_subs(self):
        if not self._home_subs:
            self.__get_sheets()
        return self._home_subs

    @property
    def away_subs(self):
        if not self._away_subs:
            self.__get_sheets()
        return self._away_subs

    @property
    def start_time(self):
        if not self._start_time:
            self.__get_start_time()
        return self._start_time

    def __get_start_time(self):
        tbl_with_object_columns = [
            tbl_id
            for tbl_id, tbl in enumerate(self.tables)
            if tbl.columns.dtype == "object"
        ]
        time_found = False
        for tbl_id in tbl_with_object_columns:
            table = self.tables[tbl_id]
            for col in table.columns:
                if "clock" in col.lower():
                    time_found = True
                    self._start_time = col
        if not time_found:
            self._start_time = "unknown"

    @staticmethod
    def __managers_in_tbl(tbl):
        for col in tbl.columns:
            if "Manager:" in col:
                return True

    def __get_managers_tbl_id(self):
        """
        Find out which table contains managers.
        tbl with manager info is empty (managers are in column names!!)
            # Empty DataFrame
            # Columns: [Manager: Mark Miller, Manager: Aleksander Puštov]
            # Index: []
        :return: table id (int
                """

        # we expect only 1 number_of_manager_tbls
        number_of_manager_tbls = 0
        for tbl_id, tbl in enumerate(self.tables):
            if tbl.columns.dtype == "object":
                if self.__managers_in_tbl(tbl):
                    number_of_manager_tbls += 1
                    manager_table_id = tbl_id
        if number_of_manager_tbls != 1:
            log.error(f"{self.url} number_of_manager_tbls is not 1")
            raise JumpToNextRow
        return manager_table_id

    def __get_managers(self):
        manager_table_id = self.__get_managers_tbl_id()
        manager_tbl = self.tables[manager_table_id]
        # we put managers in list and return the str(list) so that we keep the
        # string bytes e.g. '[abr/ux2movic]' instead of 'abromovic'
        manager_home = []
        manager_away = []
        for clm in manager_tbl:
            if "Manager:" in clm:
                if manager_home and manager_away:
                    log.error(f'{self.url} found more >2 "Managers:"')
                    raise JumpToNextRow
                full_name = clm.rsplit(sep=":")[1].strip()
                if not full_name:
                    log.error(f"{self.url} full_name not found")
                    raise JumpToNextRow
                if not manager_home:
                    manager_home.append(string_to_unicode(full_name))
                elif not manager_away:
                    manager_away.append(string_to_unicode(full_name))
        self._home_manager = str(manager_home)
        self._away_manager = str(manager_away)

    def __get_sheet_tbl_ids(self):
        nan_list = ["NaN", "nan", "Nan", "NAN"]
        found_home = False
        found_away = False
        sheet_locations = {
            "home_tbl": None,
            "home_col": None,
            "away_tbl": None,
            "away_col": None,
        }

        tbl_with_int64_columns = [
            tbl_id
            for tbl_id, tbl in enumerate(self.tables)
            if tbl.columns.dtype == "int64"
        ]
        for tbl_id in tbl_with_int64_columns:
            tbl = self.tables[tbl_id]
            for col_id, col in enumerate(tbl.columns):
                nr_rows = len(tbl[col_id])
                value_dtype = tbl[col_id].values.dtype
                if nr_rows > 6 and value_dtype == "O":
                    values = tbl[col_id].values
                    nr_chars = sum(
                        len(str(i)) for i in values if str(i) not in nan_list
                    )
                    if nr_chars > 80:
                        it_is_penalties = [
                            value for value in values if "penalty" in str(value).lower()
                        ]
                        if it_is_penalties:
                            continue
                        it_is_goals_overview = [
                            value for value in values if "goals" in str(value).lower()
                        ]
                        if it_is_goals_overview:
                            continue
                        if found_home and found_away:
                            log.error(f'{self.url} found more >2 sheets..."')
                            raise JumpToNextRow
                        if not found_home:
                            found_home = True
                            sheet_locations["home_tbl"] = tbl_id
                            sheet_locations["home_col"] = col_id
                        elif not found_away:
                            found_away = True
                            sheet_locations["away_tbl"] = tbl_id
                            sheet_locations["away_col"] = col_id
        return sheet_locations

    def __get_improved_sheet(self, list_with_subs):
        """1. Clean list from digits at end of players name, e.g ""Agius  13'". He
        scored in 13th minute..
        2. Only select starter, so loop up till 'Substitutes'
        3. Strip player string
        4. append player string to list
        ['Andrew Hogg', 'Rui', "Andrei Agius  13'", 'Jackson', 'Rodolfo Soares', 'Bjorn Kristensen', 'Johan Bezzina', 'Marco Sahanek', 'Marcelo Dias', "Clayton Failla  7'", 'Jorghino', 'Substitutes',
        :return: list with strings
        """
        starters = []
        subs = []
        this_is_starter = True
        for player in list_with_subs:
            # https://www.worldfootball.net/report/segunda-division-2017-2018-fc-barcelona-b-gimnastic/
            # in between Perez en Varo str(player) is 'nan' ...
            if str(player) == 'nan':
                continue
            stripped_player = player.strip()
            if stripped_player.lower() in ["substitutes", "substitute"]:
                this_is_starter = False
                continue
            # remove possible numbers at the end of string (with a space before!
            # <- unicode) e.g. "Andrei Agius  13'", "Clayton Failla  7'"
            if stripped_player[-1] == "'" and stripped_player[-2] in string.digits:
                player_minus_last_char = stripped_player[:-1]  # last char is "'"
                cleaned_player = player_minus_last_char.rstrip(string.digits)
                stripped_player = cleaned_player.strip()
            assert string.digits not in stripped_player, "numbers still in player name"
            stripped_player_u = string_to_unicode(stripped_player)
            if this_is_starter:
                starters.append(stripped_player_u)
            else:
                subs.append(stripped_player_u)
        # we cannot add list to a cell in sqlite... So make it a string...
        starters = str(starters)
        subs = str(subs)
        return starters, subs

    def __get_sheets(self):
        """ we expect:
        - 2 seperate tables: 1 with home_sheet info, 1 with away_sheet_info
        - sheets table column has dtype 'int64')
        - lets try to find a table with a lot of sting characters (sometimes with some ints)
        """
        sheet_tbl_ids = self.__get_sheet_tbl_ids()

        if not sheet_tbl_ids.get("home_tbl") or not sheet_tbl_ids.get("home_col"):
            self._home_sheet, self._home_subs = np.nan, np.nan
            self._away_sheet, self._away_subs = np.nan, np.nan
            return

        try:
            home_list_with_subs = self.tables[sheet_tbl_ids["home_tbl"]][
                sheet_tbl_ids["home_col"]
            ].to_list()
        except Exception as e:
            home_list_with_subs = []
        try:
            away_list_with_subs = self.tables[sheet_tbl_ids["away_tbl"]][
                sheet_tbl_ids["away_col"]
            ].to_list()
        except Exception as e:
            away_list_with_subs = []

        if home_list_with_subs:
            self._home_sheet, self._home_subs = self.__get_improved_sheet(
                home_list_with_subs
            )
        else:
            self._home_sheet, self._home_subs = np.nan, np.nan
        if away_list_with_subs:
            self._away_sheet, self._away_subs = self.__get_improved_sheet(
                away_list_with_subs
            )
        else:
            self._away_sheet, self._away_subs = np.nan, np.nan
