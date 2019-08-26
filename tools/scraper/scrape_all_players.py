import pandas as pd
import time
import string
from tools.utils import string_to_unicode, is_panda_df_empty, df_to_sqlite_table
import os
from tools.constants import TAB_DELIMETER
import logging

log = logging.getLogger(__name__)

nr_secondes_sleep = 1

SQLITE_TABLE_NAMES_UNICODE = "unicode_names"


class SheetFixer:
    def __init__(self, csv_type, full_path):
        self.csv_type = csv_type
        self.csv_file_full_path = full_path
        self._dataframe = None
        self._expected_csv_columns = None
        self._existing_csv_columns = None

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

    def run(self):
        # pd.DataFrame().reindex_like(df1)
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
        df['home_manager'] = df['home_manager'].astype(str)
        df['away_manager'] = df['away_manager'].astype(str)
        df['home_sheet'] = df['home_sheet'].astype(str)
        df['away_sheet'] = df['away_sheet'].astype(str)

        # for url = 'http://www.worldfootball.net/report/champions-league-qual-2013-2014-1-runde-eb-streymur-fc-lusitanos-la-posa/'
        # scraper raise AssertionError(f'{self.url} found more >2 sheets..."')


        for index, row in self.dataframe.iterrows():
            url = row["url"]
            scraper = ManagerSheetScraper(url)
            df["date"][index] = row["date"]
            df["home"][index] = row["home"]
            df["away"][index] = row["away"]
            df["url"][index] = row["url"]
            df["game_type"][index] = row["game_type"]
            df["game_name"][index] = row["game_name"]
            df["country"][index] = row["country"]
            df["source_file"][index] = row["source_file"]
            df["home_manager"][index] = scraper.home_manager
            df["away_manager"][index] = scraper.away_manager
            df["home_sheet"][index] = scraper.home_sheet
            df["away_sheet"][index] = scraper.away_sheet
        df_to_sqlite_table(
            df, table_name=SQLITE_TABLE_NAMES_UNICODE, if_exists="append"
        )


class ManagerSheetScraper:
    def __init__(self, url):
        log.info(f'start scraping for url: {url}')
        self.url = url
        self._tables = None
        self._home_manager = None
        self._away_manager = None
        self._home_sheet = None
        self._away_sheet = None

    @property
    def tables(self):
        if not self._tables:
            self._tables = pd.read_html(self.url)
            time.sleep(nr_secondes_sleep)
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
        assert number_of_manager_tbls == 1
        return manager_table_id

    def __get_managers(self):
        manager_table_id = self.__get_managers_tbl_id()
        manager_tbl = self.tables[manager_table_id]
        manager_home = None
        manager_away = None
        for clm in manager_tbl:
            if "Manager:" in clm:
                if manager_home and manager_away:
                    raise AssertionError(f'{self.url} found more >2 "Managers:"')
                full_name = clm.rsplit(sep=":")[1].strip()
                assert full_name
                if not manager_home:
                    manager_home = string_to_unicode(full_name)
                elif not manager_away:
                    manager_away = string_to_unicode(full_name)
        self._home_manager = manager_home
        self._away_manager = manager_away

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
                            value
                            for value in values
                            if "Penalty" in str(value) or "penalty" in str(value)
                        ]
                        if it_is_penalties:
                            continue
                        if found_home and found_away:
                            raise AssertionError(f'{self.url} found more >2 sheets..."')
                        if not found_home:
                            found_home = True
                            sheet_locations["home_tbl"] = tbl_id
                            sheet_locations["home_col"] = col_id
                        elif not found_away:
                            found_away = True
                            sheet_locations["away_tbl"] = tbl_id
                            sheet_locations["away_col"] = col_id
        return sheet_locations

    def __get_clean_sheet(self, list_with_subs):
        """1. Clean list from digits at end of players name, e.g ""Agius  13'". He
        scored in 13th minute..
        2. Only select starter, so loop up till 'Substitutes'
        3. Strip player string
        4. append player string to list
        ['Andrew Hogg', 'Rui', "Andrei Agius  13'", 'Jackson', 'Rodolfo Soares', 'Bjorn Kristensen', 'Johan Bezzina', 'Marco Sahanek', 'Marcelo Dias', "Clayton Failla  7'", 'Jorghino', 'Substitutes',
        :return: list with strings
        """
        clean_list = []
        for player in list_with_subs:
            stripped_player = player.strip()
            if stripped_player.lower() in ["substitutes", "substitute"]:
                break
            if stripped_player[-1] == "'" and stripped_player[-2] in string.digits:
                # remove numbers at the end of string (with a space before! <- unicode)
                # "Andrei Agius  13'", "Clayton Failla  7'"
                player_minus_last_char = stripped_player[:-1]  # last char is "'"
                cleaned_player = player_minus_last_char.rstrip(string.digits)
                stripped_player = cleaned_player.strip()
            stripped_player_u = string_to_unicode(stripped_player)
            clean_list.append(stripped_player_u)
        return clean_list

    def __get_sheets(self):
        """ we expect:
        - 2 seperate tables: 1 with home_sheet info, 1 with away_sheet_info
        - sheets table column has dtype 'int64')
        - lets try to find a table with a lot of sting characters (sometimes with some ints)
        """
        sheet_tbl_ids = self.__get_sheet_tbl_ids()
        home_list_with_subs = self.tables[sheet_tbl_ids["home_tbl"]][
            sheet_tbl_ids["home_col"]
        ].to_list()
        away_list_with_subs = self.tables[sheet_tbl_ids["away_tbl"]][
            sheet_tbl_ids["away_col"]
        ].to_list()
        self._home_sheet = self.__get_clean_sheet(home_list_with_subs)
        self._away_sheet = self.__get_clean_sheet(away_list_with_subs)
