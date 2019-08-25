import pandas as pd
import time
import numpy as np
import string

nr_secondes_sleep = 3


class CupScraper:
    def __init__(self, url):
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

    def get_managers_tbl_id(self):
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
        manager_table_id = self.get_managers_tbl_id()
        manager_tbl = self.tables[manager_table_id]
        manager_home = None
        manager_away = None
        for clm in manager_tbl:
            if "Manager:" in clm:
                if manager_home and manager_away:
                    raise AssertionError(f'{self.url} found more >2 "Managers:"')
                full_name = clm.rsplit(sep=':')[1].strip()
                if not full_name:
                    full_name = np.NaN
                if not manager_home:
                    manager_home = full_name
                elif not manager_away:
                    manager_away = full_name
        self._home_manager = manager_home
        self._away_manager = manager_away

    def get_sheet_tbl_ids(self):
        nan_list = ['NaN', 'nan', 'Nan', 'NAN']
        found_home = False
        found_away = False
        sheet_locations = {'home_tbl': None,
                        'home_col': None,
                        'away_tbl': None,
                        'away_col': None,
                        }
        tbl_with_int64_columns = [tbl_id for tbl_id, tbl in enumerate(self.tables) if tbl.columns.dtype == 'int64']
        for tbl_id in tbl_with_int64_columns:
            tbl = self.tables[tbl_id]
            for col_id, col in enumerate(tbl.columns):
                nr_rows = len(tbl[col_id])
                value_dtype = tbl[col_id].values.dtype
                if nr_rows > 6 and value_dtype == 'O':
                    values = tbl[col_id].values
                    nr_chars = sum(len(str(i)) for i in values if str(i) not in nan_list)
                    if nr_chars > 80:
                        if found_home and found_away:
                            raise AssertionError(
                                f'{self.url} found more >2 sheets..."')
                        if not found_home:
                            found_home = True
                            sheet_locations['home_tbl'] = tbl_id
                            sheet_locations['home_col'] = col_id
                        elif not found_away:
                            found_away = True
                            sheet_locations['away_tbl'] = tbl_id
                            sheet_locations['away_col'] = col_id
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
            if stripped_player.lower() in ['substitutes', 'substitute']:
                break
            if stripped_player[-1] == "'" and stripped_player[-2] in string.digits:
                # remove numbers at the end of string (with a space before! <- unicode)
                # "Andrei Agius  13'", "Clayton Failla  7'"
                player_minus_last_char = stripped_player[:-1]  # last char is "'"
                cleaned_player = player_minus_last_char.rstrip(string.digits)
                stripped_player = cleaned_player.strip()
            clean_list.append(stripped_player)
        return clean_list

    def __get_sheets(self):
        """ we expect:
        - 2 seperate tables: 1 with home_sheet info, 1 with away_sheet_info
        - sheets table column has dtype 'int64')
        - lets try to find a table with a lot of sting characters (sometimes with some ints)
        """
        sheet_tbl_ids = self.get_sheet_tbl_ids()
        home_list_with_subs = self.tables[sheet_tbl_ids['home_tbl']][sheet_tbl_ids['home_col']].to_list()
        away_list_with_subs = self.tables[sheet_tbl_ids['away_tbl']][
            sheet_tbl_ids['away_col']].to_list()
        self._home_sheet = self.__get_clean_sheet(home_list_with_subs)
        self._away_sheet = self.__get_clean_sheet(away_list_with_subs)
