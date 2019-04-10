from tools.constants import RAW_CSV_DIRS
from tools.logging import log

import pandas as pd
import os


class RawCsvInfo:
    def __init__(self):
        self.raw_cup_dir = RAW_CSV_DIRS.get("cup")
        self.raw_league_dir = RAW_CSV_DIRS.get("league")
        self.raw_player_dir = RAW_CSV_DIRS.get("player")
        self._csv_info = []  # list with tuples
        self.check_constants()

    def check_constants(self):
        assert os.path.isdir(self.raw_cup_dir)
        assert os.path.isdir(self.raw_league_dir)
        assert os.path.isdir(self.raw_player_dir)

    @staticmethod
    def get_csvs_from_dir(folder_dir):
        """ get all .csvs from a directory
        :param folder_dir: string of dir's full path
        :return: csv_paths (list with strings)"""
        csv_paths = [
            os.path.join(folder_dir, file)
            for file in os.listdir(folder_dir)
            if file.endswith(".csv") and "_valid" not in file and "_invalid" not in file
        ]
        return csv_paths

    def update_csv_info(self):
        for csv_full_filepath in self.get_csvs_from_dir(self.raw_cup_dir):
            self._csv_info.append(("cup", csv_full_filepath))
        for csv_full_filepath in self.get_csvs_from_dir(self.raw_league_dir):
            self._csv_info.append(("league", csv_full_filepath))
        for csv_full_filepath in self.get_csvs_from_dir(self.raw_player_dir):
            self._csv_info.append(("player", csv_full_filepath))

    @property
    def csv_info(self):
        if self._csv_info:
            return self._csv_info
        self.update_csv_info()
        return self._csv_info


class CheckResults:
    def __init__(self):
        """invalid_dct = {  row_idx_1: ['msg_column_a'],
                            row_idx_2: ['msg_column_a', 'msg_column_c], } """
        self._invalid_dct = {}

    def add_invalid(self, row_idx, msg):
        assert isinstance(row_idx, int)
        assert isinstance(msg, str)
        assert len(msg) != 0
        log.info("add invalid row to check results")
        if row_idx in self._invalid_dct.keys():
            # append a string to the list of existing check row
            self._invalid_dct[row_idx].append(msg)
        else:
            self._invalid_dct.update({row_idx: [msg]})

    def _get_invalid_row_idx_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_row_idx_list

    def _get_invalid_msg_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_msg_list

    def get_valid_df(self, stripped_df, clm_desired_dtype_dict):
        invalid_row_idx_list = self._get_invalid_row_idx_list()
        # get inverse row index of dataframe
        valid_row_idx_list = list(
            set(stripped_df.index.to_list()) - set(invalid_row_idx_list)
        )
        return stripped_df.iloc[valid_row_idx_list].astype(clm_desired_dtype_dict)

    def get_invalid_df(self, df_orig):
        invalid_row_idx_list = self._get_invalid_row_idx_list()
        invalid_msg_list = self._get_invalid_msg_list()
        # update df_copy with msg for all invalid rows
        df_orig["msg"] = pd.DataFrame(
            {"msg": invalid_msg_list}, index=invalid_row_idx_list
        )
        return df_orig.iloc[invalid_row_idx_list]

    @property
    def count_invalid_rows(self):
        return len(self._get_invalid_row_idx_list())


class RowChecker:
    def __init__(self, clm_desired_dtype_dict, df_nr_rows):
        self.check_results = CheckResults()
        self.clm_desired_dtype_dict = clm_desired_dtype_dict
        self.df_nr_rows = df_nr_rows

        # constants
        self.min_len_teamname = 2
        self.max_len_teamname = 20
        self.date_format = "%d/%m/%Y"
        # TODO: min_date and max_date should depend on season
        self.min_date = pd.to_datetime("31/12/1999", format=self.date_format)
        self.max_date = pd.to_datetime("31/12/2019", format=self.date_format)

    def do_all_checks(self, row_idx, df_row):
        s_row_idx = str(row_idx)
        log.info(f"check row {s_row_idx}/{self.df_nr_rows}")
        if not self.can_convert_dtypes(row_idx, df_row):
            return
        # if check_date is not okay, then still successive checks need to be done on
        # same row
        df_row_convert = df_row.astype(self.clm_desired_dtype_dict)
        self.check_date(row_idx, df_row)
        self.check_home_nr_chars(row_idx, df_row_convert)
        self.check_away_nr_chars(row_idx, df_row_convert)
        self.check_score_format(row_idx, df_row_convert)

    def can_convert_dtypes(self, row_idx, df_row):
        """Can all columns be converted to desired datatype?"""
        try:
            df_row.astype(self.clm_desired_dtype_dict)
            return True
        except Exception as e:
            s = str(e).replace(",", "")
            msg = "could not convert row" + s
            self.check_results.add_invalid(row_idx, msg)
            return False

    def check_date(self, row_idx, df_row_convert):
        column_name = "date"
        date = pd.to_datetime(df_row_convert[column_name], format=self.date_format)
        okay = (date > self.min_date).bool() & (date < self.max_date).bool()
        if okay:
            return
        msg = column_name + " not in logic range"
        self.check_results.add_invalid(row_idx, msg)

    def check_home_nr_chars(self, row_idx, df_row_convert):
        column_name = "home"
        okay = (
            (df_row_convert[column_name].str.len() > self.min_len_teamname).bool()
            & (df_row_convert[column_name].str.len() < self.max_len_teamname).bool()
        )
        if okay:
            return
        msg = "home too little/much chars: " + df_row_convert[column_name].values[0]
        self.check_results.add_invalid(row_idx, msg)

    def check_away_nr_chars(self, row_idx, df_row_convert):
        column_name = "away"
        okay = (
            (df_row_convert[column_name].str.len() > self.min_len_teamname).bool()
            & (df_row_convert[column_name].str.len() < self.max_len_teamname).bool()
        )
        if okay:
            return
        msg = "away too little/much chars: " + df_row_convert[column_name].values[0]
        self.check_results.add_invalid(row_idx, msg)

    def check_score_format(self, row_idx, df_row_convert):
        """ - this string must be 3 chars long e.g "3:5"
            - field must contain 1 digit int, a colon (:), and 1 digit int"""
        column_name = "score"
        my_string = df_row_convert[column_name].values[0]
        okay = my_string[0].isdigit() and my_string[1] == ":" and my_string[2].isdigit()
        if okay:
            return
        msg = "not correct score format: " + df_row_convert[column_name].values[0]
        self.check_results.add_invalid(row_idx, msg)
