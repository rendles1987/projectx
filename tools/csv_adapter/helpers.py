from tools.constants import RAW_CSV_DIRS, COUNTRY_CUP_NAMES, COUNTRY_LEAGUE_NAMES
from tools.logging import log
import pandas as pd
import os


class RawCsvInfo:
    """ This class gets an overview of all raw csvs. Class is instanced in early
    stage of raw data check. Only public method is csv_info """

    def __init__(self):
        self._raw_cup_dir = RAW_CSV_DIRS.get("cup")
        self._raw_league_dir = RAW_CSV_DIRS.get("league")
        self._raw_player_dir = RAW_CSV_DIRS.get("player")
        self._csv_info = []  # list with tuples
        self.__check_constants()

    @property
    def csv_info(self):
        """
        :return: self._csv_info (list with tuples) e.g.: [
        ('cup', '/work/data/raw_data/cup/all_games_ned_knvb_beker.csv'),
        ('league', '/work/data/raw_data/league/ned_eredivisie-2016-2017-spieltag.csv')
        ]
        """
        if self._csv_info:
            return self._csv_info
        self.__update_csv_info()
        return self._csv_info

    def __check_constants(self):
        """ private method called in constructor """
        assert os.path.isdir(self._raw_cup_dir)
        assert os.path.isdir(self._raw_league_dir)
        assert os.path.isdir(self._raw_player_dir)

    def __update_file_name(self, csv_full_filepath):
        if "-" in csv_full_filepath:
            updated_path = csv_full_filepath.replace("-", "_")
            os.rename(csv_full_filepath, csv_full_filepath.replace("-", "_"))
            return updated_path
        return csv_full_filepath

    def __update_csv_info(self):
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(
            self._raw_cup_dir
        ):
            updated_csv_filename = self.__update_file_name(csv_full_filepath)
            self._csv_info.append(("cup", updated_csv_filename))
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(
            self._raw_league_dir
        ):
            updated_csv_filename = self.__update_file_name(csv_full_filepath)
            self._csv_info.append(("league", updated_csv_filename))
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(
            self._raw_player_dir
        ):
            updated_csv_filename = self.__update_file_name(csv_full_filepath)
            self._csv_info.append(("player", updated_csv_filename))

    @staticmethod
    def __get_not_checked_csvs_from_dir(folder_dir):
        """ get all .csvs - that have no 'valid' or 'invalid' in filename -
        from a directory
        :param folder_dir: string of dir's full path
        :return: csv_paths (list with strings)"""
        csv_paths = [
            os.path.join(folder_dir, file)
            for file in os.listdir(folder_dir)
            if file.endswith(".csv") and "_valid" not in file and "_invalid" not in file
        ]
        return csv_paths


class CheckResults:
    """ Class that stores check results from RowChecker. Each row in a csv is checked.
    If wrong? then CheckResults is updated. (Only wrong!!) check results are stored in
    a dict "self._invalid_dct". invalid_dct is e.g. {
        row_idx_1: ['msg_column_a'],
        row_idx_2: ['msg_column_a', 'msg_column_c],
    }
    """

    def __init__(self):
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
            # add new k,v to dict
            self._invalid_dct.update({row_idx: [msg]})

    def get_valid_df(self, stripped_df, clm_desired_dtype_dict):
        """get valid rows from original panda dataframe """
        # first get invalid row indices
        invalid_row_idx_list = self.__get_invalid_row_idx_list()
        # get inverse row indices of dataframe
        valid_row_idx_list = list(
            set(stripped_df.index.to_list()) - set(invalid_row_idx_list)
        )
        return stripped_df.iloc[valid_row_idx_list].astype(clm_desired_dtype_dict)

    def get_invalid_df(self, df_orig):
        """get invalid rows from original panda dataframe """
        invalid_row_idx_list = self.__get_invalid_row_idx_list()
        invalid_msg_list = self.__get_invalid_msg_list()
        # update df_copy with msg for all invalid rows
        df_orig["msg"] = pd.DataFrame(
            {"msg": invalid_msg_list}, index=invalid_row_idx_list
        )
        return df_orig.iloc[invalid_row_idx_list]

    @property
    def count_invalid_rows(self):
        return len(self.__get_invalid_row_idx_list())

    def __get_invalid_row_idx_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_row_idx_list

    def __get_invalid_msg_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_msg_list


class RowChecker:
    def __init__(self, clm_desired_dtype_dict, df_nr_rows, strip_clmns):
        self.clm_desired_dtype_dict = clm_desired_dtype_dict
        self.df_nr_rows = df_nr_rows
        self.strip_clmns = strip_clmns

        self.check_results = CheckResults()
        # TODO put these constants in constants.py
        self.min_len_teamname = 2
        self.max_len_teamname = 20
        self.date_format = "%d/%m/%Y"
        # TODO: min_date and max_date should depend on season
        self.min_date = pd.to_datetime("31/12/1999", format=self.date_format)
        self.max_date = pd.to_datetime("31/12/2019", format=self.date_format)

    def do_all_checks(self, row_idx, df_row):
        s_row_idx = str(row_idx)
        log.info(f"check row {s_row_idx}/{self.df_nr_rows}")

        # 1. convert
        if not self.can_convert_dtypes(row_idx, df_row):
            return
        df_row_convert = df_row.astype(self.clm_desired_dtype_dict)

        # 2. strip
        df_row_convert_strip = self.strip_columns_whitespace(df_row_convert)

        # 3. checks
        self.check_date(row_idx, df_row_convert_strip)
        self.check_home_nr_chars(row_idx, df_row_convert_strip)
        self.check_away_nr_chars(row_idx, df_row_convert_strip)
        self.check_score_format(row_idx, df_row_convert_strip)

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

    def strip_columns_whitespace(self, df):
        """strip some columns in df that have strip_whitespace=True and is_column_name=True
        :param df: panda dataframe
        :param strip_these_columns: list
        :return: updated panda dataframe (with stripped columns)
        """
        for clm in self.strip_clmns:
            try:
                df[clm] = df[clm].str.strip()
            except AttributeError as e:
                raise Exception("cannot strip this column" + str(e))
            """
            >>> my_string = '  aa bb  cc '
            >>> print(my_string.strip())
            >>> print(my_string.lstrip())
            >>> print(my_string.rstrip())
            'aa bb  cc'
            'aa bb  cc '
            '  aa bb  cc'
            """
        return df

    def check_date(self, row_idx, df_row_convert):
        column_name = "date"
        date = pd.to_datetime(df_row_convert[column_name], format=self.date_format)
        okay = (date > self.min_date).bool() & (date < self.max_date).bool()
        if okay:
            return
        msg = column_name + " not in logic range"
        self.check_results.add_invalid(row_idx, msg)

    def check_home_nr_chars(self, row_idx, df_row_convert):
        """ check if len(home teamname) > 2 and < 20 chars """
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
        """ check if len(away teamname) > 2 and < 20 chars """
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


class GameFilenameChecker:
    """ handles cup and league csv """

    def __init__(self, csv_file_full_path, csv_type):
        self.csv_file_full_path = csv_file_full_path
        self.csv_type = csv_type
        self._country = None
        self._game_name = None
        self._season = None

    @property
    def csv_file_name_with_extension(self):
        return self.csv_file_full_path.split("/")[-1]  # 'xx.csv'

    @property
    def csv_file_name_without_extension(self):
        return (self.csv_file_full_path.split("/")[-1]).split(".")[0]  # 'xx'

    @property
    def country(self):
        if self._country:
            return self._country
        self._country = self.csv_file_name_with_extension[0:3]
        self.__check_country()
        return self._country

    @property
    def game_name(self):
        if self._game_name:
            return self._game_name
        self._game_name = self.__get_game_name()
        return self._game_name

    @property
    def season(self):
        if self.csv_type == "league":
            if self._season:
                return self._season
            self._season = self.__get_season()
            return self._season
        else:
            raise AssertionError("only league has season in filename")

    def __check_country(self):
        # check if 4th char csvfilename is underscore
        assert self.csv_file_name_with_extension[3] == "_", (
            "4th char of csv file",
            self.csv_file_name_with_extension,
            "name is not underscore",
        )

        # check if country in whitelist
        if self.csv_type == "cup":
            assert self.country in COUNTRY_CUP_NAMES.keys(), (
                "country",
                self.country,
                "not in ",
                COUNTRY_CUP_NAMES.keys(),
            )
        elif self.csv_type == "league":
            assert self.country in COUNTRY_LEAGUE_NAMES.keys(), (
                "country",
                self.country,
                "not in ",
                COUNTRY_LEAGUE_NAMES.keys(),
            )

    def __get_game_name(self):
        country = self.country
        expected_names = None
        if self.csv_type == "cup":
            expected_names = COUNTRY_CUP_NAMES.get(country).values()
        elif self.csv_type == "league":
            expected_names = COUNTRY_LEAGUE_NAMES.get(country).values()
        for gamename in expected_names:
            if gamename in self.csv_file_name_without_extension:
                return gamename
        # if not found gamename
        raise Exception(
            "could not find gamename for " + self.csv_file_name_without_extension
        )

    def __get_season(self):
        """
        >>> str = "h3110 23 cat 444.4 rabbit 11 2 dog"
        >>> [int(s) for s in str.split() if s.isdigit()]
        [23, 11, 2]
        """
        numbers = [
            int(s)
            for s in self.csv_file_name_without_extension.split("_")
            if s.isdigit()
        ]
        season = min(numbers)
        assert 2000 <= season <= 2019
        return season
