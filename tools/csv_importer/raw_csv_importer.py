import os

import pandas as pd
from tools.constants import (
    BASE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    DTYPES,
    LEAGUE_GAME_PROPERTIES,
    PLAYER_PROPERTIES,
    TEMP_DIR,
)
from tools.csv_importer.check_result import CheckResults
from tools.logging import log
from tools.utils import df_to_csv, ensure_corect_date_format, is_panda_df_empty


def check_nan_fix_required(csv_path):
    delimiter_type = detect_delimeter_type(csv_path)  # '\t' or ','
    temp_df = pd.read_csv(csv_path, sep=delimiter_type)

    """ checks if all columns
    - if column contains False/True and other strings --> then nan_fix_required
    """

    # this only works for 'cup' csvs
    score_columns = [clm for clm in temp_df.columns if str(clm).startswith("score_")]
    for column in score_columns:
        nr_nan = 0
        nr_false = 0
        nr_true = 0
        contains_nan = temp_df[column].isnull().values.any()
        contains_false = (temp_df[column] == "False").any()
        contains_true = (temp_df[column] == "True").any()

        if not any((contains_nan, contains_false, contains_true)):
            continue

        keys = temp_df[column].value_counts(dropna=False).keys().to_list()
        values = temp_df[column].value_counts(dropna=False).to_list()

        nr_rows = len(temp_df)
        if contains_false:
            nr_false = values[keys.index("False")]
        if contains_true:
            nr_true = values[keys.index("True")]
        if contains_nan:
            nr_nan = nr_rows - temp_df.count()
        nr_bool = nr_false + nr_true

        if nr_rows - (nr_bool + nr_nan) > 0:
            # column includes a leftover (no boolean, no nan, but another string?
            # we need to fix this,
            return True
    return False


def detect_delimeter_type(csv_file_full_path):
    """" detects whether it is a .csv or .tsv """
    # is it a .tsv ?
    try:
        pd_text_file_reader = pd.read_csv(
            csv_file_full_path, sep="\t", skiprows=0, chunksize=1
        )
        row = pd_text_file_reader.get_chunk()
        row["date"]
        return "\t"
    except Exception:
        try:
            # then it should be a .tsv ?
            pd_text_file_reader = pd.read_csv(
                csv_file_full_path, sep=",", skiprows=0, chunksize=1
            )
            row = pd_text_file_reader.get_chunk()
            row["date"]
            return ","
        except Exception:
            raise AssertionError(csv_file_full_path + "not a .tsv/.csv ??")


def remove_tab_strings(csv_path):
    unwanted_strings = [r"\t", r"\r", r"\n", r"\\t", r"\\r", r"\\n"]
    new_df = pd.read_csv(csv_path, sep="\t")
    for unwanted in unwanted_strings:
        new_df.replace(unwanted, "", regex=True, inplace=True)
    os.remove(csv_path)
    df_to_csv(new_df, csv_path)


def fix_nan_values(csv_path):
    """
    # play_round    score_45    score_90    score_105   score_120   aet     pso             home_goals      away_goals      season          url         home_manager    away_manager        home_sheet      away_sheet
    # 1	            0:0	        False	    False	    2	        0	    -2017-2018/	    http....	    Hans van Arum	Dennis Dekkers	[xx, yy]    [xx, yy]        nan                 nan             nan



    # 1. copy eerst naar df_copy
    # 2. loop door df
    # 3. - als score_90 is False/True dan moet alles in (alle col vanaf score_90) 3 naar rechts
    #    - als score_105 is False/True dan moet alles (alle col vanaf score_105) 2 naar rechts
    #    - als score_105 is False/True dan moet alles (alle col vanaf score_120) 1 naar rechts

    """

    sequence_columns = (
        "score_90",
        "score_105",
        "score_120",
        "aet",
        "pso",
        "home_goals",
        "away_goals",
        "season",
        "url",
        "home_manager",
        "away_manager",
        "home_sheet",
        "away_sheet",
    )

    import numpy as np

    delimiter_type = detect_delimeter_type(csv_path)  # '\t' or ','

    for row_idx, row in enumerate(
        pd.read_csv(csv_path, sep=delimiter_type, skiprows=0, chunksize=1)
    ):

        log.info("fix_nan_values row_index: " + str(row_idx))

        shift_right = 0

        if row["score_90"].dtype.name == "bool":
            shift_right = 3
        elif row["score_105"].dtype.name == "bool":
            shift_right = 2
        elif row["score_120"].dtype.name == "bool":
            shift_right = 1

        row_copy = row.copy()

        if shift_right == 3:
            update_these_columns_first = list(sequence_columns)[3:]
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[:-3]
            # ['score_90', 'score_105', 'score_120', 'aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_90"] = np.NaN
            row_copy["score_105"] = np.NaN
            row_copy["score_120"] = np.NaN
        elif shift_right == 2:
            update_these_columns_first = list(sequence_columns)[3:]
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[1:-2]
            # ['score_105', 'score_120', 'aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager', 'away_manager']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_105"] = np.NaN
            row_copy["score_120"] = np.NaN
        elif shift_right == 1:
            update_these_columns_first = list(sequence_columns)[3:]
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[2:-1]
            # ['score_120', 'aet', 'pso', 'home_goals', 'away_goals', 'season', 'url', 'home_manager', 'away_manager', 'home_sheet']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_120"] = np.NaN

        if row_idx == 0:
            new_df = row_copy
        else:
            new_df = pd.concat([new_df, row_copy], ignore_index=True)

    # remove columns with "unnamed" in header
    unnamed_columns = [clm for clm in new_df.columns if "unnamed" in clm.lower()]
    new_df.drop(unnamed_columns, axis=1, inplace=True)
    os.remove(csv_path)
    df_to_csv(new_df, csv_path)


def check_properties(props, orig_df_columns, csv_full_path):
    strip_clms = [prop.name for prop in props if prop.strip_whitespace]
    expected_clms = [prop.name for prop in props if prop.is_column_name]
    string_fields = [prop.name for prop in props if prop.desired_type == "string"]
    not_string_fields = [prop.name for prop in props if prop.desired_type != "string"]

    # all to be stripped columns must be in expected columns'
    for clm in strip_clms:
        if clm not in expected_clms:
            raise AssertionError(clm + " is not a column, cannot be stripped")
    # check 2 strip_these_fields must be in string_fields
    for clm in strip_clms:
        if clm not in string_fields:
            raise AssertionError(clm + " not in string_columns")
    # check 3 strip_these_fields can not be in not_string_fields
    for clm in strip_clms:
        if clm in not_string_fields:
            raise AssertionError(clm + " can not be in not_string_columns")
    # check 4 expected columns must be in existing columns
    existing_clms = orig_df_columns
    for expected_clm in expected_clms:
        if expected_clm not in existing_clms:
            raise AssertionError(
                csv_full_path + " " + expected_clm + " not in existing csv columns"
            )


def delete_tmp_csv(tmp_csv_path):
    # remove if file exists
    if os.path.isfile(tmp_csv_path):
        os.remove(tmp_csv_path)
    else:
        log.warning("want to remove " + tmp_csv_path + " but it does not exists")


class BaseCsvImporter:
    """each game csv goes trough a child class of BaseCsvImporter.
    Class contains a lot of properties that must be retrieved from game csv
    Two game csv exists: cup, league """

    def __init__(self, csvfilepath):
        self.csv_type = None
        self.csv_file_full_path = csvfilepath  # '/work/data/raw_data/league/xx.csv'
        self.properties = BASE_GAME_PROPERTIES
        self._dataframe = None
        self._expected_csv_columns = None
        self._existing_csv_columns = None
        self._strip_clmns = None
        self._clm_desired_dtype_dct = None
        self.df = None

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
        delimiter_type = detect_delimeter_type(self.csv_file_full_path)  # '\t' or ','
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep=delimiter_type)
        return self._dataframe

    @property
    def existing_csv_columns(self):
        if self._existing_csv_columns:
            return self._existing_csv_columns
        columns = self.dataframe.columns.to_list()
        self._existing_csv_columns = columns
        return self._existing_csv_columns

    @property
    def expected_csv_columns(self):
        if self._expected_csv_columns:
            return self._expected_csv_columns
        self._expected_csv_columns = [
            prop.name for prop in self.properties if prop.is_column_name
        ]
        return self._expected_csv_columns

    @property
    def strip_clmns(self):
        if self._strip_clmns:
            return self._strip_clmns
        self._strip_clmns = [
            prop.name for prop in self.properties if prop.strip_whitespace
        ]
        return self._strip_clmns

    @property
    def clm_desired_dtype_dict(self):
        if self._clm_desired_dtype_dct:
            return self._clm_desired_dtype_dct
        map_column_to_desired_type = {
            prop.name: prop.desired_type
            for prop in self.properties
            if prop.is_column_name
        }  # {'date': 'date', 'home': 'string', 'home_goals': 'int', ......}
        map_type_to_panda_type = {type.name: type.panda_type for type in DTYPES}
        # {'string': 'object', 'int': 'int64', 'date': 'datetime64', ....}
        clm_desired_dtype_dict = {
            k: map_type_to_panda_type[v] for k, v in map_column_to_desired_type.items()
        }  # {'date': 'datetime64', 'home': 'object', 'home_goals': 'int64', ...}
        self._clm_desired_dtype_dct = clm_desired_dtype_dict
        return self._clm_desired_dtype_dct

    @staticmethod
    def can_convert_dtypes_table(df, clm_desired_dtype_dict):
        """Can all columns whole table be converted to desired datatype?"""
        try:
            df.astype(clm_desired_dtype_dict)
            return True
        except:
            return False

    @staticmethod
    def can_convert_dtypes_one_row(df_row, clm_desired_dtype_dict):
        """Can all columns on row be converted to desired datatype?"""
        try:
            df_row.astype(clm_desired_dtype_dict)
            return True, ""
        except Exception as e:
            s = str(e).replace(",", "")
            msg = "could not convert row" + s
            return False, msg

    @staticmethod
    def do_strip_columns(df):
        """ strip all object columns of a df row or whole df table """
        df_obj = df.select_dtypes(["object"])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        return df

    def create_valid_invalid_df(self, df_selection, check_results):
        df_nr_rows = df_selection.shape[0]
        count_invalid_rows = check_results.count_invalid_rows
        count_valid_rows = df_nr_rows - count_invalid_rows

        if count_invalid_rows > 0:
            log.info(f"create invalid df with {count_invalid_rows} rows")
            invalid_df = check_results.get_invalid_df(self.dataframe)
            file_name = self.csv_file_name_without_extension + "_invalid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            # save it
            df_to_csv(invalid_df, full_path)

        if count_valid_rows > 0:
            log.info(f"create valid (and converted) df with {count_valid_rows} rows")
            valid_df = check_results.get_valid_df(
                df_selection, self.clm_desired_dtype_dict
            )
            log.info(f"save valid df")
            file_name = self.csv_file_name_without_extension + "_valid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            # save it
            df_to_csv(valid_df, full_path)

    def import_whole_table_at_once(self, df_selection):
        """ import whole table at once (convert, strip and save) """
        log.info(f"++ convert {self.csv_file_name_without_extension} in once")
        df_convert = df_selection.astype(self.clm_desired_dtype_dict)
        df_convert = ensure_corect_date_format(df_convert)
        df_convert_stripped = self.do_strip_columns(df_convert)
        file_name = self.csv_file_name_without_extension + "_valid.csv"
        full_path = os.path.join(self.csv_file_dir, file_name)
        df_to_csv(df_convert_stripped, full_path)

    def import_row_by_row(self, df_selection):
        """ convert row by row and get wrong rows into seperate df """
        log.info(f"-- convert row by row {self.csv_file_name_without_extension}")
        """ save df to csv and then read row by row. Why? we want a panda Dataframe
        and not a panda Series (result of df.iterrows() and df.iloc[idx]) """
        tmp_csv_path = os.path.join(TEMP_DIR, "tmp.csv")
        df_to_csv(df_selection, tmp_csv_path)
        check_results = CheckResults()
        df_nr_rows = df_selection.shape[0]
        for row_idx, df_row in enumerate(
            pd.read_csv(tmp_csv_path, sep="\t", skiprows=0, chunksize=1)
        ):
            log.info(f"check row {row_idx}/{df_nr_rows}")
            okay, msg = self.can_convert_dtypes_one_row(
                df_row, self.clm_desired_dtype_dict
            )
            if not okay:
                check_results.add_invalid(row_idx, msg)
        delete_tmp_csv(tmp_csv_path)
        self.create_valid_invalid_df(df_selection, check_results)

    def run(self):
        # 1. first do some checks on constants
        check_properties(
            self.properties, self.dataframe.columns.tolist(), self.csv_file_full_path
        )

        # 2. get the columns we want
        log.info("start raw csv importer class: " + self.__class__.__name__)
        df_selection = self.dataframe[self.expected_csv_columns]

        # 3. check we can take a shortcut (convert whole table at once)
        whole_table_in_once = self.can_convert_dtypes_table(
            df_selection, self.clm_desired_dtype_dict
        )
        if whole_table_in_once:
            self.import_whole_table_at_once(df_selection)
        else:
            self.import_row_by_row(df_selection)


class LeagueCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES


class CupCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = CUP_GAME_PROPERTIES


class PlayerCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "player"
        self.properties = PLAYER_PROPERTIES
