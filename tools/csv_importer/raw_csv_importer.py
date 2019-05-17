import pandas as pd
import os

from tools.csv_importer.filename_checker import (
    CupFilenameChecker,
    LeagueFilenameChecker,
    PlayerFilenameChecker,
)
from tools.csv_importer.row_checker import LeagueRowChecker, CupRowChecker

from tools.constants import (
    BASE_GAME_PROPERTIES,
    LEAGUE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    PLAYER_PROPERTIES,
    DTYPES,
    TEMP_DIR,
)

from tools.logging import log


def check_fix_required(csv_path):
    temp_df = pd.read_csv(csv_path, sep="\t")
    nan_in_df = temp_df.isnull().values.any()
    if nan_in_df:
        return False
    return True


def detect_delimeter_type(csv_file_full_path):
    """" detects whether it is a .csv or .tsv """

    # just try tab first
    try:
        pd_text_file_reader = pd.read_csv(
            csv_file_full_path, sep="\t", skiprows=0, chunksize=1
        )
        row = pd_text_file_reader.get_chunk()
        row["date"]
        return "\t"
    except Exception:
        try:
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

    for row_idx, row in enumerate(
        pd.read_csv(csv_path, sep="\t", skiprows=0, chunksize=1)
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

    for clm in new_df.columns:
        if "unnamed" in clm.lower():
            new_df.drop(clm, axis=1, inplace=True)

    os.remove(csv_path)
    df_to_csv(new_df, csv_path)


def is_panda_df_empty(panda_df):
    """ check is panda dataframe is empty or not
    :return: bool """
    try:
        if panda_df.empty:
            return True
        else:
            return False
    except ValueError:
        try:
            if panda_df:
                return False
            else:
                return True
        except Exception as e:
            raise AssertionError("nested try-except " + str(e))
    except AttributeError:
        # 'NoneType' object has no attribute 'empty'
        return True
    # all other exceptions
    except Exception as e:
        raise AssertionError("No ValueError or AttributeError, but: " + str(e))


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


def df_to_csv(df, csv_path):
    """convert panda dataframe to csv
    By default the following values are interpreted as NaN:
        ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’,
        ‘1.#IND’, ‘1.#QNAN’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’.
    Throughout this whole project we deal with pd's nan in the csv as 'NA' (you see
    NA in the file) """
    # check if file not already exists
    if os.path.isfile(csv_path):
        log.warning("creating " + csv_path + ", but it already exists. Replacing..")
        os.remove(csv_path)
    # df.to_csv(csv_path, index=False, sep="\t", na_rep="NA")
    df.to_csv(csv_path, sep="\t", na_rep="NA", index=False)


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

        self.country = None
        self.season = None
        self.game_name = None

        self.row_checker = None
        self.df_nr_rows = None
        self.df_only_expected_col = None

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

    def create_valid_invalid_df(self):
        count_invalid_rows = self.row_checker.check_results.count_invalid_rows
        if count_invalid_rows > 0:
            log.info(f"create invalid df with {count_invalid_rows} rows")
            invalid_df = self.row_checker.check_results.get_invalid_df(self.dataframe)

            file_name = self.csv_file_name_without_extension + "_invalid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            # save it
            df_to_csv(invalid_df, full_path)
        count_valid_rows = self.df_nr_rows - count_invalid_rows
        if count_valid_rows > 0:
            log.info(f"create valid (and converted) df with {count_valid_rows} rows")
            valid_df = self.row_checker.check_results.get_valid_df(
                self.df_only_expected_col, self.clm_desired_dtype_dict
            )
            log.info(f"save valid df")
            file_name = self.csv_file_name_without_extension + "_valid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            # save it
            df_to_csv(valid_df, full_path)

    def run(self):
        raise NotImplementedError


class LeagueCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES

    def run(self):
        # 1. first do some checks on constants
        check_properties(
            self.properties, self.dataframe.columns.tolist(), self.csv_file_full_path
        )

        # 2. get info from filename
        filename_checker = LeagueFilenameChecker(self.csv_file_full_path)
        self.country = filename_checker.country
        self.season = filename_checker.season
        self.game_name = filename_checker.game_name

        # 3. get the columns we want
        log.info("start raw csv importer class: " + self.__class__.__name__)
        self.df_only_expected_col = self.dataframe[self.expected_csv_columns]
        self.df_nr_rows = self.df_only_expected_col.shape[0]

        # 4. save to tmp csv
        """now save df to csv and then read row by row. Why? we want a panda Dataframe
        and not a panda Series (result of df.iterrows() and df.iloc[idx]) """

        tmp_csv_path = os.path.join(TEMP_DIR, "tmp.csv")
        # tmp_csv_path = "/work/data/tmp_data/tmp.csv"
        df_to_csv(self.df_only_expected_col, tmp_csv_path)

        # 5. check row for row
        self.row_checker = LeagueRowChecker(
            self.clm_desired_dtype_dict, self.df_nr_rows, self.strip_clmns
        )
        for row_idx, df_row in enumerate(
            pd.read_csv(tmp_csv_path, sep="\t", skiprows=0, chunksize=1)
        ):
            self.row_checker.run(row_idx, df_row)
        delete_tmp_csv(tmp_csv_path)
        self.create_valid_invalid_df()


class CupCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = CUP_GAME_PROPERTIES

    def run(self):
        # 1. first do some checks on constants
        existing_columns = self.dataframe.columns.tolist()
        check_properties(self.properties, existing_columns, self.csv_file_full_path)

        # 2. get info from filename
        filename_checker = CupFilenameChecker(self.csv_file_full_path)
        self.country = filename_checker.country
        self.game_name = filename_checker.game_name

        # 3. get the columns we want
        log.info("start raw csv importer class: " + self.__class__.__name__)
        self.df_only_expected_col = self.dataframe[self.expected_csv_columns]
        self.df_nr_rows = self.df_only_expected_col.shape[0]

        # 4. save to tmp csv
        """now save df to csv and then read row by row. Why? we want a panda Dataframe
        and not a panda Series (result of df.iterrows() and df.iloc[idx]) """
        tmp_csv_path = os.path.join(TEMP_DIR, "tmp.csv")
        df_to_csv(self.df_only_expected_col, tmp_csv_path)

        # 5. check row for row
        self.row_checker = CupRowChecker(
            self.clm_desired_dtype_dict, self.df_nr_rows, self.strip_clmns
        )
        for row_idx, df_row in enumerate(
            pd.read_csv(tmp_csv_path, sep="\t", skiprows=0, chunksize=1)
        ):
            self.row_checker.run(row_idx, df_row)
        delete_tmp_csv(tmp_csv_path)
        self.create_valid_invalid_df()


class PlayerCsvImporter(BaseCsvImporter):
    def __init__(self, csvfilepath):
        BaseCsvImporter.__init__(self, csvfilepath)
        self.csv_type = "player"
        self.properties = PLAYER_PROPERTIES

    def run(self):
        # 1. first do some checks on constants
        check_properties(
            self.properties, self.dataframe.columns.tolist(), self.csv_file_full_path
        )

        # 2. get info from filename
        filename_checker = PlayerFilenameChecker(self.csv_file_full_path)
        self.country = filename_checker.country
        self.game_name = filename_checker.game_name
