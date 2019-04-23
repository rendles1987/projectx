import pandas as pd
import os

from tools.csv_adapter.helpers import RowChecker, GameFilenameChecker
from tools.logging import log
from tools.constants import (
    BASE_GAME_PROPERTIES,
    LEAGUE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    DTYPES,
)


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


def check_properties(props, orig_df_columns):
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
            raise AssertionError(expected_clm + " not in existing csv columns")


def df_to_csv(df, csv_path):
    """convert panda dataframe to csv"""
    # check if file not already exists
    if os.path.isfile(csv_path):
        log.warning("creating " + csv_path + ", but it already exists. Replacing..")
        os.remove(csv_path)
    df.to_csv(csv_path, sep="\t")


def delete_tmp_csv(tmp_csv_path):
    # remove if file exists
    if os.path.isfile(tmp_csv_path):
        os.remove(tmp_csv_path)
    else:
        log.warning("want to remove " + tmp_csv_path + " but it does not exists")


class BaseGameCsvAdapter:
    """each game csv goes trough a child class of GameBaseCsvAdapter.
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
        self._country = None

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
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep="\t")
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

    def run(self):

        # 1. first do some checks on constants
        check_properties(self.properties, self.dataframe.columns.tolist())

        # 2. get info from filename
        filename_checker = GameFilenameChecker(self.csv_file_full_path, self.csv_type)
        country = filename_checker.country
        season = filename_checker.season
        game_name = filename_checker.game_name

        # 3. get the columns we want
        log.info("start raw csv adapter class: " + self.__class__.__name__)
        df_only_expected_col = self.dataframe[self.expected_csv_columns]

        # 4. save to tmp csv
        """now save df to csv and then read row by row. Why? we want a panda Dataframe
        and not a panda Series (result of df.iterrows() and df.iloc[idx]) """
        tmp_csv_path = "/work/data/tmp_data/tmp.csv"
        df_to_csv(df_only_expected_col, tmp_csv_path)
        df_nr_rows = df_only_expected_col.shape[0]
        row_checker = RowChecker(
            self.clm_desired_dtype_dict, df_nr_rows, self.strip_clmns
        )
        for row_idx, df_row in enumerate(
            pd.read_csv(tmp_csv_path, sep="\t", skiprows=0, chunksize=1)
        ):
            row_checker.do_all_checks(row_idx, df_row)
        delete_tmp_csv(tmp_csv_path)

        count_invalid_rows = row_checker.check_results.count_invalid_rows
        if count_invalid_rows > 0:
            log.info(f"create invalid df with {count_invalid_rows} rows")
            invalid_df = row_checker.check_results.get_invalid_df(self.dataframe)
            log.info(f"save invalid df")
            file_name = self.csv_file_name_without_extension + "_invalid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            self.df_to_csv(invalid_df, full_path)
        count_valid_rows = df_nr_rows - count_invalid_rows
        if count_valid_rows > 0:
            log.info(f"create valid (and converted) df with {count_valid_rows} rows")
            valid_df = row_checker.check_results.get_valid_df(
                df_only_expected_col, self.clm_desired_dtype_dict
            )
            log.info(f"save valid df")
            file_name = self.csv_file_name_without_extension + "_valid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            self.df_to_csv(valid_df, full_path)


class LeagueCsvAdapter(BaseGameCsvAdapter):
    def __init__(self, csvfilepath):
        BaseGameCsvAdapter.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES


class CupCsvAdapter(BaseGameCsvAdapter):
    def __init__(self, csvfilepath):
        BaseGameCsvAdapter.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = CUP_GAME_PROPERTIES
