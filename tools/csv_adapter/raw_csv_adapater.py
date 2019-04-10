import pandas as pd
import os

from tools.csv_adapter.helpers import RowChecker
from tools.logging import log
from tools.constants import (
    COUNTRY_WHITE_LIST,
    BASE_GAME_PROPERTIES,
    LEAGUE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    DTYPES,
)


class GameBaseCsvAdapter:
    """each game csv goes trough a child class of GameBaseCsvAdapter.
    Class contains a lot of properties that must be retrieved from game csv
    Two game csv exists: cup, league """

    def __init__(self, csvfilepath):
        self.csv_type = "base"
        self.csv_file_full_path = csvfilepath  # '/work/data/raw_data/league/xx.csv'
        self.properties = BASE_GAME_PROPERTIES

        self._dataframe = None
        self._expected_csv_columns = None
        self._existing_csv_columns = None
        self._clm_desired_dtype_dct = None
        self._country = None

        self.check_nametuple_constants()

    @property
    def class_name(self):
        return self.__class__.__name__

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

    def check_nametuple_constants(self):
        strip_these_fields = [
            prop.name for prop in self.properties if prop.strip_whitespace
        ]
        string_fields = [
            prop.name for prop in self.properties if prop.desired_type == "string"
        ]
        not_string_fields = [
            prop.name for prop in self.properties if prop.desired_type != "string"
        ]

        # check 1 strip_these_fields must be in string_fields
        for clm in strip_these_fields:
            if clm not in string_fields:
                raise AssertionError(clm + " not in string_columns")

        # check 2 strip_these_fields can not be in not_string_fields
        for clm in strip_these_fields:
            if clm in not_string_fields:
                raise AssertionError(clm + " can not be in not_string_columns")

        # check 3 expected columns (prop.is_column_name) must be in existing columns
        for expected_clm in self.expected_csv_columns:
            if expected_clm not in self.existing_csv_columns:
                raise AssertionError(expected_clm + " not in existing csv columns")

    def columns_strip_whitespace(self, df):
        """ stip all columns in dataframe that strip_whitespace=True and
        is_column_name=True and return updated dataframe
        :param df: any panda dataframe
        :return: return stripped dataframe
        """
        strip_these_columns = [
            prop.name
            for prop in self.properties
            if prop.strip_whitespace and prop.is_column_name
        ]
        for clm in strip_these_columns:
            df[clm] = df[clm].str.strip()
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

    @property
    def dataframe(self):
        if not self.is_panda_df_empty(self._dataframe):
            return self._dataframe
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep="\t")
        return self._dataframe

    @property
    def country(self):
        if self._country:
            return self._country
        self._country = self.csv_file_name[0:3]
        return self._country

    def check_country(self):
        """check if 4th char csvfilename is underscore,
        check if country in whitelist """
        assert self.csv_file_name[3] != "_", (
            "4th char of csv file",
            self.csv_file_name,
            'name is not "_"',
        )
        assert self.country in COUNTRY_WHITE_LIST, (
            "country",
            self.country,
            "not in ",
            COUNTRY_WHITE_LIST,
        )

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

    # def check_not_null(self):
    #     not_null_columns = [
    #         prop.name
    #         for prop in self.properties
    #         if prop.is_column_name and prop.is_not_null
    #     ]
    #     for _field in not_null_columns:
    #         column_data = self.dataframe_copy[_field]
    #         if column_data.isnull().values.any():
    #             print("ERROR: column:", _field, " has at least 1 emtpy record")

    # def get_prop_info(self, prop):
    #     prop_list = [tupl for tupl in self.properties if tupl.name == prop]
    #     assert len(prop_list) == 1
    #     prop_name_tupl = prop_list[0]
    #     return prop_name_tupl

    @staticmethod
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
            except:
                pass
        except AttributeError:  # 'NoneType' object has no attribute 'empty'
            return True
        raise AssertionError("wtf")

    @staticmethod
    def df_to_csv(df, csv_path):
        """convert panda dataframe to csv"""
        # check if file not already exists
        if os.path.isfile(csv_path):
            log.warning("want to create csv, but it already exists. Replacing...")
            os.remove(csv_path)
        df.to_csv(csv_path, sep="\t")

    @staticmethod
    def delete_tmp_csv(tmp_csv_path):
        # check if file exists
        if os.path.isfile(tmp_csv_path):
            os.remove(tmp_csv_path)
        else:
            log.warning("want to remove tmp csv, but it does not exists")

    def save_to_csv(self):
        # make sure each child class implements its own 'save_to_csv' method
        raise NotImplementedError

    def run(self):
        log.info("start raw csv adapter class: " + self.class_name)
        df_only_expected_col = self.dataframe[self.expected_csv_columns]
        df_only_expected_col = self.columns_strip_whitespace(df_only_expected_col)
        df_nr_rows = df_only_expected_col.shape[0]
        # now save df to csv and then read row by row. Why? we want a panda Dataframe
        # and not a panda Series (result of df.iterrows() and df.iloc[idx])
        tmp_csv_path = "/work/data/tmp_data/tmp.csv"
        self.df_to_csv(df_only_expected_col, tmp_csv_path)
        row_checker = RowChecker(self.clm_desired_dtype_dict, df_nr_rows)
        for row_idx, df_row in enumerate(
            pd.read_csv(tmp_csv_path, sep="\t", skiprows=0, chunksize=1)
        ):
            row_checker.do_all_checks(row_idx, df_row)
        self.delete_tmp_csv(tmp_csv_path)

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


class LeagueCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES

    def save_to_csv(self):
        pass


class CupCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = CUP_GAME_PROPERTIES

    def save_to_csv(self):
        pass
