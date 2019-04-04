from collections import namedtuple
from tools.constants import (
    COUNTRY_WHITE_LIST,
    BASE_GAME_PROPERTIES,
    LEAGUE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    DTYPES,
)

import pandas as pd
import os
import datetime


class RawCsvInfo:
    def __init__(self):
        self.raw_data_dir = "/work/raw_data/"
        self.raw_cup_dir = self.raw_data_dir + "cup/"
        self.raw_league_dir = self.raw_data_dir + "league/"
        self.raw_player_dir = self.raw_data_dir + "player/"
        self._csv_info = []  # list with tuples

    @staticmethod
    def get_csvs_from_dir(folder_dir):
        csv_paths = [
            os.path.join(folder_dir, file)
            for file in os.listdir(folder_dir)
            if file.endswith(".csv")
        ]
        return csv_paths

    def update_csv_info(self):
        cup_csv_list = self.get_csvs_from_dir(self.raw_cup_dir)
        league_csv_list = self.get_csvs_from_dir(self.raw_league_dir)
        player_csv_list = self.get_csvs_from_dir(self.raw_player_dir)
        if cup_csv_list:
            self.add_to_csv_info(cup_csv_list, csv_type="cup")
        if league_csv_list:
            self.add_to_csv_info(league_csv_list, csv_type="league")
        if player_csv_list:
            self.add_to_csv_info(player_csv_list, csv_type="player")

    def add_to_csv_info(self, csv_list, csv_type=None):
        for csv in csv_list:
            self._csv_info.append((csv_type, csv))

    @property
    def csv_info(self):
        if self._csv_info:
            return self._csv_info
        self.update_csv_info()
        return self._csv_info


class GameBaseCsvAdapter:
    """each game csv goes trough a child class of GameBaseCsvAdapter.
    Class contains a lot of properties that must be retrieved from game csv
    Two game csv exists: cup, league """

    def __init__(self, csvfilepath):
        self.csv_file_path = csvfilepath
        self.converted_row = None
        self._csv_file_name = None
        self._country = None
        self._dataframe = None
        self._dataframe_copy = None
        self._home = None
        self._away = None
        self._csv_columns = None
        self._convert_dct = None
        self.row_valid_list = []
        self.row_invalid_list = []
        self.properties = BASE_GAME_PROPERTIES
        # self._set_meta_attrs()
        self.min_len_teamname = 2
        self.max_len_teamname = 20

        self.check_nametuple_constants()

    # def _set_meta_attrs(self):
    #     """ - Function is called in __init__ and adds metadata to this class.
    #     - the metadata for property 'self.x' is added as property self.x_meta
    #     self.csv_file_name_meta.descr = 'filename + extension'
    #     self.csv_file_name_meta.desired_type = ''
    #     :return:
    #     """
    #     # not all models must have a properties instance
    #     if not hasattr(self, 'properties'):
    #         return
    #     # not all models have filled properties
    #     if not self.properties:
    #         return
    #     # add the meta information to the instance. Uses the field name
    #     # + '_meta' as attribute name.
    #     for _field in self.properties:
    #         if not hasattr(self, _field.name):
    #             continue
    #         setattr(self, _field.name+'_meta', _field)

    @property
    def csv_file_name(self):
        if self._csv_file_name:
            return self._csv_file_name
        self._csv_file_name = self.csv_file_path.split("/")[-1]
        return self._csv_file_name

    def try_row_convert(self, df_row):
        # 1. can whole row be converted to desired datatypes?
        try:
            df_row.astype(self.convert_dct)
            return True, ""
        except Exception as e:
            return False, str(e)

    def create_valid_and_invalid_df(self, invalid_dct):
        # after all checks
        df_copy = pd.read_csv(self.csv_file_path, sep="\t")

        invalid_row_idx_list = list(invalid_dct.keys())
        invalid_msg_list = list(invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)

        # get inverse row index of dataframe
        valid_row_idx_list = list(
            set(df_copy.index.to_list()) - set(invalid_row_idx_list)
        )

        # update df_copy with msg for all invalid rows
        df_copy["msg"] = pd.DataFrame(
            {"msg": invalid_msg_list}, index=invalid_row_idx_list
        )

        # get valid and invalid df (combine these and you get the orig df)
        valid_df = df_copy.iloc[valid_row_idx_list].astype(self.convert_dct)
        invalid_df = df_copy.iloc[invalid_row_idx_list]
        return valid_df, invalid_df

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

        # strip_these_fields must be in string_fields
        for clm in strip_these_fields:
            if clm not in string_fields:
                raise AssertionError(clm + " not in string_columns")

        # strip_these_fields can not be in not_string_fields
        for clm in strip_these_fields:
            if clm in not_string_fields:
                raise AssertionError(clm + " can not be in not_string_columns")

    def columns_strip_whitespace(self, df_copy):
        strip_these_columns = [
            prop.name
            for prop in self.properties
            if prop.strip_whitespace and prop.is_column_name
        ]

        for clm in strip_these_columns:
            df_copy[clm] = df_copy[clm].str.strip()

        return df_copy

    def do_row_by_row(self):
        invalid_dct = {}
        """
        invalid_dct key     = int = row index of panda dataframe (0-based) 
                            = int = row_idx below (also 0-based)
        invalid_dct value   = string = explaining msg why row is invalid  
        invalid_dct e.g. =  (   1, 'could not covert dtype column x',
                                3, 'home_check_xxx failed',
                            }
        """
        df_copy = pd.read_csv(self.csv_file_path, sep="\t")
        df_copy = self.columns_strip_whitespace(df_copy)

        for row_idx, df_row in df_copy.iterrows():

            # for row_idx, df_row in enumerate(
            #     pd.read_csv(self.csv_file_path, sep="\t", skiprows=0, chunksize=1)
            # ):

            # 1. can be converted to desired datatype?
            # 2. check date
            # 3. check home
            # 4. check away
            # 5. check score
            # 6. check play_round
            # 7. check home_goals
            # 8. check away_goals
            # 9. check url
            # 10. check home_manager
            # 11. check away_manager
            # 12. check home_sheet
            # 13. check away_sheet
            # 14. save invalid row as dict and append to self.row_invalid_list
            # 15. save valid row as dict and append to self.row_valid_list

            # can be converted to desired datatype?
            try:
                df_row_convert = df_row.astype(self.convert_dct)
            except Exception as e:
                msg = str(e)
                msg.replace(",", "")
                invalid_dct.update({row_idx: msg})
                continue  # go to next df_row

            # check_date
            okay, msg = self.check_date(df_row_convert)
            if not okay:
                invalid_dct.update({row_idx: msg})
                continue  # go to next df_row

            # check_home_nr_chars
            okay, msg = self.check_home_nr_chars(df_row_convert)
            if not okay:
                invalid_dct.update({row_idx: msg})
                continue  # go to next df_row

            # check_away_nr_chars
            okay, msg = self.check_away_nr_chars(df_row_convert)
            if not okay:
                invalid_dct.update({row_idx: msg})
                continue  # go to next df_row

            # check_score
            okay, msg = self.check_score(df_row_convert)
            if not okay:
                invalid_dct.update({row_idx: msg})
                continue  # go to next df_row

        valid_df, invalid_df = self.create_valid_and_invalid_df(invalid_dct)

    def check_date(self, df_row_convert):
        # between 1999 and 2019
        min_date = "1999-12-31"
        max_date = "2019-12-31"
        okay = (df_row_convert["date"] > min_date).bool() & (
            df_row_convert["date"] < max_date
        ).bool()
        if okay:
            return (True,)
        msg = "date ", df_row_convert["date"], " not in logic range"
        return False, msg

    def check_home_nr_chars(self, df_row_convert):
        column_name = "home"
        okay = (
            (df_row_convert[column_name].str.len() > self.min_len_teamname).bool()
            & (df_row_convert[column_name].str.len() < self.max_len_teamname).bool()
        )
        if okay:
            return True, ""
        msg = "home too little/much chars:" + df_row_convert[column_name].values[0]
        return False, msg

    def check_away_nr_chars(self, df_row_convert):
        column_name = "away"
        okay = (
            (df_row_convert[column_name].str.len() > self.min_len_teamname).bool()
            & (df_row_convert[column_name].str.len() < self.max_len_teamname).bool()
        )
        if okay:
            return True, ""
        msg = "away too little/much chars:" + df_row_convert[column_name].values[0]
        return False, msg

    def check_score(self, df_row_convert):
        """this field must contain 1 digit int, a colon (:), and 1 digit int"""
        column_name = "score"
        okay = True
        if okay:
            return True, ""
        msg = "score weird format" + df_row_convert[column_name].values[0]
        return False, msg

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
        except AttributeError:
            # AttributeError: 'NoneType' object has no attribute 'empty'
            return True
        raise AssertionError("wtf")

    @property
    def dataframe(self):
        if not self.is_panda_df_empty(self._dataframe):
            return self._dataframe
        self._dataframe = pd.read_csv(self.csv_file_path, sep="\t")
        return self._dataframe

    @property
    def dataframe_copy(self):
        if not self.is_panda_df_empty(self._dataframe_copy):
            return self._dataframe_copy
        self._dataframe_copy = pd.read_csv(self.csv_file_path, sep="\t")
        return self._dataframe_copy

    @dataframe_copy.setter
    def dataframe_copy(self, value):
        self._dataframe_copy = value

    def save_to_csv(self):
        # make sure each child class implements its own 'save_csv' method
        raise NotImplementedError

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
    def home(self):
        if self._home:
            return self._home
        self._home = self.dataframe["home"]
        return self._home

    @property
    def away(self):
        if self._away:
            return self._away
        self._away = self.dataframe["away"]
        return self._away

    @property
    def csv_columns(self):
        if self._csv_columns:
            return self._csv_columns
        columns = self.dataframe.columns.to_list()
        remove_this = "Unnamed: 0"
        if remove_this in columns:
            index = columns.index(remove_this)
            del columns[index]
        self._csv_columns = columns
        return self._csv_columns

    @property
    def convert_dct(self):
        if self._convert_dct:
            return self._convert_dct

        map_column_to_desired_type = {
            prop.name: prop.desired_type
            for prop in self.properties
            if prop.is_column_name
        }
        # {'date': 'date', 'home': 'string', 'home_goals': 'int', ......}
        map_type_to_panda_type = {type.name: type.panda_type for type in DTYPES}
        # {'string': 'object', 'int': 'int64', 'date': 'datetime64', ....}
        convert_dct = {
            k: map_type_to_panda_type[v] for k, v in map_column_to_desired_type.items()
        }
        # {'date': 'datetime64', 'home': 'object', 'home_goals': 'int64', ...}

        # keys must be column names in dataframe!
        for col_name in convert_dct.keys():
            assert col_name in self.csv_columns
        self._convert_dct = convert_dct
        return self._convert_dct

    def convert_to_desired_dtype(self):
        """ convert dataframe column datatype all at once (more robust),
        or column by column (faster)? We do one by one """
        import numpy as np

        print("start converting dtype columns for csv: ", self.csv_file_name)
        convert_dct = self.get_convert_dct()
        # convert column "a" to int64 dtype and "b" to complex type
        # df  = df.astype({"a": int, "b": complex})
        self.dataframe_copy = self.dataframe.astype(convert_dct)

    def check_not_null(self):
        not_null_columns = [
            prop.name
            for prop in self.properties
            if prop.is_column_name and prop.is_not_null
        ]
        for _field in not_null_columns:
            column_data = self.dataframe_copy[_field]
            if column_data.isnull().values.any():
                print("ERROR: column:", _field, " has at least 1 emtpy record")

    def run(self):
        self.do_row_by_row()
        #
        # for clmn in self.csv_columns:
        #     self.column_dtype_homogeneous(clmn)
        #
        # self.convert_to_desired_dtype()
        # self.check_not_null()

        # if not csv_prop_dtype == desired_dtype:
        #     self._dataframe_copy = self._dataframe_copy.astype(
        #         {"home_goals": np.float16})

        # dict1_keys = {k * 2: v for (k, v) in dict1.items()}

        # name_desired_dtype_dict = {k:v for (k,v) in self.properties

        # if csv_prop_dtype == desired_dtype:
        # change dtype 1 column. Rest of dataframe remains the same
        # self._dataframe_copy = self._dataframe_copy.astype({"home_goals": np.float16})
        # self.dataframe_copy = self.dataframe_copy

        # convert column "a" to int64 dtype and "b" to complex type
        # df = df.astype({"a": int, "b": complex})

    def get_prop_info(self, prop):
        prop_list = [tupl for tupl in self.properties if tupl.name == prop]
        assert len(prop_list) == 1
        prop_name_tupl = prop_list[0]
        return prop_name_tupl

    def remove_check_prefix(self, function_name):
        prefix = "check_"
        assert function_name.startswith(prefix)
        return function_name[len(prefix) :]

    def check_dtype(self, base_name):
        desired_dtype = self.get_prop_info(base_name).desired_dtype
        raw_data_dtype = getattr(self, base_name).dtype()  # get datatype from dataframe
        return desired_dtype == raw_data_dtype

    @property
    def home(self):
        if self._home:
            return self._home
        self._home = self.dataframe["home"]
        return self._home

    def save_to_csv(self):
        # make sure each child class implements its own 'save_to_csv' method
        raise NotImplementedError

    # def get_slashes_date(self, s_date):
    #
    #     s_date = str(s_date)
    #     # Sept. is not recognized by datetime
    #     s_date = s_date.replace('Sept.', 'Sep.')
    #
    #     date_patterns = ['%B %d, %Y',  # <-- with comma
    #                      '%b. %d, %Y',  # <-- with comma
    #                      '%B %d %Y',
    #                      '%b. %d %Y', ]
    #
    #     # %b = maand afkorting (zonder punt!)   # Jan, Feb, …, Dec (Sept bestaat niet!!)
    #     # %B = maand uitgeschreven              # January, February, …, December
    #     # %d = zero-padded decimal number
    #     # %Y = year 4 digits
    #     for pattern in date_patterns:
    #         try:
    #             dd_mm_yyyy = datetime.strptime(s_date, pattern).date()
    #             date_slashes = dd_mm_yyyy.strftime("%d/%m/%Y")  # '24/06/1987'
    #             return date_slashes
    #         except Exception as e:
    #             pass
    #     raise AssertionError('Date %s is not in expected format' % s_date)

    # def check_date(self):
    #     # check dtype
    #     type = self.dataframe['date'].dtype
    #     assert type == self.date.csv_type
    #
    #     # make sure it is dd/mm/yyyy
    #     date_format = 12
    #

    # def convert(self):
    #            self.convert_date()
    #
    #     for data_property in self._meta:
    #         if data_property.name in
    #     for data_property in data_adapter._meta:
    #         print('name: ', data_property.name,
    #               'desired_type: ', data_property.desired_type,

    # print('hoi')
    #
    #
    # self.check_country()
    # self.check_country()
    #
    # csv_file_name = self.csv_file_name
    # dataframe = self.dataframe
    # dataframe_copy = self.dataframe_copy
    #
    #
    #
    # assert self.csv_file_name is not None
    # assert self.dataframe is not None
    # assert self.dataframe_copy is not None

    # csv_file_dir
    # csv_file_name
    # country
    # game_type
    # game_name
    # date
    # home
    # away
    # home_goals
    # away_goals
    # season
    # url
    # home_manager
    # away_manager
    # home_sheet
    # away_sheet


class CupCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.game_type = "cup"
        self.properties = CUP_GAME_PROPERTIES

    def save_to_csv(self):
        pass


class LeagueCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.game_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES

    def save_to_csv(self):
        pass


class CupCsvAdapter(GameBaseCsvAdapter):
    @property
    def score_45(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("score_45", not_null, csvtype)

    @property
    def score_90(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("score_90", not_null, csvtype)

    @property
    def score_105(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("score_105", not_null, csvtype)

    @property
    def score_120(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("score_120", not_null, csvtype)

    @property
    def score_aet(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("aet", not_null, csvtype)

    @property
    def score_pso(self, exists=True, not_null=True, csvtype="object"):
        return self.name_tuple("pso", not_null, csvtype)

    def save_csv(self):
        pass


# self.score_45_not_null = True
# self.score_45_type = 'object'
# self.score_exists = True
#
# self.score_90_not_null = True
# self.score_90_type = 'object'
# self.score_90_exists = True
#
# self.score_105_not_null = False
# self.score_105_type = 'object'
# self.score_105_exists = True
#
# self.score_120_not_null = False
# self.score_120_type = 'object'
# self.score_120_exists = True
#
# self.aet_not_null = True
# self.aet_type = 'bool'
# self._aet_exists = True
#
# self.pso_not_null = True
# self.pso_type = 'bool'
# self.pso_exists = True
#
