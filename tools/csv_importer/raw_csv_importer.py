from tools.constants import BASE_GAME_PROPERTIES
from tools.constants import COMMA_DELIMETER
from tools.constants import CUP_GAME_PROPERTIES
from tools.constants import DTYPES
from tools.constants import LEAGUE_GAME_PROPERTIES
from tools.constants import PLAYER_PROPERTIES
from tools.constants import TAB_DELIMETER
from tools.constants import TEMP_DIR
from tools.csv_importer.check_result import CheckResults
from tools.utils import df_to_csv
from tools.utils import ensure_corect_date_format
from tools.utils import is_panda_df_empty

import logging
import numpy as np
import os
import pandas as pd


log = logging.getLogger(__name__)


def detect_delimeter_type(csv_type, csv_file_full_path):
    """Detects whether it is a .csv or .tsv
    csv_type = 'cup' --> column 'date' should exist
    csv_type = 'league' --> column 'date' should exist
    csv_type = 'player' --> column 'xxx' should exist
    """
    expected_column = None
    if csv_type in ["cup", "league"]:
        expected_column = "date"
    elif csv_type == "player":
        # TODO: implement this for players csv as well
        pass
    else:
        raise AssertionError("unexpected csv_type")
    try:
        # is it a .tsv ?
        pd_text_file_reader = pd.read_csv(
            csv_file_full_path, sep=TAB_DELIMETER, skiprows=0, chunksize=1
        )
        row = pd_text_file_reader.get_chunk()
        test = row[expected_column]
        return "\t"
    except Exception:
        try:
            # then it should be a .tsv ?
            pd_text_file_reader = pd.read_csv(
                csv_file_full_path, sep=COMMA_DELIMETER, skiprows=0, chunksize=1
            )
            row = pd_text_file_reader.get_chunk()
            test = row[expected_column]
            return ","
        except Exception:
            raise AssertionError(csv_file_full_path + "not a .tsv/.csv ??")


def possible_fix_delimeter_type(csv_type, csv_path):
    delimiter_type = detect_delimeter_type(csv_type, csv_path)
    if delimiter_type not in [TAB_DELIMETER, COMMA_DELIMETER]:
        msg = "unsupported delimeter type:" + str(delimiter_type) + str(csv_path)
        log.critical(msg)
        raise AssertionError(msg)
    if delimiter_type != TAB_DELIMETER:
        # we need to fix delimeter type
        log.info(f"incorrect delimeter type {delimiter_type} found for csv {csv_path}")
        temp_df = pd.read_csv(csv_path, sep=delimiter_type)
        os.remove(csv_path)
        df_to_csv(temp_df, csv_path)


def check_nan_fix_required(csv_path):
    """Only for csv_type 'cup'
    Check all 'blablabla_score' columns (one by one) if column contains a
    True/False AND a string. If so, then then nan_fix_required
    """
    temp_df = pd.read_csv(csv_path, sep=TAB_DELIMETER)

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
            nr_nan = nr_rows - temp_df[column].count()
        nr_bool = nr_false + nr_true

        if nr_rows > (nr_bool + nr_nan):
            # column includes a leftover: no boolean, no nan, but another string?
            # we need to fix this, so nan_fix_required = True
            return True
    return False


def remove_tab_strings(csv_path):
    unwanted_strings = [r"\t", r"\r", r"\n", r"\\t", r"\\r", r"\\n"]
    new_df = pd.read_csv(csv_path, sep=TAB_DELIMETER)
    for unwanted in unwanted_strings:
        new_df.replace(unwanted, "", regex=True, inplace=True)
    os.remove(csv_path)
    df_to_csv(new_df, csv_path)


def fix_nan_values(csv_path):
    """
    # play_round    score_45    score_90    score_105   score_120   aet     pso             home_goals      away_goals      season          url         home_manager    away_manager        home_sheet      away_sheet      # noqa
    # 1	            0:0	        False	    False	    2	        0	    -2017-2018/	    http....	    Hans van Arum	Dennis Dekkers	[xx, yy]    [xx, yy]        nan                 nan             nan             # noqa


    # 1. copy eerst naar df_copy
    # 2. loop door df
    # 3. - als score_90 is False/True dan moet alles in (alle col vanaf score_90) 3 naar rechts  # noqa
    #    - als score_105 is False/True dan moet alles (alle col vanaf score_105) 2 naar rechts   # noqa
    #    - als score_105 is False/True dan moet alles (alle col vanaf score_120) 1 naar rechts   # noqa

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

    for row_idx, row in enumerate(
        pd.read_csv(csv_path, sep=TAB_DELIMETER, skiprows=0, chunksize=1)
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
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url',
            # 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[:-3]
            # ['score_90', 'score_105', 'score_120', 'aet', 'pso', 'home_goals',
            # 'away_goals', 'season', 'url', 'home_manager']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_90"] = np.NaN
            row_copy["score_105"] = np.NaN
            row_copy["score_120"] = np.NaN
        elif shift_right == 2:
            update_these_columns_first = list(sequence_columns)[3:]
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url',
            # 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[1:-2]
            # ['score_105', 'score_120', 'aet', 'pso', 'home_goals', 'away_goals',
            # 'season', 'url', 'home_manager', 'away_manager']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_105"] = np.NaN
            row_copy["score_120"] = np.NaN
        elif shift_right == 1:
            update_these_columns_first = list(sequence_columns)[3:]
            # ['aet', 'pso', 'home_goals', 'away_goals', 'season', 'url',
            # 'home_manager', 'away_manager', 'home_sheet', 'away_sheet']
            update_to_these_columns = list(sequence_columns)[2:-1]
            # ['score_120', 'aet', 'pso', 'home_goals', 'away_goals', 'season', 'url',
            # 'home_manager', 'away_manager', 'home_sheet']
            assert len(update_these_columns_first) == len(update_to_these_columns)
            row_copy[update_these_columns_first] = row[update_to_these_columns]
            row_copy["score_120"] = np.NaN

        # cummulate the rows to 1 df
        if row_idx == 0:
            new_df = row_copy
        else:
            new_df = pd.concat([new_df, row_copy], ignore_index=True)

    df = get_df_fix_players(csv_path, df=new_df)
    df_to_csv(df, csv_path)  # replace if exists


def get_df_fix_players(csv_path, df=None):
    if is_panda_df_empty(df):
        df = pd.read_csv(csv_path, sep=TAB_DELIMETER)
    fix_needed, index_wrong = fix_needed_players_sheets_in_one_column(csv_path, df)
    if fix_needed:
        df = fix_players_sheets(df, csv_path, index_wrong)
    unnamed_columns = [clm for clm in df.columns if "unnamed" in clm.lower()]
    if len(unnamed_columns) > 0:
        df.drop(unnamed_columns, axis=1, inplace=True)
    return df


def fix_players_sheets(new_df, csv_path, index_wrong):
    index_wrong_list = index_wrong.to_list()

    for row_idx, row in enumerate(
        pd.read_csv(csv_path, sep=TAB_DELIMETER, skiprows=0, chunksize=1)
    ):
        if row_idx not in index_wrong_list:
            continue

        log.warning(f"fix player sheet row_idx {row_idx}")

        # sometimes players are in seperate columns...
        # luckly enough I only put brackets around playersheets, so:
        # - detect first column with '[' in it
        # - detect first column with ']' in it
        # - detect second column with '[' in it
        # - detect second column with ']' in it

        bracket_open_first = None
        bracket_open_second = None
        bracket_close_first = None
        bracket_close_second = None

        # loop though columns
        for clm_idx, clm in enumerate(row.columns):
            if str(row[clm].values).count("[") == 2:
                if not bracket_open_first:
                    bracket_open_first = clm_idx
                else:
                    if not bracket_open_second:
                        # we already count brackets in check_count_brackets(),
                        # so no need to raise here
                        bracket_open_second = clm_idx
            if str(row[clm].values).count("]") == 2:
                if not bracket_close_first:
                    bracket_close_first = clm_idx
                else:
                    if not bracket_close_second:
                        bracket_close_second = clm_idx

        if not bracket_open_second or not bracket_close_second:
            raise AssertionError(
                f"{csv_path} row_idx {row_idx} could not find last column with bracket"
            )

        # dataFrame.loc[<ROWS RANGE> , <COLUMNS RANGE>]
        home_df = row.iloc[:, bracket_open_first : bracket_close_first + 1]
        if is_panda_df_empty(home_df):
            raise AssertionError("wtf, why home_df empty")
        away_df = row.iloc[:, bracket_open_second : bracket_close_second + 1]
        if is_panda_df_empty(away_df):
            raise AssertionError("wtf, why away_df empty")

        # merge df to 1 column
        home_merged = home_df.to_string(header=False, index=False)
        home_merged = home_merged.replace("   ", ",")
        home_merged = home_merged.replace("[,", "[")
        home_merged = home_merged.replace(",]", "]")

        away_merged = away_df.to_string(header=False, index=False)
        away_merged = away_merged.replace("   ", ",")
        away_merged = away_merged.replace("[,", "[")
        away_merged = away_merged.replace(",]", "]")

        # update new_df
        new_df.at[row_idx, "home_sheet"] = home_merged
        new_df.at[row_idx, "away_sheet"] = away_merged

    return new_df


def get_df_fix_managers(csv_file_path):
    """manager cell may not startwith '[' and endwith ']'. If so, change cell to np.Nan """
    df = pd.read_csv(csv_file_path, sep=TAB_DELIMETER)
    for manager in ["home_manager", "away_manager"]:
        all_managers_nan = df[manager].isna().all()
        if not all_managers_nan:
            mask_home_manager_wrong = df[manager].str.startswith("[", na=False) | df[
                manager
            ].str.endswith("]", na=False)
            df.loc[mask_home_manager_wrong, manager] = np.NaN
    return df


def fix_needed_players_sheets_in_one_column(csv_path, df):
    """Check if the _sheet columns are closed by brackets. If not (and url startswith 'http')
    then that row is wrong.
    :param csv_path
    :param df:
    :return:    fix_is_needed --> boolean
                index_true --> pd.Index() (indices of df rows which must be fixed)
    """

    for bracket_type in ["opening", "closing"]:
        # check_count_brackets raise AssertionError if nr bracket is wrong
        check_count_brackets(df, csv_path, bracket_type=bracket_type)
    df_obj = df.select_dtypes(["object"])
    # create same size panda df filled with False
    df_obj_closed_by_bracket = pd.DataFrame(
        False, index=np.arange(len(df_obj)), columns=df_obj.columns
    )
    # update False to True if cell startwith '[' and endswith ']'
    for col in df_obj.columns:
        mask_col_closed_by_brackets = (df_obj[col].str.startswith("[")) & (
            df_obj[col].str.endswith("]")
        )
        if mask_col_closed_by_brackets.any():
            df_obj_closed_by_bracket[col] = mask_col_closed_by_brackets
    # check if columns home_sheet and away_sheet DO contain '[' and ']'
    mask_sheets_okay = (
        df_obj_closed_by_bracket["home_sheet"] & df_obj_closed_by_bracket["away_sheet"]
    )
    mask_url_startswith_http = df["url"].str.startswith("http")
    # we can only expect sheets with brackets is url starts with http
    mask_wrong = mask_url_startswith_http & ~mask_sheets_okay
    idx_wrong = mask_wrong[mask_wrong].index
    if len(idx_wrong) > 0:
        # one/both sheet column(s) do/does not startwith '[' and end with ']'
        return True, idx_wrong
    return False, None


def check_count_brackets(df, csv_path, bracket_type=None):
    assert bracket_type in ["opening", "closing"]
    if bracket_type == "opening":
        bracket_sign = "\["
    else:
        bracket_sign = "\]"

    df_obj = df.select_dtypes(["object"])
    df_obj_count_bracket = pd.DataFrame(
        0, index=np.arange(len(df_obj)), columns=df_obj.columns
    )
    for col in df_obj.columns:
        mask_count_brackets = df_obj[col].str.count(bracket_sign)
        if mask_count_brackets.sum() > 0:
            df_obj_count_bracket[col] = mask_count_brackets

    if max(df_obj_count_bracket.max()) != 1:
        # We expect no more than 1 opening bracket and 1 closing bracket type per cell
        mask_multiple_bracket_per_cell = (df_obj_count_bracket > 1).sum(axis=1) > 0
        if mask_multiple_bracket_per_cell.any():
            idx = mask_multiple_bracket_per_cell[mask_multiple_bracket_per_cell].index
            raise AssertionError(
                f"more than 1 {bracket_type} bracket found in 1 cell in row(s) {str(idx)} in {csv_path}"
            )
        # We expect no more than 2 opening and 2 closing brackets row
        mask_too_many_brackets_per_row = (df_obj_count_bracket).sum(axis=1) > 2
        if mask_too_many_brackets_per_row.any():
            idx = mask_too_many_brackets_per_row[mask_too_many_brackets_per_row].index
            raise AssertionError(
                f"more than 2 {bracket_type} brackets found in row(s) {str(idx)} in {csv_path}"
            )


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
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep=TAB_DELIMETER)
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
            log.info(
                f"{self.csv_file_name_without_extension} create valid (and "
                f"converted) df with {count_valid_rows} rows"
            )
            valid_df = check_results.get_valid_df(
                df_selection, self.clm_desired_dtype_dict
            )
            valid_df_stripped = self.do_strip_columns(valid_df)
            log.info(f"save valid df")
            file_name = self.csv_file_name_without_extension + "_valid.csv"
            full_path = os.path.join(self.csv_file_dir, file_name)
            # save it
            df_to_csv(valid_df_stripped, full_path)

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
            pd.read_csv(tmp_csv_path, sep=TAB_DELIMETER, skiprows=0, chunksize=1)
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
