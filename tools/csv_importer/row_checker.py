from tools.csv_importer.check_result import CheckResults
from tools.logging import log

import pandas as pd


# def date_in_range(df_row, column_name, min_date, max_date):
#     date_format = "%d/%m/%Y"
#     date = pd.to_datetime(df_row[column_name], format=date_format)
#     okay = (date > min_date).bool() & (date < max_date).bool()
#     if not okay:
#         msg = column_name + " not in logic range"
#         return False, msg
#     return True, ""


# def string_len_in_range(df_row, column_name, min_len, max_len):
#     okay = (df_row[column_name].str.len() > min_len).bool() & (
#         df_row[column_name].str.len() < max_len
#     ).bool()
#     if okay:
#         return True, ""
#     msg = (
#         str(column_name)
#         + " too little/much chars: "
#         + str(df_row[column_name].values[0])
#     )
#     return False, msg


# def score_format(column_name, df_row, max_nr_digits):
#     """ - this string must be 3 chars long e.g "3:5"
#         - field must contain 1 digit int, a colon (:), and 1 digit int"""
#     score_string = df_row[column_name].values[0]
#     home_score = score_string.split(":")[0]
#     away_score = score_string.split(":")[1]
#
#     okay = (
#         score_string.count(":") == 1
#         and home_score.isdigit()
#         and away_score.isdigit()
#         and len(home_score) <= max_nr_digits
#         and len(away_score) <= max_nr_digits
#     )
#     if okay:
#         return True, ""
#     msg = "not correct score format: " + score_string
#     return False, msg


# def url_unknown(column_name, df_row_convert_strip, expected_unknown, expected_known):
#     okay = (
#         expected_known in df_row_convert_strip[column_name]
#         or expected_unknown in df_row_convert_strip[column_name]
#     )
#     if okay:
#         return True, ""
#     unexpected_string = df_row_convert_strip[column_name]
#     msg = "unexpected unknown in column: " + column_name + ": " + unexpected_string
#     return False, msg


class LeagueRowChecker:
    def __init__(self, clm_desired_dtype_dict, df_nr_rows, strip_clmns):
        self.clm_desired_dtype_dict = clm_desired_dtype_dict
        self.df_nr_rows = df_nr_rows
        self.strip_clmns = strip_clmns
        self.check_results = CheckResults()
        # self.date_format = "%d/%m/%Y"
        # # TODO: min_date and max_date should depend on season
        # self.min_date = pd.to_datetime("31/12/1999", format=self.date_format)
        # self.max_date = pd.to_datetime("31/12/2019", format=self.date_format)

    def row_checks(self, row_idx, df_row):
        # 1. can convert columns?
        okay, msg = can_convert_dtypes_one_row(df_row, self.clm_desired_dtype_dict)
        if not okay:
            self.check_results.add_invalid(row_idx, msg)
            return False
        df_row_convert = convert_dtypes(df_row, self.clm_desired_dtype_dict)
        # 2. can strip ?
        okay, msg = can_strip_columns_one_row(df_row_convert, self.strip_clmns)
        if not okay:
            self.check_results.add_invalid(row_idx, msg)
            return False
        return True

    # def check_date_in_range(self, row_idx, df_row_convert_strip):
    #     column_name = "date"
    #     okay, msg = date_in_range(
    #         df_row_convert_strip, column_name, self.min_date, self.max_date
    #     )
    #     if not okay:
    #         self.check_results.add_invalid(row_idx, msg)
    #
    # def check_score_format(self, row_idx, df_row_convert_strip):
    #     column_name = "score"
    #     max_nr_digits = 1  # for league results we expect nr of goals 1 team <10
    #     okay, msg = score_format(column_name, df_row_convert_strip, max_nr_digits)
    #     if not okay:
    #         self.check_results.add_invalid(row_idx, msg)
    #
    # def check_url_unknown(self, row_idx, df_row_convert_strip):
    #     column_name = "url"
    #     expected_unknown = "no_url_exists"
    #     expected_known = "http://www."
    #     okay, msg = url_unknown(
    #         column_name, df_row_convert_strip, expected_unknown, expected_known
    #     )
    #     if not okay:
    #         self.check_results.add_invalid(row_idx, msg)

    def run(self, row_idx, df_row):
        s_row_idx = str(row_idx)
        log.info(f"check row {s_row_idx}/{self.df_nr_rows}")
        self.row_checks(row_idx, df_row)

        # df_row_convert = convert_dtypes(df_row, self.clm_desired_dtype_dict)
        # df_row_convert_strip = strip_columns(df_row_convert, self.strip_clmns)

        # self.check_date_in_range(row_idx, df_row_convert_strip)
        # self.check_score_format(row_idx, df_row_convert_strip)
        # self.check_url_unknown(row_idx, df_row_convert_strip)


class CupRowChecker(LeagueRowChecker):
    def __init__(self, clm_desired_dtype_dict, df_nr_rows, strip_clmns):
        LeagueRowChecker.__init__(self, clm_desired_dtype_dict, df_nr_rows, strip_clmns)
        self.df_nr_rows = df_nr_rows
        self.strip_clmns = strip_clmns
        self.df_row_convert_strip = None
        self.check_results = CheckResults()
        # TODO put these constants in constants.py
        self.date_format = "%Y/%m/%d"
        # TODO: min_date and max_date should depend on season
        self.min_date = pd.to_datetime("1999/12/31", format=self.date_format)
        self.max_date = pd.to_datetime("2019/12/12", format=self.date_format)

    # def check_score_format(self, row_idx, df_row_convert_strip):
    #     column_name = "score"
    #     max_nr_digits = (
    #         2
    #     )  # noqa for cup results we expect nr of goals of 1 team <100 incl penalties
    #     okay, msg = score_format(column_name, df_row_convert_strip, max_nr_digits)
    #     if not okay:
    #         self.check_results.add_invalid(row_idx, msg)
