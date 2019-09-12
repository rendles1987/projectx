from tools.constants import CUP_GAME_PROPERTIES
from tools.constants import LEAGUE_GAME_PROPERTIES
from tools.constants import TAB_DELIMETER, SQLITE_TABLE_NAMES_UNICODE
from tools.scraper.league_scraper import LeagueScraper
from tools.utils import df_to_csv
from tools.utils import is_panda_df_empty, sqlite_table_to_df, df_to_sqlite_table
from tools.utils import temp_get_unicode
from ast import literal_eval
import logging
import numpy as np

log = logging.getLogger(__name__)

import os
import pandas as pd


class BaseCsvCleaned:
    def __init__(self, csv_type, full_path):
        self.csv_type = csv_type
        self.csv_file_full_path = full_path
        self._dataframe = None
        self._expected_csv_columns = None
        self._existing_csv_columns = None
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

    def save_repaired_csv(self, df):
        file_name = self.csv_file_name_without_extension + "_repaired.csv"
        full_path = os.path.join(self.csv_file_dir, file_name)
        df_to_csv(df, full_path)

    def run(self):
        raise NotImplementedError


class UpdateNamesGameCsv(BaseCsvCleaned):
    """ update names (sheets, managers, etc). Adds column start_time to csvs that have
    just been copied to clean dir """

    def __init__(self, csv_type, full_path):
        log.info(f"update {full_path} with unicode names")
        BaseCsvCleaned.__init__(self, csv_type, full_path)
        self.csv_type = csv_type
        self.full_path = full_path

    def run(self):
        """
        Update 6 columns for all rows that are in sqlite:
        - home_manager, away_manager
        - home_sheet, away_sheet,
        - home_subs, away_sheets
        Add 1 new column:
        - start_time
        """

        df_unicodes = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)

        match_columns = ["date", "home", "away"]
        suffixes = ("_orig", "_new")
        df_merge = pd.merge(
            self.dataframe,
            df_unicodes,
            how="inner",
            on=match_columns,
            suffixes=suffixes,
        )

        if not len(df_merge) == len(self.dataframe):
            print("hoi")

        mask = df_merge["url_orig"] != df_merge["url_new"]
        if len(mask[mask == True]) > 0:
            # url new and orig are not the same
            # if orig = 'no_url_exists' and new = None --> then no problem
            assert all(df_merge[mask]["url_orig"] == "no_url_exists")
            assert all(df_merge[mask]["url_new"].isna())

        # select these columns anyway
        default_col = [
            "date",
            "start_time",
            "home",
            "away",
            "score",
            "home_goals",
            "away_goals",
            "url_new",
            "home_manager_new",
            "away_manager_new",
            "home_sheet_new",
            "away_sheet_new",
            "home_subs",
            "away_subs",
            "source_file",
            "game_type",
            "game_name",
            "country",
        ]
        if self.csv_type == "league":
            desired_col = default_col
        elif self.csv_type == "cup":
            desired_col = default_col
            desired_col.extend(
                [
                    "round_text",
                    "play_round",
                    "score_45",
                    "score_90",
                    "score_105",
                    "score_120",
                    "aet",
                    "pso",
                ]
            )

        df_final = df_merge[desired_col]

        rename_columns = {
            "url_new": "url",
            "home_manager_new": "home_manager",
            "away_manager_new": "home_manager",
            "home_sheet_new": "home_manager",
            "away_sheet_new": "home_manager",
        }

        df_final.rename(columns=rename_columns, inplace=True)

        df_to_csv(df_final, self.csv_file_full_path)


# class LeagueCsvRepair(BaseCsvCleaned):
#     def __init__(self, csv_type, full_path):
#         BaseCsvCleaned.__init__(self, csv_type, full_path)
#         self.csv_type = "league"
#         self.properties = LEAGUE_GAME_PROPERTIES
#
#     def run(self):
#         for index, row in self.dataframe.iterrows():
#             url = row["url"]
#             scraper = LeagueScraper(url)
#             scraper.scrape()
#         # self.save_repaired_csv

#
# class CupCsvRepair(BaseCsvCleaned):
#     def __init__(self, csv_type, full_path):
#         BaseCsvCleaned.__init__(self, csv_type, full_path)
#         self.csv_type = "cup"
#         self.properties = CUP_GAME_PROPERTIES
#
#     def managers_the_same(self, new, orig):
#         assert isinstance(new, bytes)
#         assert isinstance(orig, str)
#         return new == temp_get_unicode(orig)
#
#     def sheets_the_same(self, new, orig):
#         assert isinstance(new, list)
#         assert isinstance(orig, str)
#         orig = orig.replace("[", "")
#         orig = orig.replace("]", "")
#         orig_list = orig.split(",")
#         # orig_list_compare = [string_to_unicode(x.strip()) for x in orig_list if x]
#         orig_list_compare = [temp_get_unicode(x.strip()) for x in orig_list if x]
#         return set(new) == set(orig_list_compare)
#
#     def check_managers(self, index, row, df_copy, scraper):
#         updated = False
#         new_home_manager = scraper.home_manager
#         new_away_manager = scraper.away_manager
#         if not self.managers_the_same(new_home_manager, row.home_manager):
#             df_copy.iloc[index]["home_manager"] = new_home_manager
#             updated = True
#         if not self.managers_the_same(new_away_manager, row.away_manager):
#             df_copy.iloc[index]["away_manager"] = new_away_manager
#             updated = True
#         return updated, df_copy
#
#     def check_sheets(self, index, row, df_copy, scraper):
#         updated = False
#         new_home_sheet = scraper.home_sheet
#         new_away_sheet = scraper.away_sheet
#         if not self.sheets_the_same(new_home_sheet, row.home_sheet):
#             df_copy.iloc[index]["home_sheet"] = new_home_sheet
#             updated = True
#         if not self.sheets_the_same(new_away_sheet, row.away_sheet):
#             df_copy.iloc[index]["away_sheet"] = new_away_sheet
#             updated = True
#         return updated, df_copy
#
#     def run(self):
#         df_copy = self.dataframe.copy()
#         df_unicodes = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
#         for index, orig_row in self.dataframe.iterrows():
#             # track whether managers and/or sheets have been updated
#             update_tracker = {"managers": False, "sheets": False}
#
#             # check if this orig_row exists in df_unicodes
#             mask = (
#                 (df_unicodes.home == orig_row.home)
#                 & (df_unicodes.away == orig_row.away)
#                 & (df_unicodes.date == orig_row.date)
#             )
#             if mask.any():
#                 unicode_row = df_unicodes[mask == True]
#                 assert len(unicode_row) == 1
#             else:
#                 # we cannot do this _invalid.csv, lets go to the next one
#                 return
#
#             if "managers" in orig_row["msg"]:
#                 orig_home_manager = orig_row.home_manager
#                 unicode_home_manager = unicode_row.home_manager.values[
#                     0
#                 ]  # "[b'Nikola Rakojevi\\xc4\\x87']"
#                 unstring_list = self.unlist_a_string(
#                     unicode_home_manager
#                 )  # [b'Nikola Rakojevi\xc4\x87']
#                 assert len(unstring_list) == 1
#                 string_home_manager = unstring_list[0].decode()
#
#                 unicode_home_manager = unicode_home_manager.rsplit(sep="[")[
#                     1
#                 ]  # "b'Nikola Rakojevi\\xc4\\x87']"
#                 unicode_home_manager = unicode_home_manager.rsplit(sep="']")[
#                     0
#                 ]  # "b'Nikola Rakojevi\\xc4\\x87"
#
#                 c = unicode_home_manager
#
#                 # my_unicode = my_string.encode("utf-8").decode("unicode_escape").encode()
#
#                 unicode_home_manager = unicode_home_manager.replace("[", "")
#                 unicode_home_manager = unicode_home_manager.replace("]", "")
#                 unicode_home_manager = unicode_home_manager.replace('"', "")

#
#     update_tracker["managers"], df_copy = self.check_managers(
#         index, row, df_copy, scraper
#     )
# if "home- and away sheet" in row["msg"]:
#     update_tracker["sheets"], df_copy = self.check_sheets(
#         index, row, df_copy, scraper
#     )
#
# # check if new data is okay
