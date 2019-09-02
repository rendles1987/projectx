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
    def __init__(self, csv_type, full_path):
        BaseCsvCleaned.__init__(self, csv_type, full_path)

    def unlist_a_string(self, list_with_strings):
        try:
            unstring_list = literal_eval(list_with_strings)
            return unstring_list
        except Exception as e:
            print("hoi")

    def __unicode_manager_to_string(self, unicode_row):
        # "[b'Nikola Rakojevi\\xc4\\x87']"
        unicode_home_manager = unicode_row.home_manager.values[0]
        # [b'Nikola Rakojevi\xc4\x87']
        if unicode_home_manager:
            unstring_list = self.unlist_a_string(unicode_home_manager)
            assert len(unstring_list) == 1
            string_home_manager = unstring_list[0].decode()
        else:
            string_home_manager = np.nan

        unicode_away_manager = unicode_row.away_manager.values[0]
        if unicode_away_manager:
            unstring_list = self.unlist_a_string(unicode_away_manager)
            assert len(unstring_list) == 1
            string_away_manager = unstring_list[0].decode()
        else:
            string_away_manager = np.nan

        return string_home_manager, string_away_manager

    def update_managers(self, df_copy, index, unicode_row):
        string_home_manager, string_away_manager = self.__unicode_manager_to_string(
            unicode_row
        )
        orig_home_manager = df_copy["home_manager"][index]
        orig_away_manager = df_copy["away_manager"][index]

        if orig_home_manager != string_home_manager:
            log.info(
                f"{unicode_row.url.values[0]} update home_manager {orig_home_manager} to {string_home_manager}"
            )
            df_copy["home_manager"][index] = string_home_manager
        if orig_away_manager != string_away_manager:
            log.info(
                f"{unicode_row.url.values[0]} update away_manager {orig_home_manager} to {string_away_manager}"
            )
            df_copy["away_manager"][index] = string_away_manager
        return df_copy

    def __unicode_subs_to_string(self, unicode_row):
        home_subs_strings = []
        unicode_home_subs = unicode_row.home_subs.values[0]
        unstring_list = self.unlist_a_string(unicode_home_subs)
        for home_sub in unstring_list:
            home_subs_strings.append(home_sub.decode())
        if not home_subs_strings:
            home_subs_strings = np.NaN

        away_subs_strings = []
        unicode_away_subs = unicode_row.away_subs.values[0]
        unstring_list = self.unlist_a_string(unicode_away_subs)
        for away_sub in unstring_list:
            away_subs_strings.append(away_sub.decode())
        if not away_subs_strings:
            away_subs_strings = np.NaN

        return home_subs_strings, away_subs_strings

    def __unicode_sheets_to_string(self, unicode_row):
        home_sheet_strings = []
        unicode_home_sheet = unicode_row.home_sheet.values[0]
        unstring_list = self.unlist_a_string(unicode_home_sheet)
        for home_player in unstring_list:
            home_sheet_strings.append(home_player.decode())
        away_sheet_strings = []
        unicode_away_sheet = unicode_row.away_sheet.values[0]
        unstring_list = self.unlist_a_string(unicode_away_sheet)
        for away_player in unstring_list:
            away_sheet_strings.append(away_player.decode())
        return home_sheet_strings, away_sheet_strings

    def add_subs(self, df_copy, index, unicode_row):
        list_string_home_subs, list_string_away_subs = self.__unicode_subs_to_string(
            unicode_row
        )
        df_copy["home_subs"][index] = str(list_string_home_subs)
        df_copy["away_subs"][index] = str(list_string_home_subs)
        return df_copy

    def update_sheets(self, df_copy, index, unicode_row):
        list_string_home_sheet, list_string_away_sheet = self.__unicode_sheets_to_string(
            unicode_row
        )
        # always update the original value
        df_copy["home_sheet"][index] = str(list_string_home_sheet)
        df_copy["away_sheet"][index] = str(list_string_home_sheet)
        return df_copy

    def run(self):
        df_copy = self.dataframe.copy()
        df_copy["home_subs"] = ""
        df_copy["away_subs"] = ""
        df_unicodes = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
        for index, row in self.dataframe.iterrows():

            # check if this orig_row exists in df_unicodes
            mask = (
                (df_unicodes.home == row.home)
                & (df_unicodes.away == row.away)
                & (df_unicodes.date == row.date)
            )
            if mask.any():
                unicode_row = df_unicodes[mask == True]
                assert len(unicode_row) == 1
            else:
                log.error(
                    f"cant update: {self.csv_file_full_path} as this url is "
                    f"not in sqlite {row.url}"
                )
                # we cannot do this csv, lets go to the next one
                return

            df_copy = self.update_managers(df_copy, index, unicode_row)
            df_copy = self.update_sheets(df_copy, index, unicode_row)
            df_copy = self.add_subs(df_copy, index, unicode_row)

        df_to_csv(df_copy, self.csv_file_full_path)


class LeagueCsvRepair(BaseCsvCleaned):
    def __init__(self, csv_type, full_path):
        BaseCsvCleaned.__init__(self, csv_type, full_path)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES

    def run(self):
        for index, row in self.dataframe.iterrows():
            url = row["url"]
            scraper = LeagueScraper(url)
            scraper.scrape()
        # self.save_repaired_csv


class CupCsvRepair(BaseCsvCleaned):
    def __init__(self, csv_type, full_path):
        BaseCsvCleaned.__init__(self, csv_type, full_path)
        self.csv_type = "cup"
        self.properties = CUP_GAME_PROPERTIES

    def managers_the_same(self, new, orig):
        assert isinstance(new, bytes)
        assert isinstance(orig, str)
        return new == temp_get_unicode(orig)

    def sheets_the_same(self, new, orig):
        assert isinstance(new, list)
        assert isinstance(orig, str)
        orig = orig.replace("[", "")
        orig = orig.replace("]", "")
        orig_list = orig.split(",")
        # orig_list_compare = [string_to_unicode(x.strip()) for x in orig_list if x]
        orig_list_compare = [temp_get_unicode(x.strip()) for x in orig_list if x]
        return set(new) == set(orig_list_compare)

    def check_managers(self, index, row, df_copy, scraper):
        updated = False
        new_home_manager = scraper.home_manager
        new_away_manager = scraper.away_manager
        if not self.managers_the_same(new_home_manager, row.home_manager):
            df_copy.iloc[index]["home_manager"] = new_home_manager
            updated = True
        if not self.managers_the_same(new_away_manager, row.away_manager):
            df_copy.iloc[index]["away_manager"] = new_away_manager
            updated = True
        return updated, df_copy

    def check_sheets(self, index, row, df_copy, scraper):
        updated = False
        new_home_sheet = scraper.home_sheet
        new_away_sheet = scraper.away_sheet
        if not self.sheets_the_same(new_home_sheet, row.home_sheet):
            df_copy.iloc[index]["home_sheet"] = new_home_sheet
            updated = True
        if not self.sheets_the_same(new_away_sheet, row.away_sheet):
            df_copy.iloc[index]["away_sheet"] = new_away_sheet
            updated = True
        return updated, df_copy

    def run(self):
        df_copy = self.dataframe.copy()
        df_unicodes = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
        for index, orig_row in self.dataframe.iterrows():
            # track whether managers and/or sheets have been updated
            update_tracker = {"managers": False, "sheets": False}

            # check if this orig_row exists in df_unicodes
            mask = (
                (df_unicodes.home == orig_row.home)
                & (df_unicodes.away == orig_row.away)
                & (df_unicodes.date == orig_row.date)
            )
            if mask.any():
                unicode_row = df_unicodes[mask == True]
                assert len(unicode_row) == 1
            else:
                # we cannot do this _invalid.csv, lets go to the next one
                return

            if "managers" in orig_row["msg"]:
                orig_home_manager = orig_row.home_manager
                unicode_home_manager = unicode_row.home_manager.values[
                    0
                ]  # "[b'Nikola Rakojevi\\xc4\\x87']"
                unstring_list = self.unlist_a_string(
                    unicode_home_manager
                )  # [b'Nikola Rakojevi\xc4\x87']
                assert len(unstring_list) == 1
                string_home_manager = unstring_list[0].decode()

                unicode_home_manager = unicode_home_manager.rsplit(sep="[")[
                    1
                ]  # "b'Nikola Rakojevi\\xc4\\x87']"
                unicode_home_manager = unicode_home_manager.rsplit(sep="']")[
                    0
                ]  # "b'Nikola Rakojevi\\xc4\\x87"

                c = unicode_home_manager

                # my_unicode = my_string.encode("utf-8").decode("unicode_escape").encode()

                unicode_home_manager = unicode_home_manager.replace("[", "")
                unicode_home_manager = unicode_home_manager.replace("]", "")
                unicode_home_manager = unicode_home_manager.replace('"', "")

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
