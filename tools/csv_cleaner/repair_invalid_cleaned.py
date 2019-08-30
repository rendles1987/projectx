from tools.constants import CUP_GAME_PROPERTIES
from tools.constants import LEAGUE_GAME_PROPERTIES
from tools.constants import TAB_DELIMETER
from tools.scraper.cup_scraper import CupScraper
from tools.scraper.league_scraper import LeagueScraper
from tools.utils import df_to_csv
from tools.utils import is_panda_df_empty
from tools.utils import string_to_unicode
from tools.utils import temp_get_unicode

import os
import pandas as pd


class BaseRepairInvalidCleaned:
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


class LeagueCsvRepair(BaseRepairInvalidCleaned):
    def __init__(self, csv_type, full_path):
        BaseRepairInvalidCleaned.__init__(self, csv_type, full_path)
        self.csv_type = "league"
        self.properties = LEAGUE_GAME_PROPERTIES

    def run(self):
        for index, row in self.dataframe.iterrows():
            url = row["url"]
            scraper = LeagueScraper(url)
            scraper.scrape()

        # self.save_repaired_csv


class CupCsvRepair(BaseRepairInvalidCleaned):
    def __init__(self, csv_type, full_path):
        BaseRepairInvalidCleaned.__init__(self, csv_type, full_path)
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
        update_tracker = {"managers": False, "sheets": False}
        for index, row in self.dataframe.iterrows():
            scraper = CupScraper(row.url)
            if "managers" in row["msg"]:
                update_tracker["managers"], df_copy = self.check_managers(
                    index, row, df_copy, scraper
                )
            if "home- and away sheet" in row["msg"]:
                update_tracker["sheets"], df_copy = self.check_sheets(
                    index, row, df_copy, scraper
                )
            # check if new data is okay
