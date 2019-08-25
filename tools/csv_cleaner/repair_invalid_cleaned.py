import os
from tools.utils import is_panda_df_empty, df_to_csv
from tools.constants import TAB_DELIMETER, LEAGUE_GAME_PROPERTIES, CUP_GAME_PROPERTIES
import pandas as pd
from tools.scraper.cup_scraper import CupScraper
from tools.scraper.league_scraper import LeagueScraper


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

    def run(self):
        for index, row in self.dataframe.iterrows():
            scraper = CupScraper(row.url)
            if "managers" in row["msg"]:
                new_home_manager = scraper.home_manager
                new_away_manager = scraper.away_manager
            if "home- and away sheet" in row["msg"]:
                home_sheet = scraper.home_sheet
                away_sheet = scraper.away_sheet

