import os
import pandas as pd
from tools.constants import (
    BASE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    LEAGUE_GAME_PROPERTIES,
    PLAYER_PROPERTIES,
)
from tools.utils import is_panda_df_empty


class BaseCsvCleaner:
    def __init__(self, csvfilepath):
        self.csv_type = None
        self.csv_file_full_path = csvfilepath
        self.properties = BASE_GAME_PROPERTIES
        self._dataframe = None

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

    def run(self):
        pass


class CupCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = CUP_GAME_PROPERTIES


class LeagueCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = LEAGUE_GAME_PROPERTIES


class PlayerCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "player"
        self.properties = LEAGUE_GAME_PROPERTIES
