from tools.csv_importer.filename_checker import CupFilenameChecker
from tools.csv_importer.filename_checker import LeagueFilenameChecker
from tools.utils import df_to_csv
from tools.utils import is_panda_df_empty

import os
import pandas as pd


class BaseCsvEnricher:
    def __init__(self, csvfilepath):
        self.csv_type = None
        self.csv_file_full_path = csvfilepath
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

    @dataframe.setter
    def dataframe(self, dataframe):
        self._dataframe = dataframe

    def run(self):
        raise NotImplementedError


class CupCsvEnricher(BaseCsvEnricher):
    def __init__(self, csvfilepath):
        BaseCsvEnricher.__init__(self, csvfilepath)
        self.csv_type = "cup"

    def run(self):
        """ column season is allready filled in csv_cleaner"""
        filenamechecker = CupFilenameChecker(self.csv_file_full_path)
        self.dataframe["game_type"] = self.csv_type
        self.dataframe["game_name"] = filenamechecker.game_name
        self.dataframe["country"] = filenamechecker.country
        self.dataframe["source_file"] = self.csv_file_name_with_extension
        os.remove(self.csv_file_full_path)
        df_to_csv(self.dataframe, self.csv_file_full_path)


class LeagueCsvEnricher(BaseCsvEnricher):
    def __init__(self, csvfilepath):
        BaseCsvEnricher.__init__(self, csvfilepath)
        self.csv_type = "league"

    def run(self):
        filenamechecker = LeagueFilenameChecker(self.csv_file_full_path)
        self.dataframe["game_type"] = self.csv_type
        self.dataframe["game_name"] = filenamechecker.game_name
        self.dataframe["season"] = filenamechecker.season
        self.dataframe["country"] = filenamechecker.country
        self.dataframe["source_file"] = self.csv_file_name_with_extension
        os.remove(self.csv_file_full_path)
        df_to_csv(self.dataframe, self.csv_file_full_path)
