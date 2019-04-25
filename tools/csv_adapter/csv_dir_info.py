from tools.constants import RAW_CSV_DIRS, CLEAN_CSV_DIRS
import os


class RawCsvInfo:
    """ This class gets an overview of all raw csvs. Class is instanced in early
    stage of raw data check. Only public method is csv_info """

    def __init__(self):
        self.cup_dir = RAW_CSV_DIRS.get("cup")
        self.league_dir = RAW_CSV_DIRS.get("league")
        self.player_dir = RAW_CSV_DIRS.get("player")
        self._csv_info = []  # list with tuples

    @property
    def csv_info(self):
        """
        :return: self._csv_info (list with tuples) e.g.: [
        ('cup', '/work/data/raw_data/cup/all_games_ned_knvb_beker.csv'),
        ('league', '/work/data/raw_data/league/ned_eredivisie-2016-2017-spieltag.csv')
        ]
        """
        if self._csv_info:
            return self._csv_info
        self.__update_csv_info()
        return self._csv_info

    def __check_constants(self):
        """ private method called in constructor """
        assert os.path.isdir(self.cup_dir)
        assert os.path.isdir(self.league_dir)
        assert os.path.isdir(self.player_dir)

    def __update_csv_info(self):
        self.__check_constants()
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(self.cup_dir):
            self._csv_info.append(("cup", csv_full_filepath))
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(self.league_dir):
            self._csv_info.append(("league", csv_full_filepath))
        for csv_full_filepath in self.__get_not_checked_csvs_from_dir(self.player_dir):
            self._csv_info.append(("player", csv_full_filepath))

    @staticmethod
    def __get_not_checked_csvs_from_dir(folder_dir):
        """ get all .csvs - that have no 'valid' or 'invalid' in filename -
        from a directory
        :param folder_dir: string of dir's full path
        :return: csv_paths (list with strings)"""
        csv_paths = [
            os.path.join(folder_dir, file)
            for file in os.listdir(folder_dir)
            if file.endswith(".csv")
        ]
        return csv_paths


class CleanCsvInfo(RawCsvInfo):
    def __init__(self):
        self.cup_dir = CLEAN_CSV_DIRS.get("cup")
        self.league_dir = CLEAN_CSV_DIRS.get("league")
        self.player_dir = CLEAN_CSV_DIRS.get("player")
        self._csv_info = []  # list with tuples
