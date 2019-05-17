import os

from tools.csv_adapter.raw_csv_adapter import (
    CupCsvAdapter,
    LeagueCsvAdapter,
    PlayerCsvAdapter,
)
from tools.constants import RAW_CSV_DIRS, CLEAN_CSV_DIRS
from tools.csv_adapter.filename_checker import CupFilenameChecker, LeagueFilenameChecker
from tools.csv_adapter.raw_csv_adapter import (
    fix_nan_values,
    check_fix_required,
    remove_tab_strings,
)
from tools.csv_adapter.csv_dir_info import RawCsvInfo, CleanCsvInfo
from tools.logging import log

import shutil

# in general:
# https://stackoverflow.com/questions/39100971/how-do-i-release-memory-used-by-a-pandas-dataframe


class ProcessController:
    def __init__(self):
        self.raw_cup_dir = RAW_CSV_DIRS.get("cup")
        self.raw_league_dir = RAW_CSV_DIRS.get("league")
        self.raw_player_dir = RAW_CSV_DIRS.get("player")
        self.raw_dirs = [self.raw_cup_dir, self.raw_league_dir, self.raw_player_dir]

        self.clean_cup_dir = CLEAN_CSV_DIRS.get("cup")
        self.clean_league_dir = CLEAN_CSV_DIRS.get("league")
        self.clean_player_dir = CLEAN_CSV_DIRS.get("player")
        self.clean_dirs = [
            self.clean_cup_dir,
            self.clean_league_dir,
            self.clean_player_dir,
        ]

        """
        ---------------------------------------------------
        scrape data                    datasource raw
        collect all data               ..
        check scraped data             ..
        clean scraped data             ..
        ---------------------------------------------------
        convert data to desired format datasource adapter 1
        save data
        ---------------------------------------------------
        link players                   datasource enriched
        travel distance                ..
        etc                            ..
        ---------------------------------------------------
        select data for ML model       datasource adapter 2
        save data
        ---------------------------------------------------
        do ML stuff                    ML
        ---------------------------------------------------
        """

    def start_scrape(self):
        pass
        # print(NedLeague.scraper_type)

    def check_dirs_exist(self):
        log.info("check_dirs_exist")
        for csv_dir in self.raw_dirs + self.clean_dirs:
            assert os.path.isdir(csv_dir)

    @staticmethod
    def check_raw_data_exists():
        log.info("check_raw_data_exists")
        raw_csv_info = RawCsvInfo()
        assert len(raw_csv_info.csv_info) > 0

    @staticmethod
    def empty_clean_dirs():
        log.info("empty_clean_dirs")
        clean_csv_info = CleanCsvInfo()
        # assert len(clean_csv_info.csv_info) == 0
        if len(clean_csv_info.csv_info) != 0:
            for csv_type, full_path in clean_csv_info.csv_info:
                os.remove(full_path)

    def copy_raw_to_clean(self):
        log.info("copy_raw_to_clean")
        for src_dir, dest_dir in [
            (self.raw_cup_dir, self.clean_cup_dir),
            (self.raw_league_dir, self.clean_league_dir),
            (self.raw_player_dir, self.clean_player_dir),
        ]:
            csv_paths = [
                os.path.join(src_dir, file)
                for file in os.listdir(src_dir)
                if file.endswith(".csv")
            ]
            for csv in csv_paths:
                shutil.copy(csv, dest_dir)

    def improve_cleaned_filename(self):
        log.info("improve_cleaned_filename")
        for csv_dir in self.clean_dirs:
            csv_paths = [
                os.path.join(csv_dir, file)
                for file in os.listdir(csv_dir)
                if file.endswith(".csv")
            ]
            for csv_full_filepath in csv_paths:
                if "-" in csv_full_filepath:
                    os.rename(csv_full_filepath, csv_full_filepath.replace("-", "_"))

    @staticmethod
    def check_improved_filename():
        log.info("check_improved_filename")
        clean_csvs = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csvs.csv_info:
            if csv_type == "cup":
                cup_filename_checker = CupFilenameChecker(csv_file_path)
                cup_filename_checker.check_all()
            if csv_type == "league":
                league_filename_checker = LeagueFilenameChecker(csv_file_path)
                league_filename_checker.check_all()

    def correct_nan_in_csv(self):
        log.info("correct_nan_in_csv")
        raw_csv_info = RawCsvInfo()
        for csv_type, csv_file_path in raw_csv_info.csv_info:
            # i've only seen this in the cup csvs
            if csv_type == "cup":
                # first check if fix_nan_values() is required
                fix_required = check_fix_required(csv_file_path)
                if fix_required:
                    # fix nan and replace csv file
                    fix_nan_values(csv_file_path)

    def remove_tab_strings_from_df(self):
        log.info("remove tab strings from pd df")
        raw_csv_info = RawCsvInfo()
        for csv_type, csv_file_path in raw_csv_info.csv_info:
            remove_tab_strings(csv_file_path)

    def convert_csv_data(self):
        """
        1. put all csvs in the right dirs! see: tools.constants.py RAW_CSV_DIRS
        2. empty output folder
        3. convert raw csv into desired format
        """
        log.info("convert_csv_data")
        raw_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in raw_csv_info.csv_info:
            if csv_type == "league":
                league_adapter = LeagueCsvAdapter(csv_file_path)
                league_adapter.run()
            # # TODO: enable also cup and player!
            if csv_type == "cup":
                cup_adapter = CupCsvAdapter(csv_file_path)
                cup_adapter.run()
            # elif csv_type == "player":
            #     player_adapter = PlayerCsvAdapter(csv_file_path)
            #     player_adapter.run()

    def start_check_collect(self):
        pass

    def start_clean_collect(self):
        pass

    def data_adaption_1(self):
        pass

    def save_data_1(self):
        pass

    def link_players(self):
        pass

    def travel_distance(self):
        pass

    def data_adaption_2(self):
        pass

    def save_data_2(self):
        pass

    def run_ml(self):
        pass

    def do_raw_data(self):
        # self.start_scrape()
        self.check_dirs_exist()  # raises when not exists
        self.check_raw_data_exists()  # raises when not exists
        self.empty_clean_dirs()  # empties dir when not empty
        self.copy_raw_to_clean()
        self.improve_cleaned_filename()  # replace '-' with '_'
        self.check_improved_filename()  # raises when filename incorrect
        self.correct_nan_in_csv()  # noqa only for cup csv: shifts cells in cup csv to right and add 'NA'  overwrites existing raw_csv
        self.remove_tab_strings_from_df()  # noqa replace strings like '\t' with '', overwrites existing raw_csv
        self.convert_csv_data()
        # self.start_collect()
        # self.start_check_collect()
        # self.start_clean_collect()
        # self.data_adaption_1()
        # self.save_data_1()

    def do_enrich(self):
        self.link_players()
        self.travel_distance()
        self.data_adaption_2()

    def do_ml(self):
        self.run_ml()

    def do_all(self):
        self.do_raw_data()
        self.do_enrich()
        self.do_ml()
        log.info("shutting down")
