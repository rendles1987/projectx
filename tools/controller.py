import os

from tools.csv_importer.raw_csv_importer import CupCsvImporter, LeagueCsvImporter
from tools.constants import RAW_CSV_DIRS, IMPORT_CSV_DIRS, CLEAN_CSV_DIRS
from tools.csv_importer.filename_checker import (
    CupFilenameChecker,
    LeagueFilenameChecker,
)
from tools.csv_importer.raw_csv_importer import (
    fix_nan_values,
    check_fix_required,
    remove_tab_strings,
)
from tools.csv_dir_info import RawCsvInfo, ImportCsvInfo, CleanCsvInfo
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

        self.import_cup_dir = IMPORT_CSV_DIRS.get("cup")
        self.import_league_dir = IMPORT_CSV_DIRS.get("league")
        self.import_player_dir = IMPORT_CSV_DIRS.get("player")
        self.import_dirs = [
            self.import_cup_dir,
            self.import_league_dir,
            self.import_player_dir,
        ]

        self.clean_cup_dir = CLEAN_CSV_DIRS.get("cup")
        self.clean_league_dir = CLEAN_CSV_DIRS.get("league")
        self.clean_player_dir = CLEAN_CSV_DIRS.get("player")
        self.clean_dirs = [
            self.clean_cup_dir,
            self.clean_league_dir,
            self.clean_player_dir,
        ]

    def start_scrape(self):
        pass
        # print(NedLeague.scraper_type)

    @staticmethod
    def check_dirs_exist(*args):
        for directory in args:
            directory_string = str(directory)
            log.info(f"check if {directory_string} dirs exist")
            for csv_dir in directory:
                assert os.path.isdir(csv_dir)

    @staticmethod
    def check_raw_data_exists():
        log.info("check if 1 or more raw data files exists")
        raw_csv_info = RawCsvInfo()
        assert len(raw_csv_info.csv_info) > 0

    @staticmethod
    def check_valid_import_data_exists():
        log.info("check if 1 or more valid import data files exists")
        import_csv_info = ImportCsvInfo()
        assert import_csv_info.count_total_valid_csv() > 0

    @staticmethod
    def empty_import_dirs():
        log.info("empty import dirs")
        import_csv_info = ImportCsvInfo()
        if len(import_csv_info.csv_info) != 0:
            for csv_type, full_path in import_csv_info.csv_info:
                os.remove(full_path)

    @staticmethod
    def empty_clean_dirs():
        log.info("empty clean dirs")
        clean_csv_info = CleanCsvInfo()
        if len(clean_csv_info.csv_info) != 0:
            for csv_type, full_path in clean_csv_info.csv_info:
                os.remove(full_path)

    def copy_raw_to_import(self):
        log.info("copy all existing .csv from raw to import dirs")
        for src_dir, dest_dir in [
            (self.raw_cup_dir, self.import_cup_dir),
            (self.raw_league_dir, self.import_league_dir),
            (self.raw_player_dir, self.import_player_dir),
        ]:
            csv_paths = [
                os.path.join(src_dir, file)
                for file in os.listdir(src_dir)
                if file.endswith(".csv")
            ]
            for csv in csv_paths:
                shutil.copy(csv, dest_dir)

    def copy_valid_import_to_clean(self):
        log.info("copy all existing .csv from import to clean dirs")
        for src_dir, dest_dir in [
            (self.import_cup_dir, self.clean_cup_dir),
            (self.import_league_dir, self.clean_league_dir),
            (self.import_player_dir, self.clean_player_dir),
        ]:
            valid_csv_paths = [
                os.path.join(src_dir, file)
                for file in os.listdir(src_dir)
                if file.endswith("_valid.csv")
            ]
            for csv in valid_csv_paths:
                shutil.copy(csv, dest_dir)

    def improve_imported_filename(self):
        log.info("improve imported filename")
        for csv_dir in self.import_dirs:
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
        log.info("check improved filename")
        import_csvs = ImportCsvInfo()
        for csv_type, csv_file_path in import_csvs.csv_info:
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
        raw_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in raw_csv_info.csv_info:
            if csv_type == "league":
                league_importer = LeagueCsvImporter(csv_file_path)
                league_importer.run()
            # # TODO: enable also cup and player!
            if csv_type == "cup":
                cup_importer = CupCsvImporter(csv_file_path)
                cup_importer.run()
            # elif csv_type == "player":
            #     player_importer = PlayerCsvImporter(csv_file_path)
            #     player_importer.run()

    def clean(self):
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

    def do_scrape(self):
        pass

    def do_import(self):
        self.check_dirs_exist(self.raw_dirs, self.import_dirs)  # raise if not exists
        self.check_raw_data_exists()  # raises when not exists
        self.empty_import_dirs()  # empties dir when not empty
        self.copy_raw_to_import()
        self.improve_imported_filename()  # replace '-' with '_'
        self.check_improved_filename()  # raises when filename incorrect
        self.correct_nan_in_csv()  # noqa only for cup csv: shifts cells in cup csv to right and add 'NA'  overwrites existing raw_csv
        self.remove_tab_strings_from_df()  # noqa replace strings like '\t' with '', overwrites existing raw_csv
        self.convert_csv_data()

    def do_clean(self):
        self.check_dirs_exist(self.import_dirs, self.clean_dirs)  # raise if not exists
        self.check_valid_import_data_exists()  # raises when not exists
        self.empty_clean_dirs()  # empties dir when not empty
        self.copy_valid_import_to_clean()
        self.clean()

    def do_enrich(self):
        """ from here on no .csv anymore, but to sqlite3 """
        self.link_players()
        self.travel_distance()
        self.data_adaption_2()

    def do_ml(self):
        self.run_ml()

    def do_all(self):
        # self.do_scrape()  # scrap data (webpage --> raw)

        # self.do_import()  # import raw data (raw --> import)
        self.do_clean()  # clean data (import --> clean)
        # self.do_enrich()  # enrich data (clean --> enrich)
        # self.do_ml()
        log.info("shutting down")
