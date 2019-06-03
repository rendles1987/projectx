import os
import shutil

from tools.constants import CLEAN_CSV_DIRS, IMPORT_CSV_DIRS, RAW_CSV_DIRS
from tools.csv_cleaner.csv_cleaner import (
    CupCsvCleaner,
    LeagueCsvCleaner,
    PlayerCsvCleaner,
)
from tools.csv_dir_info import CleanCsvInfo, ImportCsvInfo, RawCsvInfo
from tools.csv_importer.filename_checker import (
    CupFilenameChecker,
    LeagueFilenameChecker,
)
from tools.csv_importer.raw_csv_importer import (
    CupCsvImporter,
    LeagueCsvImporter,
    check_nan_fix_required,
    fix_nan_values,
    remove_tab_strings,
)
from tools.logging import log


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

    def copy_rename_one_file(self):
        pass

    def copy_valid_import_to_clean(self):
        """ copy all "*_valid.csv" files from import to clean dirs"""
        log.info("copy all existing .csv from import to clean dirs")
        for src_dir, dest_dir in [
            (self.import_cup_dir, self.clean_cup_dir),
            (self.import_league_dir, self.clean_league_dir),
            (self.import_player_dir, self.clean_player_dir),
        ]:
            valid_csv_full_paths = [
                os.path.join(src_dir, file)
                for file in os.listdir(src_dir)
                if file.endswith("_valid.csv")
            ]
            valid_csv_filenames = [
                file for file in os.listdir(src_dir) if file.endswith("_valid.csv")
            ]

            # copy them
            for csv_full_path in valid_csv_full_paths:
                # remove _valid from filename since it will be validated again in
                # clean dir
                shutil.copy(csv_full_path, dest_dir)

            # rename the dest files (remove suffix "_valid"
            for filename in valid_csv_filenames:
                filename_without_suffix = filename.split("_valid.csv")[0] + ".csv"
                dest_filename = os.path.join(dest_dir, filename)
                new_dest_filename = os.path.join(dest_dir, filename_without_suffix)
                os.rename(dest_filename, new_dest_filename)

    def improve_imported_filename(self):
        """ replace filename '-' with '_' """
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
    def check_imported_filename():
        log.info("check imported filename")
        import_csvs = ImportCsvInfo()
        for csv_type, csv_file_path in import_csvs.csv_info:
            if csv_type == "cup":
                cup_filename_checker = CupFilenameChecker(csv_file_path)
                cup_filename_checker.check_all()
            if csv_type == "league":
                league_filename_checker = LeagueFilenameChecker(csv_file_path)
                league_filename_checker.check_all()

    def correct_nan_in_csv(self):
        """ only for cup csv: shifts empty cells in cup csv to right and adds
        'NA' to empty cell """
        log.info("correct_nan_in_csv")
        import_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in import_csv_info.csv_info:
            # i've only seen this in the cup csvs
            if csv_type == "cup":
                # first check if fix_nan_values() is required
                nan_fix_required = check_nan_fix_required(csv_file_path)
                if nan_fix_required:
                    # fix nan and replace csv file
                    fix_nan_values(csv_file_path)
            # else:
            #     nan_fix_required = check_nan_fix_required(csv_file_path)
            #     if nan_fix_required:
            #         raise AssertionError("I only expected nan error in cup csvs..")

    def remove_tab_strings_from_df(self):
        log.info("remove tab strings from pd df")
        import_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in import_csv_info.csv_info:
            remove_tab_strings(csv_file_path)

    def convert_csv_data(self):
        """ convert csv data into desired format """
        log.info("convert_csv_data")
        import_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in import_csv_info.csv_info:
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
        log.info("clean csv_data")
        clean_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csv_info.csv_info:
            if csv_type == "league":
                league_cleaner = LeagueCsvCleaner(csv_file_path)
                league_cleaner.run()
            elif csv_type == "cup":
                cup_cleaner = CupCsvCleaner(csv_file_path)
                cup_cleaner.run()
            # TODO: enable also cup and player!
            # elif csv_type == "player":
            #     player_cleaner = PlayerCsvCleaner(csv_file_path)
            #     player_cleaner.run()

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
        self.copy_raw_to_import()  # replace '-' with '_'
        self.improve_imported_filename()  # replace '-' with '_'
        self.check_imported_filename()  # raises when filename incorrect
        self.correct_nan_in_csv()  # noqa only for cup csv: shifts empty cells in cup csv to right and adds 'NA' to empty cell
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

    def run(self):
        # self.do_scrape()  # scrap data (webpage --> raw)
        self.do_import()  # import raw data (raw --> import)
        self.do_clean()  # clean data (import --> clean)
        # self.do_enrich()  # enrich data (clean --> enrich)
        # self.do_ml()
        log.info("shutting down")
