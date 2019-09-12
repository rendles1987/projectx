import pandas as pd
from tools.constants import (
    CLEAN_CSV_DIRS,
    IMPORT_CSV_DIRS,
    RAW_CSV_DIRS,
    SQLITE_TABLE_NAMES_UNICODE,
)
from tools.csv_cleaner.csv_cleaner import CupCsvCleaner
from tools.csv_cleaner.csv_cleaner import LeagueCsvCleaner

# from tools.csv_cleaner.repair_invalid_cleaned import CupCsvRepair, LeagueCsvRepair
from tools.csv_dir_info import CleanCsvInfo
from tools.csv_dir_info import ImportCsvInfo
from tools.csv_dir_info import RawCsvInfo
from tools.csv_importer.filename_checker import CupFilenameChecker
from tools.csv_importer.filename_checker import LeagueFilenameChecker
from tools.csv_importer.raw_csv_importer import check_nan_fix_required
from tools.csv_importer.raw_csv_importer import CupCsvImporter
from tools.csv_importer.raw_csv_importer import fix_nan_values
from tools.csv_importer.raw_csv_importer import get_df_fix_managers
from tools.csv_importer.raw_csv_importer import get_df_fix_players
from tools.csv_importer.raw_csv_importer import LeagueCsvImporter
from tools.csv_importer.raw_csv_importer import possible_fix_delimeter_type
from tools.csv_importer.raw_csv_importer import remove_tab_strings
from tools.csv_merger.csv_enricher import CupCsvEnricher
from tools.csv_merger.csv_enricher import LeagueCsvEnricher
from tools.csv_merger.csv_merger import MergeCsvToSqlite
from tools.sqlite_teams.club_stats import TeamStatsLongTerm, TeamStatsShortTerm
from tools.sqlite_teams.teams_unique import TeamsUnique
from tools.sqlite_teams.teams_unique import UpdateGamesWithIds
from tools.utils import df_to_sqlite_table, sqlite_table_to_df, is_panda_df_empty
import numpy as np
from tools.scraper.scrape_all_players import UnicodeScraper
from tools.csv_cleaner.repair_invalid_cleaned import UpdateNamesGameCsv
from tools.scraper.scrape_all_players import JumpToNextRow
from tools.utils import drop_duplicates_in_csv

import logging
import os
import shutil
import time

log = logging.getLogger(__name__)

from sys import getsizeof
import inspect


def retrieve_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var]


def log_size_of_variable(my_var, my_var_string):
    my_var_name = my_var_string  # retrieve_name(my_var)
    my_var_size_b = getsizeof(my_var)
    my_var_size_mb = getsizeof(my_var) / (1000 * 1000)
    log.info(f"variable {my_var_name}: {my_var_size_mb} mbytes")


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
    def check_clean_data_exists():
        log.info("check if 1 or more clean data files exists")
        clean_csv_info = CleanCsvInfo()
        assert len(clean_csv_info.csv_info) > 0

    @staticmethod
    def check_valid_import_data_exists():
        log.info("check if 1 or more valid import data files exists")
        import_csv_info = ImportCsvInfo()
        assert import_csv_info.count_total_valid_csv() > 0

    @staticmethod
    def check_invalid_clean_data_exists():
        log.info("check if 1 or more invalid clean data files exists")
        clean_csv_info = CleanCsvInfo()
        nr_invalid_clean_csvs = clean_csv_info.count_total_invalid_csv()
        log.info(f"nr_invalid_clean_csvs found: {nr_invalid_clean_csvs}")
        return nr_invalid_clean_csvs > 0

    @staticmethod
    def repair_invalid_clean_data():
        log.info("repairing invalid clean data")
        clean_csv_info = CleanCsvInfo()
        for csv_type, full_path in clean_csv_info.get_all_invalid_csv():
            if csv_type == "cup":
                repair = CupCsvRepair(csv_type, full_path)
                repair.run()
            elif csv_type == "league":
                repair = LeagueCsvRepair(csv_type, full_path)
                repair.run()
            else:
                raise AssertionError(f"{full_path} it not a cup nor a league csv..")

    @staticmethod
    def replace_managers_sheets_in_clean_dir_csvs():
        log.info("replacing managers and sheets in just copied csvs to clean dir")
        clean_csv_info = CleanCsvInfo()

        # first all valid csvs
        for csv_type, full_path in clean_csv_info.get_all_csv():
            update_names = UpdateNamesGameCsv(csv_type, full_path)
            update_names.run()

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

    def correct_delimeter_type(self):
        """Correct .csv delimeter type eventual from comma to tab"""
        import_csvs = ImportCsvInfo()
        for csv_type, csv_file_path in import_csvs.csv_info:
            possible_fix_delimeter_type(csv_type, csv_file_path)

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

    def fix_player_sheets(self):
        """ change to np.nan if '[]'"""
        log.info("fix players sheets")
        import_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in import_csv_info.csv_info:
            if csv_type in ["cup", "league"]:
                df = get_df_fix_players(csv_file_path, df=None)
                df_to_csv(df, csv_file_path)  # replace if exists

    def fix_managers(self):
        """ change to np.nan if '[]'"""
        log.info("fix managers")
        import_csv_info = ImportCsvInfo()
        for csv_type, csv_file_path in import_csv_info.csv_info:
            if csv_type in ["cup", "league"]:
                df = get_df_fix_managers(csv_file_path)
                df_to_csv(df, csv_file_path)  # replace if exists

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
            if csv_type == "cup":
                cup_importer = CupCsvImporter(csv_file_path)
                cup_importer.run()

    def clean(self):
        log.info("clean csv_data")
        clean_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csv_info.csv_info:
            log.info(f"clean csv_data {csv_file_path}")
            if csv_type == "league":
                league_cleaner = LeagueCsvCleaner(csv_file_path)
                league_cleaner.run()
            elif csv_type == "cup":
                cup_cleaner = CupCsvCleaner(csv_file_path)
                cup_cleaner.run()

    def __iter_trough_nans(self, df, df_nan):
        # log_size_of_variable(df, 'df')
        # log_size_of_variable(df_nan, 'df_nan')
        scrape_count = 0
        # row_ids = []
        for index, row in df_nan.iterrows():
            scrape_count += 1
            scraper = UnicodeScraper(row["url"])
            if not scraper.can_read_html():
                log.error(f"cannot read html {str(row['url'])}")
                continue
            try:
                if scraper.home_sheet is not np.nan:
                    df["home_sheet"][index] = scraper.home_sheet
                    df["away_sheet"][index] = scraper.away_sheet
                    df["home_subs"][index] = scraper.home_subs
                    df["away_subs"][index] = scraper.away_subs
                df["checked_nan_sheets"][index] = 1
                # row_ids.append(index)
            except JumpToNextRow:
                continue
            # sum_nans = sum(df["checked_nan_sheets"])
            # log.info(f"sum check_nan_sheets: {str(sum_nans)}")
            if scrape_count == 5:
                # df["checked_nan_sheets"][row_ids] = 1
                self.rows_to_sqlite_and_start_next_phase(df)

    def rows_to_sqlite_and_start_next_phase(self, df):
        df_to_sqlite_table(
            df, table_name=SQLITE_TABLE_NAMES_UNICODE, if_exists="replace"
        )
        df, df_nan = self.not_first_phase_df()
        if not is_panda_df_empty(df_nan):
            self.__iter_trough_nans(df, df_nan)
        else:
            log.info(f"no more nan rows")

    def first_phase_df(self):
        """Column "checked_nan_sheets" is set to False"""
        df = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
        # df["checked_nan_sheets"] = 0

        df_nan = df[
            (df["home_sheet"].isna())
            & (df["away_sheet"].isna())
            & (~df["url"].isna() & (df["checked_nan_sheets"] == 0))
        ]
        log.info(f"number of rows with nan sheets to be checked: {len(df_nan)}")
        return df, df_nan

    def not_first_phase_df(self):
        """Column "checked_nan_sheets" is not updated"""
        df = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
        df_nan = df[
            (df["home_sheet"].isna())
            & (df["away_sheet"].isna())
            & (~df["url"].isna() & (df["checked_nan_sheets"] == 0))
        ]
        log.info(f"number of rows with nan sheets to be checked: {len(df_nan)}")
        return df, df_nan

    def faaaack_temp_fix_nans_in_sheet(self):
        df, df_nan = self.first_phase_df()
        self.__iter_trough_nans(df, df_nan)

    @staticmethod
    def remove_duplicate_rows_all_clean_csv():
        clean_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csv_info.csv_info:
            drop_duplicates_in_csv(csv_file_path, keep="first")

    @staticmethod
    def get_all_player_names_per_game_and_store_them():
        log.info("fix all player names")

        df = sqlite_table_to_df(table_name=SQLITE_TABLE_NAMES_UNICODE)
        log.info(f"nr rows before dropping duplicates: {str(len(df))}")
        time.sleep(1)
        # games are unique by combination of these columns
        unique_column_set = ["date", "home", "away"]
        df.drop_duplicates(subset=unique_column_set, keep="first", inplace=True)
        log.info(f"nr rows after dropping duplicates: {str(len(df))}")
        time.sleep(1)
        df_to_sqlite_table(
            df, table_name=SQLITE_TABLE_NAMES_UNICODE, if_exists="replace"
        )

        from tools.scraper.scrape_all_players import UnicodeScraperController

        clean_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csv_info.csv_info:
            sheet_fixer = UnicodeScraperController(csv_type, csv_file_path)
            sheet_fixer.run()

    def enrich(self):
        log.info("enrich clean csv_data")
        clean_csv_info = CleanCsvInfo()
        for csv_type, csv_file_path in clean_csv_info.csv_info:
            if csv_type == "league":
                league_enricher = LeagueCsvEnricher(csv_file_path)
                league_enricher.run()
            elif csv_type == "cup":
                league_enricher = CupCsvEnricher(csv_file_path)
                league_enricher.run()

    def merge(self):
        log.info("merge clean csv_data")
        clean_csv_info = CleanCsvInfo()
        merger = MergeCsvToSqlite(clean_csv_info)
        merger.run()

    def create_unique_teams(self):
        teams_unique = TeamsUnique()
        teams_unique.run()

    def update_games_with_ids(self):
        update = UpdateGamesWithIds()
        update.run()

    def calculate_club_stats(sefl):
        team_stats_long_term = TeamStatsLongTerm()
        team_stats_long_term.run()
        team_stats_short_term = TeamStatsShortTerm()
        team_stats_short_term.run()

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
        self.correct_delimeter_type()
        self.correct_nan_in_csv()  # noqa only for cup csv: shifts empty cells in cup csv to right and adds 'NA' to empty cell
        self.fix_player_sheets()
        self.fix_managers()
        self.remove_tab_strings_from_df()  # noqa replace strings like '\t' with '', overwrites existing raw_csv
        self.convert_csv_data()

    def add_manual_repaired_to_valid(self):
        """ import dir has been filled with _invalid and _valid.
        I checked manually each game in a _invalid.csv and fixed them in _repaired.csv
        Now, lets add rows in _repaired to _valid.csv
        Next step is copy _valid from import to clean dir
        """
        import os
        import pandas as pd
        from tools.constants import MANUAL_FIX_INVALID_IMPORT_DIR as manual_dir
        from tools.utils import df_to_csv

        assert os.path.isdir(manual_dir)
        # [(filepath, filename), (filepath, filename), etc]
        repaired_csv_paths = [
            file.rsplit("_repaired.csv")[0]
            for file in os.listdir(manual_dir)
            if file.endswith("_repaired.csv")
        ]

        # find matching _valid (if match, add rows to that _valid.csv)
        import_csv_info = ImportCsvInfo()
        for csv_type, valid_csv_file_path in import_csv_info.get_valid_csv():
            file_name = valid_csv_file_path.rsplit("/")[-1].rsplit("_valid.csv")[0]
            if file_name in repaired_csv_paths:
                index = repaired_csv_paths.index(file_name)
                repaired_csv_paths.pop(index)
                repaired_full_path = (
                    os.path.join(manual_dir, file_name) + "_repaired.csv"
                )
                df_repaired = pd.read_csv(repaired_full_path, sep="\t")
                df_valid = pd.read_csv(valid_csv_file_path, sep="\t")

                default_col = [
                    "date",
                    "home",
                    "away",
                    "score",
                    "home_goals",
                    "away_goals",
                    "url",
                    "home_manager",
                    "away_manager",
                    "home_sheet",
                    "away_sheet",
                ]
                if csv_type == "league":
                    desired_col = default_col
                elif csv_type == "cup":
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
                else:
                    raise
                df_repaired_select_col = df_repaired[desired_col]

                len_before_append = len(df_valid)
                df_valid = df_valid.append(df_repaired_select_col)
                len_after_append = len(df_valid)
                if len_before_append != len_after_append:
                    diff = len_after_append - len_before_append
                    log.info(f"appended {diff} rows")

                len_before_drop = len(df_valid)
                df_valid.drop_duplicates()
                len_after_drop = len(df_valid)
                if len_before_drop != len_after_drop:
                    diff = len_before_drop - len_after_drop
                    log.info(f"dropped {diff} rows")
                df_to_csv(df_valid, valid_csv_file_path)  # overwriting file
        assert len(repaired_csv_paths) == 0
        # TODO: maybe there was no _valid created (since no rows were valid). An
        #  _invalid should exist. If not raise, if so then create an _valid.

    def do_clean(self):
        # self.check_dirs_exist(self.import_dirs, self.clean_dirs)  # raise if not exists
        # self.check_valid_import_data_exists()  # raises when not exists
        # self.empty_clean_dirs()  # empties dir when not empty

        # this is tmp stuff
        # self.add_manual_repaired_to_valid()

        # not tmp
        # self.copy_valid_import_to_clean()

        # this is tmp stuff
        # loop through clean dir, scrape, and store in sqlite
        # self.remove_duplicate_rows_all_clean_csv()
        self.get_all_player_names_per_game_and_store_them()
        # self.faaaack_temp_fix_nans_in_sheet()

        # update all csvs in clean dir and update managers and sheets
        self.replace_managers_sheets_in_clean_dir_csvs()
        ##### THIS IS TEMP #####
        # self.clean()

    def do_repair_invalid_clean(self):
        """repair all invalid csvs in folder 'clean' (as a result of 'do_clean()'"""
        self.check_dirs_exist(self.clean_dirs)  # raise if not exists
        if self.check_invalid_clean_data_exists():
            self.repair_invalid_clean_data()

    def do_merge(self):
        """Before we merge all csvs to sqlite, we add columns 1) game_type,
        2) game_name, 3) season """
        self.enrich()
        self.merge()

    def determine_teams(self):
        """ from here all happens in sqlite"""
        # self.create_unique_teams()
        # self.update_games_with_ids()
        self.calculate_club_stats()

    def do_ml(self):
        self.run_ml()

    def run(self):
        # self.do_scrape()  # scrap data (webpage --> raw)
        # self.do_import()  # import raw data (raw --> import)
        self.do_clean()  # clean data (import --> clean)
        # self.do_repair_invalid_clean()
        # self.do_merge()  # add 2 or 3 columns to clean and then merge to sqlite
        # self.determine_teams()
        # self.do_ml()
        log.info("shutting down")
