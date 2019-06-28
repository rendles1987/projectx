import logging
import os
import sqlite3

import pandas as pd
from tools.constants import SQLITE_FULL_PATH, TABLE_NAME_ALL_GAMES
from tools.utils import (
    df_to_sqlite_table,
    drop_table_if_exists_from_sqlite,
    sqlite_table_to_df,
)

log = logging.getLogger(__name__)


class MergeCsvToSqlite:
    def __init__(self, clean_csv_info):
        log.info(f"start {self.__class__.__name__}")
        self.clean_csv_info = clean_csv_info

    def merge_game_csv_to_sqlite(self, csv_file_path, table_name=None):
        assert table_name in ["league", "cup"]
        log.info(f"merging {csv_file_path}")
        df = pd.read_csv(csv_file_path, sep="\t")
        df_to_sqlite_table(df, table_name=table_name, if_exists="append")

    def merge_two_sqlite_tables(self):
        df_cup = sqlite_table_to_df(table_name="cup")
        df_league = sqlite_table_to_df(table_name="league")
        all_games = pd.concat(
            [df_cup, df_league], ignore_index=False, axis=0, sort=False
        )
        all_games.set_index("date")
        all_games.sort_values(by="date", ascending=True)
        df_to_sqlite_table(
            all_games, table_name=TABLE_NAME_ALL_GAMES, if_exists="replace"
        )

    def drop_both_seperate_tables(self):
        drop_table_if_exists_from_sqlite(table_name="cup")
        drop_table_if_exists_from_sqlite(table_name="league")

    def run(self):
        if os.path.isfile(SQLITE_FULL_PATH):
            log.debug(f"deleting {SQLITE_FULL_PATH}")
            os.remove(SQLITE_FULL_PATH)
        """ for now, we always create new sqlite with tables: cup, league, player """
        # Opens file if exists, else creates file
        log.debug(f"connect to {SQLITE_FULL_PATH}")
        self.connex = sqlite3.connect(SQLITE_FULL_PATH)
        for csv_type, csv_file_path in self.clean_csv_info.csv_info:
            if not csv_file_path.endswith("_invalid.csv"):
                self.merge_game_csv_to_sqlite(csv_file_path, table_name=csv_type)
        self.merge_two_sqlite_tables()
        self.drop_both_seperate_tables()
