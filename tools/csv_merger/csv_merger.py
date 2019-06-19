import os
import sqlite3
import logging
import pandas as pd
from tools.constants import MERGE_CSV_DIR

log = logging.getLogger(__name__)

SQLITE_NAME = "my_sqlite.db"
SQLITE_FULL_PATH = MERGE_CSV_DIR + SQLITE_NAME


class MergeCsvToSqlite:
    def __init__(self, clean_csv_info):
        log.info(f"start {self.__class__.__name__}")
        self.clean_csv_info = clean_csv_info

    def run(self):
        if os.path.isfile(SQLITE_FULL_PATH):
            log.debug(f"deleting {SQLITE_FULL_PATH}")
            os.remove(SQLITE_FULL_PATH)
        """ for now, we always create new sqlite with tables: cup, league, player """
        # Opens file if exists, else creates file
        log.debug(f"connect to {SQLITE_FULL_PATH}")
        connex = sqlite3.connect(SQLITE_FULL_PATH)
        # This object lets us actually send messages to our DB and receive results
        # cur = connex.cursor()

        for csv_type, csv_file_path in self.clean_csv_info.csv_info:
            # csv_type becomes name of the table in sqlite ('cup', 'league', 'player')
            if csv_file_path.endswith("_invalid.csv"):
                continue
            df = pd.read_csv(csv_file_path, sep="\t")
            df.to_sql(name=csv_type, con=connex, if_exists="append", index=False)
