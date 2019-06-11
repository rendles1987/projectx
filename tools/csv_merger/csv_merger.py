import os
import sqlite3

import pandas as pd
from tools.constants import MERGE_CSV_DIR


class MergeCsvToSqlite:
    def __init__(self, clean_csv_info):
        self.clean_csv_info = clean_csv_info
        self.sqlite_name = "my_sqlite.db"
        self.sqlite_full_path = MERGE_CSV_DIR + self.sqlite_name

    def run(self):
        if os.path.isfile(self.sqlite_full_path):
            os.remove(self.sqlite_full_path)
        """ for now, we always create new sqlite with tables: cup, league, player """
        # Opens file if exists, else creates file
        connex = sqlite3.connect(self.sqlite_full_path)
        # This object lets us actually send messages to our DB and receive results
        # cur = connex.cursor()

        for csv_type, csv_file_path in self.clean_csv_info.csv_info:
            # csv_type becomes name of the table in sqlite ('cup', 'league', 'player')
            if csv_file_path.endswith("_invalid.csv"):
                continue
            df = pd.read_csv(csv_file_path, sep="\t")
            df.to_sql(name=csv_type, con=connex, if_exists="append", index=False)
