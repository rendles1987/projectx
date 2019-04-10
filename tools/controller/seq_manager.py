from tools.csv_adapter.raw_csv_adapater import CupCsvAdapter, LeagueCsvAdapter
from tools.csv_adapter.helpers import RawCsvInfo
from tools.logging import log


class ProcessController:
    def __init__(self):
        pass

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

    def convert_csv_data(self):
        """
        1. put all csvs in the right dirs! see: tools.constants.py RAW_CSV_DIRS
        2. empty output folder
        3. convert raw csv into desired format
        """
        raw_csv_info = RawCsvInfo()
        for csv_type, csv_file_path in raw_csv_info.csv_info:
            if csv_type == "league":
                league_adapter = LeagueCsvAdapter(csv_file_path)
                league_adapter.run()
            # TODO: enable also cup and player!
            # elif csv_type == 'cup':
            #     cup_adapter = CupCsvAdapter(csv_file_path)
            #     cup_adapter.run()
        log.info("shutting down")

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
