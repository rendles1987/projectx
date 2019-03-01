from projectx.scrape.league import NedLeague


class ProcessController:
    def __init__(self):

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
        print(NedLeague.scraper_type)

    def start_collect(self):
        pass

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
        self.start_scrape()
        self.start_collect()
        self.start_check_collect()
        self.start_clean_collect()
        self.data_adaption_1()
        self.save_data_1()

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

        # projectx/raw_data/ is added to .gitignore
        raw_data_dir = '/raw_data/'
        fname = raw_data_dir + 'abc.txt'

        with open(fname) as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        print(content)
