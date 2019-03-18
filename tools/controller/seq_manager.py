from tools.scrape.league import NedLeague
import pandas as pd
import os
from tools.adapt_raw_csv import RawCsvInfo
from tools.adapt_raw_csv import LeagueCsvAdapter #, CupCsvAdapter, PlayerCsvAdapter



class ProcessController:
    def __init__(self):
        self.raw_csvs = None
        self.raw_csv_info = RawCsvInfo()



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
        1. put all csvs in the right dirs ( - '/work/raw_data/cup'
                                            - '/work/raw_data/league'
                                            - '/work/raw_data/player'
        2. empty output folder
        3. convert raw csv into desired format
        """
        for csv_type, csv_file_path in self.raw_csv_info.csv_info:
            if csv_type == 'league':
                data_adapter = LeagueCsvAdapter(csv_file_path)
            else:
                pass
            data_adapter.convert()
            data_adapter.save()

            #     for data_property in data_adapter._meta:
            #         print('name: ', data_property.name,
            #               'desired_type: ', data_property.desired_type,
            #               'description: ', data_property.descr)
            #
            #     data_adapter._meta
            # else:
            #     pass

            # elif csv_type == 'cup':
            #     data_adapter = CupCsvAdapter(csv_file_path)
            # elif csv_type == 'player':
            #     data_adapter = PlayerCsvAdapter(csv_file_path)
            # else:
            #     raise Exception('unknown csv_type: ', csv_type)


        print('hoi')




        """
        #
        # for
        #
        # # dir_path = os.path.dirname(os.path.realpath(__file__))
        # # raw_data_dir = '/opt/project/raw_data/'
        #
        # csv_paths = [os.path.join(self.raw_cup_dir, file) for file
        #              in os.listdir(self.raw_cup_dir) if file.endswith(".csv")]
        #
        # # check1 (csv directory exists?)
        # if not csv_paths:
        #     msg = 'raw_cup_dir:', self.raw_cup_dir, ' has no .csv'
        #     raise AssertionError(msg)
        # # check2 (csvs exists)
        # for csv_path in csv_paths:
        #     if not os.path.exists(csv_path):
        #         raise AssertionError('csv_path', csv_path, 'does not exists')
        #
        # for csv_path in csv_paths:
        #
        # print('hello')
        #

        #
        # class CheckRawCsv:
        #     def __init__(self, csv_type, csv_dir):
        #         self.csv_type = csv_type
        #         self.csv_dir = csv_dir
        #
        #
        #     def csv_to_panda(self):
        #
        #
        #     def get_csv_expectations(self):
        #         pass
        #
        #
        # raw_data_dir = '/work/raw_data/'
        # raw_cup_dir = raw_data_dir + 'cup/'
        #
        # Cup_CheckRawCsv = CheckRawCsv('cup', raw_cup_dir)
        #
        #







        #
        #
        #
        #
        #                         league_checks = csv_checks('league_checks',
        #                           True, 'datetime64[ns]',
        #                           True, 'object')
        #
        # print(cup_checks.name, cup_checks.home_not_null, cup_checks.home_type)
        # print(league_checks.name)
        #
        """

        """
        >>> csv_path = '/work/raw_data/cup/all_games_ger_supercup_copy.csv'
        >>> data = pd.read_csv(csv_path, sep='\t')
        
        >>> data['home'].dtype
        dtype('O')
        
        >>> data['home'].dtype == cup_checks.home_type
        True
        """
        
        
        





        # check if


        # cup_column_dtype = [('index', 'int64'),
        #                     ('date', '<M8[ns]'),   # datetime64[ns]
        #                     ('home', object),
        #                     ('away', object),
        #                     ('score', object),
        #                     ('round_text', object),
        #                     ('play_round', 'int64'),
        #                     ('score_45', object),
        #                     ('score_90', object),
        #                     ('score_105', object),
        #                     ('score_120', object),
        #                     ('aet', bool),
        #                     ('pso', bool),
        #                     ('home_goals', object),
        #                     ('away_goals', object),
        #                     ('season', object),
        #                     ('url', object),
        #                     ('home_manager', object),
        #                     ('away_manager', object),
        #                     ('home_sheet', object),
        #                     ('away_sheet', object),
        #                     ]
        #
        # csv_path = '/work/raw_data/cup/all_games_ger_supercup_copy.csv'
        # data = pd.read_csv(csv_path, sep='\t')
        #
        # csv_path = '/work/raw_data/cup/all_games_ger_supercup_copy.csv'
        # data = pd.read_csv(csv_path, sep='\t')
        #
        #
        # for column_dtype in cup_column_dtype:
        #     column = column_dtype[0]
        #     dtype = column_dtype[1]
        #     if column not in data.columns:
        #         raise AssertionError('we expect column in csv, but is doesnt '
        #                              'exists')
        #     if dtype != data[column].dtype:
        #         raise AssertionError('unexpected datatype of column', column)
        #
        #
        # # date
        # # convert column dtype to date
        # if data['date'].dtype != '<M8[ns]':
        #     try:
        #         data_good_dtype = pd.to_datetime(data['date'], format='%d%b%Y')
        #     except:
        #         print('could not covert to date format')
        #
        # #
        # # convert column dtype to date
        # if data['date'].dtype != '<M8[ns]':
        #     try:
        #         data_good_dtype = pd.to_datetime(data['date'], format='%d%b%Y')
        #     except:
        #         print('could not covert to date format')




        #
        # # check 1
        # check eck if home is str
        # # check if home is str
        #
        # if  in df.columns:
        #



        # with open(fname) as f:
        #     content = f.readlines()
        # content = [x.strip() for x in content]
        # print(content)



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

