from collections import namedtuple
from tools.constants import COUNTRY_WHITE_LIST, BASE_GAME_PROPERTIES, LEAGUE_GAME_PROPERTIES
import pandas as pd
import os



class RawCsvInfo:
    def __init__(self):
        self.raw_data_dir = '/work/raw_data/'
        self.raw_cup_dir = self.raw_data_dir + 'cup/'
        self.raw_league_dir = self.raw_data_dir + 'league/'
        self.raw_player_dir = self.raw_data_dir + 'player/'
        self._csv_info = []  # list with tuples

    @staticmethod
    def get_csvs_from_dir(folder_dir):
        csv_paths = [os.path.join(folder_dir, file) for file
                     in os.listdir(folder_dir) if file.endswith(".csv")]
        return csv_paths

    def update_csv_info(self):
        cup_csv_list = self.get_csvs_from_dir(self.raw_cup_dir)
        league_csv_list = self.get_csvs_from_dir(self.raw_league_dir)
        player_csv_list = self.get_csvs_from_dir(self.raw_player_dir)
        if cup_csv_list:
            self.add_to_csv_info(cup_csv_list, csv_type='cup')
        if league_csv_list:
            self.add_to_csv_info(league_csv_list, csv_type='league')
        if player_csv_list:
            self.add_to_csv_info(player_csv_list, csv_type='player')

    def add_to_csv_info(self, csv_list, csv_type=None):
        for csv in csv_list:
            self._csv_info.append((csv_type, csv))

    @property
    def csv_info(self):
        if self._csv_info:
            return self._csv_info
        self.update_csv_info()
        return self._csv_info


class GameBaseCsvAdapter:
    """each game csv goes trough a child class of GameBaseCsvAdapter.
    Class contains a lot of properties that must be retrieved from game csv
    Two game csv exists: cup, league """

    def __init__(self, csvfilepath):
        self.csv_file_path = csvfilepath
        self._csv_file_name = None
        self._country = None
        self._dataframe = None
        self._dataframe_copy = None
        self._date = None
        self._meta = BASE_GAME_PROPERTIES

    @property
    def csv_file_name(self):
        if self._csv_file_name:
            return self._csv_file_name
        self._csv_file_name = self.csv_file_path.split('/')[-1]
        return self._csv_file_name

    @property
    def dataframe(self):
        if self._dataframe:
            return self._dataframe
        self._dataframe = pd.read_csv(self.csv_file_path, sep='\t')
        return self._dataframe

    @property
    def dataframe_copy(self):
        if self._dataframe_copy:
            return self._dataframe_copy
        self._dataframe_copy = pd.read_csv(self.csv_file_path, sep='\t')
        return self._dataframe_copy

    def save_csv(self):
        # make sure each child class implements its own 'save_csv' method
        raise NotImplementedError

    @property
    def country(self):
        if self._country:
            return self._country
        self._country = self.csv_file_name[0:3]
        return self._country

    def check_country(self):
        """check if 4th char csvfilename is underscore,
        check if country in whitelist """
        assert self.csv_file_name[3] != '_', (
            '4th char of csv file', self.csv_file_name, 'name is not "_"')
        assert self.country in COUNTRY_WHITE_LIST, (
            'country', self.country, 'not in ', COUNTRY_WHITE_LIST)

    @property
    def date(self):
        if self._date:
            return self._date
        self._date = self.dataframe ['date']
        return self._date

    def check_date(self):
        pass

    def convert(self):

        print('hoi')


        self.check_country()
        self.check_country()

        csv_file_name = self.csv_file_name
        dataframe = self.dataframe
        dataframe_copy = self.dataframe_copy



        assert self.csv_file_name is not None
        assert self.dataframe is not None
        assert self.dataframe_copy is not None


    def check_date(self):
        # check dtype
        type = self.dataframe['data'].dtype
        assert type == self.date.csv_type

        # make sure it is dd/mm/yyyy
        date_format = 12


    def save_csv(self):
        # make sure each child class implements its own 'save_csv' method
        raise NotImplementedError

    # csv_file_dir
    # csv_file_name
    # country
    # game_type
    # game_name
    # date
    # home
    # away
    # home_goals
    # away_goals
    # season
    # url
    # home_manager
    # away_manager
    # home_sheet
    # away_sheet





class CupCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath, gametype='cup'):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.game_type = gametype

    def save_csv(self):
        pass




class LeagueCsvAdapter(GameBaseCsvAdapter):
    def __init__(self, csvfilepath, gametype='league'):
        GameBaseCsvAdapter.__init__(self, csvfilepath)
        self.game_type = gametype

    def save_csv(self):
        pass








    # @property
    # def home(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('home', not_null, csvtype)
    #
    # @property
    # def away(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('away', not_null, csvtype)
    #
    # @property
    # def home_goals(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('home_goals', not_null, csvtype)
    #
    # @property
    # def away_goals(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('away_goals', not_null, csvtype)
    #
    # @property
    # def season(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('season', not_null, csvtype)
    #
    # @property
    # def url(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('url', not_null, csvtype)
    #
    # @property
    # def home_manager(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('home_manager', not_null, csvtype)
    #
    # @property
    # def away_manager(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('away_manager', not_null, csvtype)
    #
    # @property
    # def home_sheet(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('home_sheet', not_null, csvtype)
    #
    # @property
    # def away_sheet(self, exists=True, not_null=True, csvtype='object'):
    #     return self.name_tuple('away_sheet', not_null, csvtype)
    #
    # def save_csv(self):
    #     # make sure each child class implements its own 'save_csv' method
    #     raise NotImplementedError




class CupCsvAdapter(GameBaseCsvAdapter):
    @property
    def score_45(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('score_45', not_null, csvtype)

    @property
    def score_90(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('score_90', not_null, csvtype)

    @property
    def score_105(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('score_105', not_null, csvtype)

    @property
    def score_120(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('score_120', not_null, csvtype)

    @property
    def score_aet(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('aet', not_null, csvtype)

    @property
    def score_pso(self, exists=True, not_null=True, csvtype='object'):
        return self.name_tuple('pso', not_null, csvtype)

    def save_csv(self):
        pass













# self.score_45_not_null = True
# self.score_45_type = 'object'
# self.score_exists = True
#
# self.score_90_not_null = True
# self.score_90_type = 'object'
# self.score_90_exists = True
#
# self.score_105_not_null = False
# self.score_105_type = 'object'
# self.score_105_exists = True
#
# self.score_120_not_null = False
# self.score_120_type = 'object'
# self.score_120_exists = True
#
# self.aet_not_null = True
# self.aet_type = 'bool'
# self._aet_exists = True
#
# self.pso_not_null = True
# self.pso_type = 'bool'
# self.pso_exists = True
#
