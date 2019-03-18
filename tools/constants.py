from collections import namedtuple

COUNTRY_WHITE_LIST = ['ger', 'eng', 'por', 'esp', 'fra', 'ita', 'ned']


base_properties = ['csv_file_dir', 'csv_file_name', 'country', 'game_type',
                   'game_name', 'date', 'home', 'away', 'home_goals',
                   'away_goals', 'season', 'url', 'home_manager',
                   'away_manager', 'home_sheet', 'away_sheet']
league_properties = base_properties.append('play_round')





# csv variable information
CSV_VAR = namedtuple('CSV_VAR', ['name', 'not_null', 'desired_type', 'descr'])

CSV_FILE_DIR = CSV_VAR('csv_file_dir', True, '', 'absolute dir without filename')
CSV_FILE_NAME = CSV_VAR('csv_file_name', True, '', 'filename + extension')
COUNTRY = CSV_VAR('country', True, '', '3 chars abbreviation of country, e.g. "ger"')
DATE = CSV_VAR('date', True, 'datetime64[ns]', 'dd/mm/yyyy')
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

BASE_GAME_PROPERTIES = [CSV_FILE_DIR, CSV_FILE_NAME, COUNTRY, DATE]
LEAGUE_GAME_PROPERTIES = BASE_GAME_PROPERTIES

