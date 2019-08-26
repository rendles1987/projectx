from collections import defaultdict
from collections import namedtuple


DATEFORMAT_YYYYMMDD = "%Y-%m-%d"

# -------- sqlite table names ----------
TABLE_NAME_ALL_TEAMS = "team_id_name_country"
TABLE_NAME_ALL_GAMES = "all_games"
TABLE_NAME_ALL_GAMES_WITH_IDS = "all_games_ids"
TABLE_NAME_LONG_TERM_STATS = "team_long_term_stats"

use_temp = True
if use_temp:
    RAW_CSV_DIRS = {
        "cup": "/work/data/temp_stuff/_01_raw/cup",
        "league": "/work/data/temp_stuff/_01_raw/league",
        "player": "/work/data/temp_stuff/_01_raw/player",
    }

    IMPORT_CSV_DIRS = {
        "cup": "/work/data/temp_stuff/_02_import/cup",
        "league": "/work/data/temp_stuff/_02_import/league",
        "player": "/work/data/temp_stuff/_02_import/player",
    }

    CLEAN_CSV_DIRS = {
        "cup": "/work/data/temp_stuff/_03_clean/cup",
        "league": "/work/data/temp_stuff/_03_clean/league",
        "player": "/work/data/temp_stuff/_03_clean/player",
    }

    MERGE_CSV_DIR = "/work/data/temp_stuff/_04_merge/"

    SQLITE_NAME = "my_sqlite_temp_stuff.db"
    SQLITE_FULL_PATH = MERGE_CSV_DIR + SQLITE_NAME

else:
    RAW_CSV_DIRS = {
        "cup": "/work/data/_01_raw/cup",
        "league": "/work/data/_01_raw/league",
        "player": "/work/data/_01_raw/player",
    }

    IMPORT_CSV_DIRS = {
        "cup": "/work/data/_02_import/cup",
        "league": "/work/data/_02_import/league",
        "player": "/work/data/_02_import/player",
    }

    CLEAN_CSV_DIRS = {
        "cup": "/work/data/_03_clean/cup",
        "league": "/work/data/_03_clean/league",
        "player": "/work/data/_03_clean/player",
    }

    MERGE_CSV_DIR = "/work/data/_04_merge/"

    SQLITE_NAME = "my_sqlite.db"
    SQLITE_FULL_PATH = MERGE_CSV_DIR + SQLITE_NAME

TAB_DELIMETER = "\t"
COMMA_DELIMETER = ","


TEMP_DIR = "/work/data/tmp"

fields = ["id", "name", "type", "country", "level"]
game_info = namedtuple("game_named_tup", fields)
# ---- competitions ----------------
ENG_PREMIER_LEAGUE = game_info(1, "premier_league", "league", "eng", 1)
ENG_CHAMPIONSHIP = game_info(2, "championship", "league", "eng", 2)
ENG_LEAGUE_ONE = game_info(3, "league_one", "league", "eng", 3)
ENG_LEAGUE_TWO = game_info(4, "league_two", "league", "eng", 4)
ESP_PRIMERA_DIVISION = game_info(5, "primera_division", "league", "esp", 1)
ESP_SEGUNDA_DIVISION = game_info(6, "segunda_division", "league", "esp", 2)
FRA_LIGUE_1 = game_info(7, "ligue_1", "league", "fra", 1)
FRA_LIGUE_2 = game_info(8, "ligue_2", "league", "fra", 2)
GER_BUNDESLIGA = game_info(9, "bundesliga", "league", "ger", 1)
GER_BUNDESLIGA_2 = game_info(10, "2_bundesliga", "league", "ger", 2)
ITA_SERIE_A = game_info(11, "serie_a", "league", "ita", 1)
ITA_SERIE_B = game_info(12, "serie_b", "league", "ita", 2)
NED_EREDIVISIE = game_info(13, "eredivisie", "league", "ned", 1)
POR_PRIMEIRA_LIGA = game_info(14, "primeira_liga", "league", "por", 1)
# ---- cups ----------------
ENG_FA_CUP = game_info(15, "fa_cup", "cup", "eng", 99)
ENG_LEAGUE_CUP = game_info(16, "league_cup", "cup", "eng", 99)
ENG_COMMUNITY_SHIELD = game_info(17, "community_shield", "cup", "eng", 99)
ENG_PLAYOFF_CHAMPIONSHIP = game_info(18, "playoff_championship", "cup", "eng", 99)
ESP_COPA_DEL_REY = game_info(19, "copa_del_rey", "cup", "esp", 99)
ESP_SUPERCOPA = game_info(20, "supercopa", "cup", "esp", 99)
FRA_COUPE_DE_FRANCE = game_info(21, "coupe_de_france", "cup", "fra", 99)
FRA_COUPE_DE_LA_LIGUE = game_info(22, "coupe_de_la_ligue", "cup", "fra", 99)
FRA_TROPHEE_DES_CHAMPIONS = game_info(23, "trophee_des_champions", "cup", "fra", 99)
GER_DFB_POKAL = game_info(24, "dfb_pokal", "cup", "ger", 99)
GER_LIGAPOKAL = game_info(25, "ligapokal", "cup", "ger", 99)
GER_SUPERCUP = game_info(26, "supercup", "cup", "ger", 99)
ITA_COPPA_ITALIA = game_info(27, "coppa_italia", "cup", "ita", 99)
ITA_SUPERCOPPA = game_info(28, "supercoppa", "cup", "ita", 99)
NED_KNVB_BEKER = game_info(29, "knvb_beker", "cup", "ned", 99)
POR_SUPERTACA = game_info(30, "supertaca", "cup", "por", 99)
POR_TACA = game_info(31, "taca", "cup", "por", 99)
POR_TACA_DA_LIGA = game_info(32, "taca_da_liga", "cup", "por", 99)
EUR_EUROPA_LEAGUE = game_info(33, "europa_league", "cup", "eur", 99)
EUR_EUROPA_LEAGUE_QUAL = game_info(34, "europa_league_qual", "cup", "eur", 99)
EUR_CHAMPIONS_LEAGUE = game_info(35, "champions_league", "cup", "eur", 99)
EUR_CHAMPIONS_LEAGUE_QUAL = game_info(36, "champions_league_qual", "cup", "eur", 99)

GAMES = [
    ENG_PREMIER_LEAGUE,
    ENG_CHAMPIONSHIP,
    ENG_LEAGUE_ONE,
    ENG_LEAGUE_TWO,
    ESP_PRIMERA_DIVISION,
    ESP_SEGUNDA_DIVISION,
    FRA_LIGUE_1,
    FRA_LIGUE_2,
    GER_BUNDESLIGA,
    GER_BUNDESLIGA_2,
    ITA_SERIE_A,
    ITA_SERIE_B,
    NED_EREDIVISIE,
    POR_PRIMEIRA_LIGA,
    ENG_FA_CUP,
    ENG_LEAGUE_CUP,
    ENG_COMMUNITY_SHIELD,
    ENG_PLAYOFF_CHAMPIONSHIP,
    ESP_COPA_DEL_REY,
    ESP_SUPERCOPA,
    FRA_COUPE_DE_FRANCE,
    FRA_COUPE_DE_LA_LIGUE,
    FRA_TROPHEE_DES_CHAMPIONS,
    GER_DFB_POKAL,
    GER_LIGAPOKAL,
    GER_SUPERCUP,
    ITA_COPPA_ITALIA,
    ITA_SUPERCOPPA,
    NED_KNVB_BEKER,
    POR_SUPERTACA,
    POR_TACA,
    POR_TACA_DA_LIGA,
    EUR_EUROPA_LEAGUE,
    EUR_EUROPA_LEAGUE_QUAL,
    EUR_CHAMPIONS_LEAGUE,
    EUR_CHAMPIONS_LEAGUE_QUAL,
]

ALL_COUNTIRES = list(set([x.country for x in GAMES]))
CUP_COUNTRIES = list(set([x.country for x in GAMES if x.type == "cup"]))
LEAGUE_COUNTRIES = list(set([x.country for x in GAMES if x.type == "league"]))

ALL_GAMENAME_ID_MAPPING = {x.name: x.id for x in GAMES}
# {'premier_league': 1, 'championship': 2, ..etc}
ALL_GAMEID_NAME_MAPPING = {x.id: x.name for x in GAMES}
# {1: 'premier_league', 2: 'championship', ..etc}
ALL_GAMEID_COUNTRY_MAPPING = {x.id: x.country for x in GAMES}
# {1: 'eng', 2: 'eng', ..etc}
GAMENAME_ID_LEAGUE = {x.id for x in GAMES if x.type == "league"}
GAMENAME_ID_CUP = {x.id for x in GAMES if x.type == "cup"}
# {1: 'league', 2: 'league', ..etc} <-- 1 is 'premier league', 2 is championship

GAMETYPE_ID_MAPPING = {"league": 1, "cup": 2}
COUNTRY_ID_MAPPING = {
    "ned": 1,
    "ita": 2,
    "fra": 3,
    "eng": 4,
    "por": 5,
    "esp": 6,
    "ger": 7,
    "eur": 8,
}
assert sorted(COUNTRY_ID_MAPPING.keys()) == sorted(ALL_COUNTIRES)


# check no double game ids
assert len(ALL_GAMENAME_ID_MAPPING.values()) == len(
    set(ALL_GAMENAME_ID_MAPPING.values())
)

ALL_GAMENAME_COUNTRY_MAPPING = {x.name: x.country for x in GAMES}
CUP_GAMENAME_COUNTRY_MAPPING = {x.name: x.country for x in GAMES if x.type == "cup"}
LEAGUE_GAMENAME_COUNTRY_MAPPING = {
    x.name: x.country for x in GAMES if x.type == "league"
}

ALL_GAMEID_COUNTRY_MAPPING = {game.id: game.country for game in GAMES}
ALL_GAMEID_LEVEL_MAPPING = {game.id: game.level for game in GAMES}

CUP_COUNTRY_GAMENAMES_MAPPING = defaultdict(list)
for name, country in CUP_GAMENAME_COUNTRY_MAPPING.items():
    CUP_COUNTRY_GAMENAMES_MAPPING[country].append(name)
#  {'eng': ['league_cup', 'community_shield', 'playoff_championship'], 'esp': ['c

LEAGUE_COUNTRY_GAMENAMES_MAPPING = defaultdict(list)
for name, country in LEAGUE_GAMENAME_COUNTRY_MAPPING.items():
    LEAGUE_COUNTRY_GAMENAMES_MAPPING[country].append(name)
#  {'eng': ['premier_league', 'championship', 'league_two'], 'esp': ['pri ...


# for checking if play date is in "logic" season range
# start date of season xxxx = 1st of July --> 01/07/xxxx
# end date of season xxxx = 1st of July --> 01/07/xxxx+1
# TODO: now all competitions are expected to have same start and end of season.
#  Perhaps, we should define this per competition
season_window_named_tup = namedtuple(
    "season_window_named_tup", ["month_start", "month_end", "day_start", "day_end"]
)
SEASON_WINDOW = season_window_named_tup(6, 7, 1, 1)


# for checking if nr goals makes sense
fields = ["cup_max_goals", "league_max_goals"]
game_specs_named_tup = namedtuple("game_specs_named_tup", fields)
GAME_SPECS = game_specs_named_tup(20, 10)


# csv variable information
csv_propinfo = namedtuple(
    "csv_propinfo",
    ["name", "is_column_name", "source", "strip_whitespace", "desired_type", "descr"],
)


# NOT COLUMN NAMES
CSV_FILE_DIR = csv_propinfo(
    "csv_file_dir", False, "filedir", False, "string", "absolute dir without filename"
)
GAME_TYPE = csv_propinfo(
    "game_type", False, "filedir", False, "string", "cup of league"
)
CSV_FILE_NAME = csv_propinfo(
    "csv_file_name", False, "filename", False, "string", "filename + extension"
)
COUNTRY = csv_propinfo(
    "country", False, "filename", False, "string", '3 chars abbrev e.g: "ger"'
)
GAME_NAME = csv_propinfo("game_name", False, "filename", False, "string", "e.g. knvb")
SEASON = csv_propinfo(
    "season", False, "filename", False, "int", '"2008/2009" will be int(2008)'
)
NOT_COLUMN_NAMES = [CSV_FILE_DIR, GAME_TYPE, CSV_FILE_NAME, COUNTRY, GAME_NAME, SEASON]


# COLUMN NAMES
DATE = csv_propinfo("date", True, "data", False, "date", "playdate format: dd/mm/yyyy")
HOME = csv_propinfo("home", True, "data", True, "string", "name of home playing team")
AWAY = csv_propinfo("away", True, "data", True, "string", "name of away playing team")
SCORE = csv_propinfo("score", True, "data", True, "string", "string of score int:int")
HOME_GOALS = csv_propinfo(
    "home_goals", True, "data", False, "int", "nr goals of home team incl aet"
)
AWAY_GOALS = csv_propinfo(
    "away_goals", True, "data", False, "int", "nr goals of away team incl aet"
)
URL = csv_propinfo("url", True, "data", True, "string", "full source path (http url)")
HOME_MANAGER = csv_propinfo(
    "home_manager", True, "data", True, "string", "managers name home team"
)
AWAY_MANAGER = csv_propinfo(
    "away_manager", True, "data", True, "string", "managers name away team"
)
HOME_SHEET = csv_propinfo(
    "home_sheet", True, "data", True, "string", "players home team"
)
AWAY_SHEET = csv_propinfo(
    "away_sheet", True, "data", True, "string", "players away team"
)
DEFAULT_COLUMN_NAMES = [
    DATE,
    HOME,
    AWAY,
    SCORE,
    HOME_GOALS,
    AWAY_GOALS,
    URL,
    HOME_MANAGER,
    AWAY_MANAGER,
    HOME_SHEET,
    AWAY_SHEET,
]

# EXTRA (CUP) COLUMN NAMES
ROUND_TEXT = csv_propinfo(
    "round_text", True, "data", True, "string", 'play round e.g. "Round of 16"'
)
PLAY_ROUND = csv_propinfo(
    "play_round", True, "data", False, "int", "play round int from 0 to inf"
)
SCORE_45 = csv_propinfo(
    "score_45", True, "data", True, "string", 'score after 45minutes "x:x"'
)
SCORE_90 = csv_propinfo(
    "score_90", True, "data", True, "string", 'score after 90minutes "x:x"'
)
SCORE_105 = csv_propinfo(
    "score_105", True, "data", True, "string", 'score after 105minutes "x:x"'
)
SCORE_120 = csv_propinfo(
    "score_120", True, "data", True, "string", 'score after 120minutes "x:x"'
)
AET = csv_propinfo(
    "aet", True, "data", False, "bool", "bool: extra time needed to end game?"
)
PSO = csv_propinfo(
    "pso", True, "data", False, "bool", "bool: penalties needed to end game?"
)
EXTRA_CUP_COLUMN_NAMES = [
    ROUND_TEXT,
    PLAY_ROUND,
    SCORE_45,
    SCORE_90,
    SCORE_105,
    SCORE_120,
    AET,
    PSO,
]

BASE_GAME_PROPERTIES = NOT_COLUMN_NAMES + DEFAULT_COLUMN_NAMES
LEAGUE_GAME_PROPERTIES = BASE_GAME_PROPERTIES
CUP_GAME_PROPERTIES = BASE_GAME_PROPERTIES + EXTRA_CUP_COLUMN_NAMES

csv_dtypes = namedtuple("csv_dtypes", ["name", "panda_type", "numpy_type"])
STRING = csv_dtypes("string", "object", "unicode")
INT = csv_dtypes("int", "int64", "int64")
FLOAT = csv_dtypes("float", "float64", "float64")
BOOL = csv_dtypes("bool", "bool", "bool_")
# DATE = csv_dtypes("date", "datetime64", "datetime64[ns]")
# convert date with as_type is buggy compared to pd.to_datetime()
# https://stackoverflow.com/questions/16158795/cant-convert-dates-to-datetime64
# for now we import date fields as string, and then do the pd.to_datetime()
DATE = csv_dtypes("date", "object", "unicode")
DTYPES = [STRING, INT, FLOAT, BOOL, DATE]

PLAYER_PROPERTIES = "temp"
