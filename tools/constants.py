from collections import namedtuple

COUNTRY_WHITE_LIST = ["eng", "esp", "fra", "ger", "ita", "ned", "por"]

COUNTRY_LEAGUE_NAMES = {
    "eng": {1: "premier_league", 2: "championship", 3: "league_one", 4: "league_two"},
    "esp": {1: "primera_division", 2: "segunda_division"},
    "fra": {1: "ligue_1", 2: "ligue_2"},
    "ger": {1: "bundesliga", 2: "2_bundesliga"},
    "ita": {1: "serie_a", 2: "serie_b"},
    "ned": {1: "eredivisie"},
    "por": {1: "primeira_liga"},
}

COUNTRY_CUP_NAMES = {
    "eng": {
        1: "fa_cup",
        2: "league_cup",
        3: "community_shield",
        4: "playoff_championship",
    },
    "esp": {1: "copa_del_rey", 2: "supercopa"},
    "fra": {1: "coupe_de_france", 2: "coupe_de_la_ligue", 3: "trophee_des_champions"},
    "ger": {1: "dfb_pokal", 2: "liga_pokal", 3: "supercup"},
    "ita": {1: "coppa_italia", 2: "supercoppa"},
    "ned": {1: "knvb_beker"},
    "por": {1: "supertaca", 2: "taca", 3: "taca_da_liga"},
    "eur": {
        1: "europa_league",
        2: "europa_league_qual",
        3: "champions_league",
        4: "champions_league_qual",
    },
}

RAW_CSV_DIRS = {
    "cup": "/work/data/raw_data/cup",
    "league": "/work/data/raw_data/league",
    "player": "/work/data/raw_data/player",
}

CLEAN_CSV_DIRS = {
    "cup": "/work/data/clean_data/cup",
    "league": "/work/data/clean_data/league",
    "player": "/work/data/clean_data/player",
}


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
    "play_round", True, "data", True, "int", "play round int from 0 to inf"
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
EXTRA_CUP_COLUMN_NAMES = [SCORE_45, SCORE_90, SCORE_105, SCORE_120, AET, PSO]

BASE_GAME_PROPERTIES = NOT_COLUMN_NAMES + DEFAULT_COLUMN_NAMES
LEAGUE_GAME_PROPERTIES = BASE_GAME_PROPERTIES
CUP_GAME_PROPERTIES = BASE_GAME_PROPERTIES + EXTRA_CUP_COLUMN_NAMES

csv_dtypes = namedtuple("csv_dtypes", ["name", "panda_type", "numpy_type"])
STRING = csv_dtypes("string", "object", "unicode")
INT = csv_dtypes("int", "int64", "int64")
FLOAT = csv_dtypes("float", "float64", "float64")
BOOL = csv_dtypes("bool", "bool", "bool_")
DATE = csv_dtypes("date", "datetime64", "datetime64[ns]")
DTYPES = [STRING, INT, FLOAT, BOOL, DATE]

PLAYER_PROPERTIES = "temp"
