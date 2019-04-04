from collections import namedtuple

COUNTRY_WHITE_LIST = ["ger", "eng", "por", "esp", "fra", "ita", "ned"]


# csv variable information
csv_propinfo = namedtuple(
    "csv_propinfo",
    ["name", "is_column_name", "source", "is_not_null", "desired_type", "descr"],
)

# NOT COLUMN NAMES
CSV_FILE_DIR = csv_propinfo(
    "csv_file_dir", False, "filedir", True, "", "absolute dir without filename"
)
GAME_TYPE = csv_propinfo(
    "game_type", False, "filename", True, "string", "cup of league"
)
CSV_FILE_NAME = csv_propinfo(
    "csv_file_name", False, "filename", True, "string", "filename + extension"
)
COUNTRY = csv_propinfo(
    "country", False, "filename", True, "string", '3 chars abbrev e.g: "ger"'
)
GAME_NAME = csv_propinfo("game_name", False, "filename", True, "string", "e.g. knvb")
SEASON = csv_propinfo(
    "season", False, "filename", True, "int", '"2008/2009" will be int(2008)'
)
NOT_COLUMN_NAMES = [CSV_FILE_DIR, GAME_TYPE, CSV_FILE_NAME, COUNTRY, GAME_NAME, SEASON]

# COLUMN NAMES
DATE = csv_propinfo("date", True, "data", True, "date", "playdate format: dd/mm/yyyy")
HOME = csv_propinfo("home", True, "data", True, "string", "name of home playing team")
AWAY = csv_propinfo("away", True, "data", True, "string", "name of away playing team")
HOME_GOALS = csv_propinfo(
    "home_goals", True, "data", True, "int", "nr goals of home team incl aet"
)
AWAY_GOALS = csv_propinfo(
    "away_goals", True, "data", True, "int", "nr goals of away team incl aet"
)
URL = csv_propinfo("url", True, "data", True, "string", "full source path (hhtp url)")
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
    HOME_GOALS,
    AWAY_GOALS,
    URL,
    HOME_MANAGER,
    AWAY_MANAGER,
    HOME_SHEET,
    AWAY_SHEET,
]

# EXTRA (CUP) COLUMN NAMES
SCORE_45 = csv_propinfo(
    "score_45", True, "data", True, "string", 'score after 45minutes "x:x"'
)
SCORE_90 = csv_propinfo(
    "score_90", True, "data", True, "string", 'score after 90minutes "x:x"'
)
SCORE_105 = csv_propinfo(
    "score_105", True, "data", False, "string", 'score after 105minutes "x:x"'
)
SCORE_120 = csv_propinfo(
    "score_120", True, "data", False, "string", 'score after 120minutes "x:x"'
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


"""
Pandas dtype	    Python type	        NumPy type	                Usage
--------------------------------------------------------------------------------------------------------
object              str	                string_, unicode_	        Text
int64	            int	                int_, int8, int16, int32,   Integer numbers 
                                        int64, uint8, uint16, 
                                        uint32, uint64	
float64	            float	            float_, float16, float32,   Floating point numbers
                                        float64	
bool	            bool	            bool_	                    True/False values
datetime64	        NA	                datetime64[ns]	            Date and time values
timedelta[ns]	    NA	                NA	                        Differences between two datetimes
category	        NA	                NA	                        Finite list of text values
"""

csv_dtypes = namedtuple("csv_dtypes", ["name", "panda_type", "numpy_type"])
STRING = csv_dtypes("string", "object", "unicode")
INT = csv_dtypes("int", "int64", "int64")
FLOAT = csv_dtypes("float", "float64", "float64")
BOOL = csv_dtypes("bool", "bool", "bool_")
DATE = csv_dtypes("date", "datetime64", "datetime64[ns]")
DTYPES = [STRING, INT, FLOAT, BOOL, DATE]


# datasource_csv_dtypes_mapping = [
#     # desired_dtype, pandas_type, numpy_type
#     ('string', 'object', 'unicode'),
#     ('int', 'int64', 'int64'),
#     ('float', 'float64', 'float64'),
#     ('bool', 'bool', 'bool_'),
#     ('date', 'datetime64', 'datetime64[ns]'),
# ]
#
# dtype_panda_dct =
#     dict([(a[0], a[1]) for a in datasource_csv_dtypes_mapping])
# dtype_numpy_dct = dict([(a[0], a[2]) for a in datasource_csv_dtypes_mapping])
