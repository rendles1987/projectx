from tools.constants import SQLITE_FULL_PATH
import logging
import os
import pandas as pd
import sqlite3


log = logging.getLogger(__name__)


def is_panda_df_empty(panda_df):
    """ check is panda dataframe is empty or not
    :return: bool """
    try:
        if panda_df.empty:
            return True
        else:
            return False
    except ValueError:
        try:
            if panda_df:
                return False
            else:
                return True
        except Exception as e:
            raise AssertionError("nested try-except " + str(e))
    except AttributeError:
        # 'NoneType' object has no attribute 'empty'
        return True
    # all other exceptions
    except Exception as e:
        raise AssertionError("No ValueError or AttributeError, but: " + str(e))


def detect_dateformat(df):
    possible_formats = ["%d/%m/%Y", "%Y-%m-%d"]
    for date_format in possible_formats:
        try:
            pd.to_datetime(df["date"], format=date_format, errors="raise")
            return date_format
        except Exception:
            pass
    raise AssertionError("date format not recognized")


def ensure_corect_date_format(df):
    """ the aim is to keep 1 date format throughout whole project because when saving
    to .csv dtype becomes string. lets convert these strings to same format as
    panda datetime format is '2016-01-26' (yyyy-mm-dd). """
    # first check if a 'date' column exists
    if "date" not in df.columns:
        return df
    date_format_detected = detect_dateformat(df)
    date = pd.to_datetime(df["date"], format=date_format_detected, errors="ignore")
    # we need to convert date to format yyyy-mm-dd (in string format since we will
    # save to .csv
    df["date"] = date.astype(str)
    return df


def drop_duplicates_in_csv(csv_full_path, keep=None):
    assert keep in ("first", "last", False)
    df = pd.read_csv(csv_full_path, sep="\t")
    nr_rows_before_drop = len(df)
    df.drop_duplicates(keep=keep, inplace=True)
    nr_rows_after_drop = len(df)
    diff = nr_rows_before_drop - nr_rows_after_drop
    if diff > 0:
        log.info(f"{csv_full_path} dropped {diff} rows")
        df_to_csv(df, csv_full_path)


def df_to_csv(df, csv_path):
    """convert panda dataframe to csv
    By default the following values are interpreted as NaN:
        ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’,
        ‘1.#IND’, ‘1.#QNAN’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’.
    Throughout this whole project we deal with pd's nan in the csv as 'NA' (you see
    NA in the file) """
    # check if file not already exists
    if os.path.isfile(csv_path):
        log.warning("creating " + csv_path + ", but it already exists. Replacing..")
        os.remove(csv_path)
    # df.to_csv(csv_path, index=False, sep="\t", na_rep="NA")
    df.to_csv(csv_path, sep="\t", na_rep="NA", index=False)


def sqlite_table_to_df(table_name=None):
    assert os.path.isfile(SQLITE_FULL_PATH)
    assert table_name
    log.info(f"sqlite table {table_name} to panda df")
    connex = sqlite3.connect(SQLITE_FULL_PATH)
    query = """
            SELECT
                *
            FROM
                {};
            """.format(
        table_name
    )
    df = pd.read_sql_query(query, connex)
    connex.close()
    return df


def df_to_sqlite_table(df, table_name=None, if_exists=None):
    """
    if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
        How to behave if the table already exists.
            fail: Raise a ValueError.
            replace: Drop the table before inserting new values.
            append: Insert new values to the existing table.
    """
    assert os.path.isfile(SQLITE_FULL_PATH)
    assert table_name
    assert if_exists in ["fail", "replace", "append"]
    log.info(f"panda df to sqlite table {table_name}")
    connex = sqlite3.connect(SQLITE_FULL_PATH)
    df.to_sql(name=table_name, con=connex, if_exists=if_exists, index=False)
    connex.close()


def drop_table_if_exists_from_sqlite(table_name=None):
    assert os.path.isfile(SQLITE_FULL_PATH)
    assert table_name
    log.info(f"drop sqlite table {table_name}")
    connex = sqlite3.connect(SQLITE_FULL_PATH)
    cursor = connex.cursor()
    query = """DROP TABLE IF EXISTS {};""".format(table_name)
    cursor.execute(query)
    connex.close()


def compress_df(df):
    """Downcast integer and float dyptes to smallest (aim is to compress sqlite)
    Downcast from int64 to uint8 (if possible) and from float64 to float32 """
    search_types = ["integer", "float"]
    for search_type in search_types:
        existing_type_columns = df.select_dtypes(include=[search_type]).columns
        for col in existing_type_columns:
            df[col] = pd.to_numeric(df[col], downcast=search_type)
    return df


def string_to_unicode(my_string):
    try:
        my_unicode = my_string.encode(encoding="utf-8")
        return my_unicode
    except Exception as e1:
        # is it already a unicode?
        try:
            my_unicode = my_string  # my_string is already unicode
            my_unicode.decode()
            return my_unicode
        except Exception as e2:
            msg = (
                str(my_string)
                + ' is instance of "bytes" (unicode), but cannot be decoded??'
            )
            raise msg + str(e2)
    msg = str(my_string) + " cannot be encoded nor decoded..??"
    raise msg + str(e1)


def unicode_to_string(my_unicode):
    try:
        my_string = my_unicode.decode(encoding="utf-8")
        return my_string
    except Exception as e1:
        # is it already a string?
        try:
            my_string = my_unicode  # my_unicode is already string
            my_string.encode()
            return my_string
        except Exception as e2:
            msg = str(my_string) + " is instance of str, but cannot be encoded??"
            raise msg + str(e2)
    msg = str(my_string) + " cannot be decoded nor encoded..??"
    raise msg + str(e1)


def temp_get_unicode(my_string):
    """
    https://stackoverflow.com/questions/39283689/converting-double-slash-utf-8-encoding
    weird stuff with unicode.
    Aleksander Puštov is away_manager of first match in this csv:
    path = '/work/data/temp_stuff/_03_clean/cup/all_games_eur_champions_league_qual_invalid.csv'

    In the csvs (raw, import, clean) it is:     'Aleksander Pu\u0161tov'.
    However, if you do self.dataframe.iloc[1].away_manager, you get an extra
    backslash: 'Aleksander Pu\\u0161tov'. Which cannot be encoded:
    print(orig.encode())  # b'Aleksander Pu\\u0161tov'
    To fix this, do:
    1) re-encode the string as UTF-8
    2) decode it with unicode escape
    a = 'Aleksander Pu\\u0161tov'
    b = a.encode("utf-8").decode("unicode_escape")  # 'Aleksander Puštov'
    c = b.encode(encoding="utf-8")                  # b'Aleksander Pu\xc5\xa1tov'

    # EXAMPLE:
    a = self.dataframe.iloc[0].away_manager
    print(a)
    # 'Aleksander Pu\\u0161tov'

    b = a.encode("utf-8").decode("unicode_escape")
    print(b)
    # 'Aleksander Puštov'

    c = b.encode(encoding="utf-8")
    print(c)
    # b'Aleksander Pu\xc5\xa1tov'
    """
    my_unicode = my_string.encode("utf-8").decode("unicode_escape").encode()
    return my_unicode
