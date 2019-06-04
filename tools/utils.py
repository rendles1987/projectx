import os
from tools.constants import dateformat_yyyymmdd
from tools.logging import log
import pandas as pd
import datetime


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
    try:
        test = df["date"][1]
    except Exception:
        return df
    # date_string = df["date"].astype(str)
    date_format_detected = detect_dateformat(df)
    date = pd.to_datetime(df["date"], format=date_format_detected, errors="ignore")
    # we need to convert date to format yyyy-mm-dd (in string format since we will
    # save to .csv
    df["date"] = date.astype(str)
    return df


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
