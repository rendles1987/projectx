import os

from tools.constants import dateformat_yyyymmdd
from tools.logging import log


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


# def date_dtype_must_be_corrected(df):
#     """ make sure that column date has dtype("<M8[ns]") and not dtype('O') """
#     # '<M8[ns]' = e.g. 2000-11-29
#     # 'O' = can be e.g. 29-11-2000
#
#     if 'date' not in df.columns:
#         return False
#     elif df['date'].dtype == '<M8[ns]':
#         return False
#     elif df['date'].dtype == 'O':
#         return True
#     else:
#         unknown_dtype = df['date'].dtype
#         raise AssertionError(f"column date has weird dtype: {unknown_dtype}")


def ensure_corect_date_format(df):
    """ the aim is to keep 1 date format throughout whole project because when saving
    to .csv dtype becomes string. lets convert these strings to same format as
    panda datetime format is '2016-01-26' (yyyy-mm-dd). """
    # first check if a 'date' column exists
    try:
        test = df["date"][1]
    except Exception:
        return df
    date_string = df["date"].astype(str)
    expected = df["date"].dt.strftime(dateformat_yyyymmdd)
    if (date_string == expected).all():
        return df
    # we need to convert date to format yyyy-mm-dd
    df["date"] = expected
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
