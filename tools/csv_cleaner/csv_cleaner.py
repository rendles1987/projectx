import os

import numpy as np
import pandas as pd
from tools.constants import (
    BASE_GAME_PROPERTIES,
    CUP_GAME_PROPERTIES,
    GAME_SPECS,
    LEAGUE_GAME_PROPERTIES,
    PLAYER_PROPERTIES,
    SEASON_WINDOW,
    dateformat_yyyymmdd,
)
from tools.logging import log
from tools.utils import df_to_csv, is_panda_df_empty
from tools.csv_importer.filename_checker import LeagueFilenameChecker

class BaseCsvCleaner:
    def __init__(self, csvfilepath):
        self.csv_type = None
        self.csv_file_full_path = csvfilepath
        self.properties = BASE_GAME_PROPERTIES
        self._dataframe = None
        self._dataframe_invalid = None
        self.max_goals = None

    @property
    def csv_file_dir(self):
        assert os.path.isfile(self.csv_file_full_path)
        assert os.path.isdir(os.path.dirname(self.csv_file_full_path))
        return os.path.dirname(self.csv_file_full_path)  # '/work/data/raw_data/league/

    @property
    def csv_file_name_with_extension(self):
        return self.csv_file_full_path.split("/")[-1]  # 'xx.csv'

    @property
    def csv_file_name_without_extension(self):
        return (self.csv_file_full_path.split("/")[-1]).split(".")[0]  # 'xx'

    @property
    def dataframe(self):
        if not is_panda_df_empty(self._dataframe):
            return self._dataframe
        self._dataframe = pd.read_csv(self.csv_file_full_path, sep="\t")
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self._dataframe = dataframe

    @property
    def dataframe_invalid(self):
        return self._dataframe_invalid

    @dataframe_invalid.setter
    def dataframe_invalid(self, dataframe):
        self._dataframe_invalid = dataframe

    def check_replace_empty_strings_with_nan(self):
        """ replace all columns that are empty (e.g. 1. "''", 2. "' '", 3. "[]",
        4. "[ ]", with np.NAN """
        # replace field that's entirely space (or empty) with NaN
        self.dataframe.replace(r"^\s*$", np.NAN, regex=True, inplace=True)
        # replace field that's is '[ ]' or '[]' with NaN
        self.dataframe.replace("[]", np.NAN, regex=False, inplace=True)
        self.dataframe.replace("[ ]", np.NAN, regex=False, inplace=True)
        self.dataframe.replace("???", np.NAN, regex=False, inplace=True)

    def check_white_list_url(self):
        """ url may only contain 'http'/'https' or np.Nan"""
        pd.options.mode.chained_assignment = None  # default='warn'
        column_name = "url"
        # create Int64Index: select indices where df[column_name] startswith 'http'
        http_index = self.dataframe[
            self.dataframe[column_name].str.startswith("http")
        ].index
        # update df[column_name] to np.Nan if index is not in http_index
        self.dataframe[column_name][~self.dataframe.index.isin(http_index)] = np.NAN
        pd.options.mode.chained_assignment = "warn"  # default='warn'

    def check_table_contains_unknown(self):
        black_list = ["unkown", "unkwown", "ukwnown", "unknown"]
        for black_string in black_list:
            # only loop trough string columns
            for column in self.dataframe.select_dtypes(include="O"):
                wrong_idx = self.dataframe.index[self.dataframe[column] == black_string]
                if wrong_idx.size == 0:
                    continue
                log.warning(
                    f"found {wrong_idx.size} rows that contain unkown/ukwnown"
                    f" {self.csv_file_name_without_extension}"
                )
                # update cells in column to np.NAN if matches a black_string
                self.dataframe[column][self.dataframe.index.isin(wrong_idx)] = np.NAN

    def get_season_from_url(self, url_string):
        """ get season (int) from the url_string and also check it """
        """ >>> str = "h3110 23 cat 444.4 rabbit 11 2 dog"
            >>> [int(s) for s in str.split() if s.isdigit()]
            [23, 11, 2] """

        if pd.isna(url_string):  # this is per row
            return np.NAN
        digits = [
            int(s)
            for s in url_string.split("-")
            if s.isdigit() and 1999 < int(s) < 2019
        ]
        if not digits:
            raise AssertionError(
                f"{self.csv_file_name_with_extension} I expect "
                f"{url_string} to have season ints"
            )
        season = min(digits)
        return season

    def get_season_from_date(self, date):

        df = pd.DataFrame({"date": [date]})

        try:
            pd_date = pd.to_datetime(df["date"], format=dateformat_yyyymmdd)
        except:
            raise AssertionError(
                f"{self.csv_file_name_with_extension} date format fail.. this should "
                f"have been fixed in ensure_corect_date_format()"
            )

        date_year = pd_date.dt.year
        for season in [date_year - 1, date_year, date_year + 1]:
            start_date = pd.to_datetime(
                pd.DataFrame(
                    {
                        "year": season,
                        "month": SEASON_WINDOW.month_start,
                        "day": SEASON_WINDOW.day_start,
                    }
                )
            )
            end_date = pd.to_datetime(
                pd.DataFrame(
                    {
                        "year": season + 1,
                        "month": SEASON_WINDOW.month_end,
                        "day": SEASON_WINDOW.day_end,
                    }
                )
            )

            # we compare only 1 date with start- and enddate, but due to code above I
            # have to include an .all()
            if pd_date.between(start_date, end_date, inclusive=True).all():
                return season
        raise AssertionError("season (based on date) is not logic")

    def handle_fail_dates(self, df_fail_dates):
        # all fail_dates['season'] should be np.NAN otherwise there is problem
        if df_fail_dates["season"].isna().all():
            # no problem, so return ('season' is np.NAN (since url is np.NAN))
            return

        # now we have a problem: season is not np.NAN and based on that a start- and
        # end date was set. Apparently is df['date'] not in between start and end.
        # Now get only those from fail_dates
        msg = "date out of logic range "
        log_these_dates = df_fail_dates[~df_fail_dates["season"].isna()]
        log.error(f"{msg} {self.csv_file_name_without_extension}: {log_these_dates}")

        # index in self.dataframe
        index_log_these_dates = log_these_dates.index.values
        # remove these rows from self.dataframe
        self.dataframe.drop(self.dataframe.index[index_log_these_dates], inplace=True)

        self.add_to_invalid_df(log_these_dates, msg)

    def add_to_invalid_df(self, df_wrong, msg):
        df_wrong["msg"] = msg
        if is_panda_df_empty(self.dataframe_invalid):
            self.dataframe_invalid = df_wrong
        else:
            self.dataframe_invalid.append(df_wrong, ignore_index=True)

    def calc_season(self):
        raise NotImplementedError

    def check_date_in_range(self):
        """ get date from column url. For league csv we can also use date in
        filename to verify date """
        assert (
            not self.dataframe["date"].isna().all(),
            "I expect 'date' column does not include any np.NAN",
        )

        self.calc_season()

        try:
            date = pd.to_datetime(self.dataframe["date"], format=dateformat_yyyymmdd)
        except:
            raise AssertionError(
                f"{self.csv_file_name_with_extension} date format fail.. this should "
                f"have been fixed in ensure_corect_date_format()"
            )

        # create a start date column
        start_date = pd.to_datetime(
            {
                "year": self.dataframe["season"],
                "month": SEASON_WINDOW.month_start,
                "day": SEASON_WINDOW.day_start,
            }
        )
        # create a start date column
        end_date = pd.to_datetime(
            {
                "year": self.dataframe["season"] + 1,
                "month": SEASON_WINDOW.month_end,
                "day": SEASON_WINDOW.day_end,
            }
        )
        # get dates that are not between start and end (if season = np.NAN, then
        # start_date and end_date are also np.NAN and that row will be selected here
        df_fail_dates = self.dataframe[
            ~date.between(start_date, end_date, inclusive=True)
        ]

        # TODO delete this "self.tmp_check()"
        self.tmp_check(df_fail_dates)

        if not is_panda_df_empty(df_fail_dates):
            self.handle_fail_dates(df_fail_dates)

    def tmp_check(self, df_fail_dates):
        # TODO: once you filled column season (by filename and url), then you delete
        # this tmp_check
        df_fail_dates_season_not_nan = df_fail_dates[~df_fail_dates["season"].isna()]
        if not is_panda_df_empty(df_fail_dates_season_not_nan):
            raise AssertionError(
                "trouble..  date not in range even if we neglect " "season is nan rows"
            )

    def _check_score_format_home(self, column_name):
        # dtype becomes now object
        home_score = self.dataframe[column_name].str.split(":", n=1, expand=True)[0]
        if not (home_score.astype(int) <= self.max_goals).all():
            wrong_home_score = self.dataframe[home_score.astype(int) > self.max_goals][
                column_name
            ]
            log.error(f"more home goals than expected: {str(wrong_home_score.values)}")
            raise AssertionError(
                f"{self.csv_file_name_with_extension} more home goals than expected"
            )
        if not all(home_score.astype(int) == self.dataframe["home_goals"]):
            # raise AssertionError("home_goals does not match score")
            raise AssertionError(
                f"{self.csv_file_name_with_extension} home_goals does not match score"
            )

    def _check_score_format_away(self, column_name):
        """ number of goals away in """
        # dtype becomes now object
        away_score = self.dataframe[column_name].str.split(":", n=1, expand=True)[1]
        if not (away_score.astype(int) <= self.max_goals).all():
            wrong_away_score = self.dataframe[away_score.astype(int) > self.max_goals][
                column_name
            ]
            log.error(f"more away goals than expected: {str(wrong_away_score.values)}")
            raise AssertionError(
                f"{self.csv_file_name_with_extension} more away goals than expected"
            )
        if not all(away_score.astype(int) == self.dataframe["away_goals"]):
            raise AssertionError(
                f"{self.csv_file_name_with_extension} away_goals does not match score"
            )

    def check_score_format(self):
        """ - this string must be 3 chars long e.g "3:5"
            - field must contain 1 digit int, a colon (:), and 1 digit int"""
        column_name = "score"

        # max one ":" per score cell
        if not (self.dataframe[column_name].str.count(":") == 1).all():
            wrong_score_format = self.dataframe[
                self.dataframe[column_name].str.count(":") != 1
            ][column_name]
            log.error(
                f'I expect only one colon ":" in score string: '
                f"{str(wrong_score_format.values)}"
            )
            raise AssertionError(
                f"{self.csv_file_name_with_extension} only one colon ':'' in score "
                f"string expected"
            )
        self._check_score_format_home(column_name)
        self._check_score_format_away(column_name)

    def update_if_url_nan(self):
        raise NotImplementedError

    def check_score_logic(self):
        raise NotImplementedError

    def save_changes(self):
        """ remove orig file and save self.dataframe to orig file filepath """
        os.remove(self.csv_file_full_path)
        df_to_csv(self.dataframe, self.csv_file_full_path)

    def save_df_invalid(self):
        if is_panda_df_empty(self.dataframe_invalid):
            return
        dir_full_path = self.csv_file_full_path.rstrip(
            self.csv_file_name_with_extension
        )
        filename = self.csv_file_name_without_extension + "_invalid.csv"
        csv_file_full_path = os.path.join(dir_full_path, filename)
        df_to_csv(self.dataframe_invalid, csv_file_full_path)

    def run(self):
        self.check_replace_empty_strings_with_nan()
        self.check_white_list_url()
        self.update_if_url_nan()
        self.check_table_contains_unknown()
        self.check_date_in_range()
        self.check_score_format()
        self.check_score_logic()
        self.save_changes()
        self.save_df_invalid()


class CupCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "league"
        self.properties = CUP_GAME_PROPERTIES
        self.max_goals = GAME_SPECS.cup_max_goals

    def calc_season(self):
        """The aim is to add column 'season' to self.dataframe (completely filled)
        For cup csvs the season can only be retrieved from column "url". If "url"
        is np.nan then just rely on date.. """
        # first create a season column (based on url)
        self.dataframe["season"] = self.dataframe["url"].apply(self.get_season_from_url)
        if (self.dataframe["season"].isna()).any():

            date_column = pd.to_datetime(
                self.dataframe["date"], format=dateformat_yyyymmdd
            )
            calc_season_column = date_column.apply(self.get_season_from_date)
            self.dataframe["season"].fillna(calc_season_column[0], inplace=True)

    def update_if_url_nan(self):
        """ if column 'url' is np.nan, then some columns must be updated to np.nan """
        columns_to_nan = [
            "home_manager",
            "away_manager",
            "home_sheet",
            "away_sheet",
            "score_45",
            "score_90",
            "score_105",
            "score_120",
        ]

        columns_to_false = ["aet", "pso"]

        mask = self.dataframe["url"].isna()
        if not mask.any():
            return
        self.dataframe.loc[self.dataframe["url"].isna(), columns_to_nan] = np.nan
        self.dataframe.loc[self.dataframe["url"].isna(), columns_to_false] = False

    def fix_aet(self):
        """ update column aet. In the raw .csvs either aet is True of pso is True.
        I assume that is pso is True, then aet is True (only penalties after extra time)
        """
        self.dataframe["aet"] = (self.dataframe["aet"] == True) | (
            self.dataframe["pso"] == True
        )

    def handle_fail_scores(self, index_msg_mapping, index_true, msg):
        if not index_msg_mapping:
            index_msg_mapping = {elem: msg for elem in index_true}
        else:
            # check for intersection
            intersected_keys = index_true.intersection(index_msg_mapping)
            if intersected_keys.size > 0:
                index_msg_mapping[intersected_keys].append(msg)
            new_keys = [elem for elem in index_true if elem not in index_msg_mapping]
            if new_keys:
                new_dict = {elem: msg for elem in new_keys}
                index_msg_mapping.update(new_dict)
        return index_msg_mapping

    def check_score_logic(self):
        """ check logic of 7 columns: 1) score_45, 2) score_90, 3) score_105,
        4) score_120, 5) aet = (bool) AfterExtraTime, 6) pso = (bool) PenaltyShootOut
        checks:
        1 score_45 cannot be NA
        2 if score_90 is not NA, then aet must be True
        3 if score_105 is not NA, then score_90 cannot be NA
        4 if score_120 is not NA, then score_105 cannot be NA <-- when does it happen?
        5 if aet is True, then score_90 cannot be NA
        6 if pso is True, then score_105 cannot be NA
        """
        self.fix_aet()

        url_nan_mask = self.dataframe["url"].isna()
        if not url_nan_mask.any():
            return

        df_url_not_nan = self.dataframe[~self.dataframe["url"].isna()]

        index_msg_mapping = {}

        # check1
        mask = ~self.dataframe["url"].isna() & self.dataframe["score_45"].isna()
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = "score check1 fail: score_45 cannot be NA"
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

            # index_true = mask[mask == True].index
            # index_msg_mapping = {elem: "score check1 fail" for elem in index_true}

        # check2 (true if score_90 is not NA) & (true if aet is false) > wrong is True
        mask = (
            ~self.dataframe["url"].isna()
            & (~(self.dataframe["score_90"].isna()))
            & (self.dataframe["aet"] == False)
        )
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = "score check2 fail: if score_90 is not NA, then aet must be True"
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

        # check3
        mask = (
            ~self.dataframe["url"].isna()
            & (~(self.dataframe["score_105"].isna()))
            & (self.dataframe["score_90"].isna())
        )
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = (
                "score check3 fail: if score_105 is not NA, then score_90 cannot be NA"
            )
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

        # check4
        mask = (
            ~self.dataframe["url"].isna()
            & (~(self.dataframe["score_120"].isna()))
            & (self.dataframe["score_105"].isna())
        )
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = (
                "score check4 fail: if score_120 is not NA, then score_105 cannot be NA"
            )
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

        # check5
        mask = (
            ~self.dataframe["url"].isna()
            & (self.dataframe["aet"] == True)
            & (self.dataframe["score_90"].isna())
        )
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = "score check5 fail: if aet is True, then score_90 cannot be NA"
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

        # check6
        mask = (
            ~self.dataframe["url"].isna()
            & (self.dataframe["pso"] == True)
            & (self.dataframe["score_105"].isna())
        )
        if mask.any():  # if 1 or more True then we have problem
            index_true = mask[mask == True].index
            msg = "score check6 fail: if pso is True, then score_105 cannot"
            index_msg_mapping = self.handle_fail_scores(
                index_msg_mapping, index_true, msg
            )

        if not index_msg_mapping:
            return
        # some score check(s) failed. First create invalid df and then remove these
        # rows from self.dataframe
        df_log_these_scores = self.dataframe.iloc[list(index_msg_mapping)]
        df_log_these_scores["msg"] = ""
        # update msg column based on dictionary values
        df_log_these_scores.msg.update(pd.Series(index_msg_mapping))

        # delete rows that have invalid scores
        self.dataframe.drop(self.dataframe.index[list(index_msg_mapping)], inplace=True)

        if is_panda_df_empty(self.dataframe_invalid):
            self.dataframe_invalid = df_log_these_scores
        else:
            self.dataframe_invalid.append(df_log_these_scores, ignore_index=True)


class LeagueCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "cup"
        self.properties = LEAGUE_GAME_PROPERTIES
        self.max_goals = GAME_SPECS.league_max_goals

    def calc_season(self):
        """ league column season can be retrieved from column "url" and from filename.
        Check consistency """
        self.dataframe["season"] = self.dataframe["url"].apply(self.get_season_from_url)
        if (self.dataframe["season"].isna()).any():
            season = LeagueFilenameChecker(self.csv_file_full_path).season

            mask = (self.dataframe['season'] != season) & (
                ~self.dataframe['season'].isna())  # problem? then true

            if mask.any():
                raise AssertionError(f'{self.csv_file_name_without_extension} '
                                     f'season from "get_season_from_url()" differs '
                                     f'from season based on filename')

            self.dataframe["season"].fillna(season, inplace=True)

    def check_score_logic(self):
        """ league csv data does not include columns 'score_90' etc, so lets skip """
        pass

    def update_if_url_nan(self):
        """ if column 'url' is np.nan, then some columns must be updated to np.nan """
        columns_to_nan = ["home_manager", "away_manager", "home_sheet", "away_sheet"]
        mask = self.dataframe["url"].isna()
        if not mask.any():
            return
        self.dataframe.loc[self.dataframe["url"].isna(), columns_to_nan] = np.nan


class PlayerCsvCleaner(BaseCsvCleaner):
    def __init__(self, csvfilepath):
        BaseCsvCleaner.__init__(self, csvfilepath)
        self.csv_type = "player"
        self.properties = PLAYER_PROPERTIES
