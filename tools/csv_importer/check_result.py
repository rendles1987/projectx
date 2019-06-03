import pandas as pd
from tools.logging import log
from tools.utils import ensure_corect_date_format


class CheckResults:
    """ Class that stores check results from RowChecker. Each row in a csv is checked.
    If wrong? then CheckResults is updated. (Only wrong!!) check results are stored in
    a dict "self._invalid_dct". invalid_dct is e.g. {
        row_idx_1: ['msg_column_a'],
        row_idx_2: ['msg_column_a', 'msg_column_c],
    }
    """

    def __init__(self):
        self._invalid_dct = {}

    def add_invalid(self, row_idx, msg):
        assert isinstance(row_idx, int)
        assert isinstance(msg, str)
        assert len(msg) != 0
        log.info("add invalid row to check results")
        if row_idx in self._invalid_dct.keys():
            # append a string to the list of existing check row
            self._invalid_dct[row_idx].append(msg)
        else:
            # add new k,v to dict
            self._invalid_dct.update({row_idx: [msg]})

    def get_valid_df(self, df_selection, clm_desired_dtype_dict):
        """get valid rows from original panda dataframe (convert + strip)"""

        # first get invalid row indices
        invalid_row_idx_list = self.__get_invalid_row_idx_list()
        # get inverse row indices of dataframe
        valid_row_idx_list = list(
            set(df_selection.index.to_list()) - set(invalid_row_idx_list)
        )
        # convert
        df_convert = df_selection.iloc[valid_row_idx_list].astype(
            clm_desired_dtype_dict
        )

        df_convert = ensure_corect_date_format(df_convert)

        # strip
        df_obj = df_convert.select_dtypes(["object"])
        df_convert[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        return df_convert

    def get_invalid_df(self, df_orig):
        """get invalid rows from original panda dataframe """
        invalid_row_idx_list = self.__get_invalid_row_idx_list()
        invalid_msg_list = self.__get_invalid_msg_list()
        # update df_copy with msg for all invalid rows
        df_orig["msg"] = pd.DataFrame(
            {"msg": invalid_msg_list}, index=invalid_row_idx_list
        )
        return df_orig.iloc[invalid_row_idx_list]

    @property
    def count_invalid_rows(self):
        return len(self.__get_invalid_row_idx_list())

    def __get_invalid_row_idx_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_row_idx_list

    def __get_invalid_msg_list(self):
        invalid_row_idx_list = list(self._invalid_dct.keys())
        invalid_msg_list = list(self._invalid_dct.values())
        assert len(invalid_row_idx_list) == len(invalid_msg_list)
        return invalid_msg_list
