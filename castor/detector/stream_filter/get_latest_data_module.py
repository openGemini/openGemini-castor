"""
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import absolute_import

import pandas as pd
import numpy as np

from ..cache.cache import CacheSet
from ...utils.exceptions import NoNewDataError, ValueNotEnoughError
from ...utils import const as con
from ...utils.common import FIFOData
from ...utils.logger import logger


class LatestData:
    def __init__(self):
        cache = CacheSet()
        self.stream_filter_cache = cache.get_cache(con.STREAM_FILTER_CACHE)
        self.data_cache = cache.get_cache(con.DATA_CACHE)

    @staticmethod
    def _construct_index_by_inferred_freq(
        input_index: pd.DatetimeIndex,
        latest_index: pd.Timestamp,
        construct_index_len: int,
    ) -> pd.DatetimeIndex:
        """
        construct index by inferred_freq of input_index
        """
        inferred_freq = input_index.inferred_freq
        if inferred_freq is None:
            inferred_freq = pd.Timedelta(input_index[0] - latest_index)
        indexes = pd.date_range(
            end=input_index[0], freq=inferred_freq, periods=construct_index_len + 1
        )[:-1]
        indexes = indexes.append(input_index)
        return indexes

    @staticmethod
    def _align_data_to_end(fifo_data: FIFOData, data_length: int) -> np.ndarray:
        """
        get data, which is shifted to end of array when data is not full
        """
        data = fifo_data.get_data()
        if not fifo_data.is_full():
            data = data.copy()
            data[-data_length:] = data[:data_length]
        return data

    def get_data(
        self, data: pd.DataFrame, latest_index: pd.Timestamp, window: int
    ) -> pd.DataFrame:
        if latest_index is not None:
            new_indexes = data.index[data.index > latest_index]

            # return row data if the old data length of row data is more than window
            new_data_len = len(new_indexes)
            old_data_len = len(data) - new_data_len
            if old_data_len >= window:
                return data.iloc[-(new_data_len + window) :, :]

            return self._get_data_with_cache(data, latest_index, window, new_indexes)

        else:
            if len(data) < window + 1:
                raise ValueNotEnoughError("not enough data for detection")
            else:
                self.not_detected_log(data.index[0], data.index[window])
                return data

    def _get_data_with_cache(
        self,
        data: pd.DataFrame,
        latest_index: pd.Timestamp,
        window: int,
        new_indexes: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """
        get latest data by comparing the length of cache data and input data.
        if the length of cache data and input data are both not enough, raise ValueNotEnoughError
        """
        new_data_len = len(new_indexes)

        # get cache when the length of (cache + new data) is more than (window + 1)
        # window + 1 is the minimum length of algorithm require
        min_length_of_required_cache = max(window + 1 - new_data_len, 0)
        try:
            cache_data_total, valid_col_list = self._get_enough_length_cache_data(
                min_length_of_required_cache, data.columns, window
            )
        except ValueNotEnoughError as error:
            # if len(cache data + new data) < window + 1 and len(data) < window + 1, raise ValueNotEnoughError.
            if len(data) < window + 1:
                raise error
            # if return detect, there are some points which won't be detected
            self.not_detected_log(new_indexes[0], data.index[window])
            return data

        cache_data_len = len(cache_data_total)
        # concat cache and new data
        if len(cache_data_total) > len(data) - new_data_len:
            data = data.loc[new_indexes, valid_col_list]
            result = np.concatenate((cache_data_total, data), axis=0)
        else:
            # if return detect, there are some points which won't be detected
            self.not_detected_log(new_indexes[0], data.index[window])
            return data
        # construct index of the data which concat the cache data and new data
        indexes = self._construct_index_by_inferred_freq(
            data.index, latest_index, cache_data_len
        )
        result = pd.DataFrame(result, index=indexes, columns=data.columns)

        # if cache_data_len < window, there are some points which won't be detected
        if cache_data_len < window:
            self.not_detected_log(new_indexes[0], result.index[window])
        return result

    @staticmethod
    def not_detected_log(begin, end):
        logger.info(
            "the points, whose index satisfies the conditions that ( %s <= index < %s ), were not be detected",
            begin,
            end,
        )

    def _get_enough_length_cache_data(
        self, required_cache_length: int, columns: pd.Index, window: int
    ) -> (np.array, list):
        if window == 0:
            return np.array([]), None
        data_cache = self.data_cache
        cache_data_list = []
        col_list = []
        min_index = window
        for col in columns:
            col_cache_data = data_cache.get_value(str(col))
            if col_cache_data is not None:
                real_data_length = col_cache_data.get_length()
                if real_data_length >= required_cache_length:
                    col_data = self._align_data_to_end(col_cache_data, real_data_length)
                    cache_data_list.append(col_data)
                    col_list.append(col)
                    min_index = min(real_data_length, min_index)
        if cache_data_list:
            return np.stack(cache_data_list, axis=1)[-min_index:, :], col_list
        else:
            raise ValueNotEnoughError("not enough data for detection")

    def update(self, window: int, data: pd.DataFrame) -> None:
        """
        update data cache and Stream Filter cache by input data
        """
        if window > 0:
            latest_index = get_latest_index(data.columns)
            if latest_index is not None:
                data = data.loc[data.index > latest_index, :]
            data_cache = self.data_cache
            stream_filter_cache = self.stream_filter_cache
            index = data.index[-1]
            columns = data.columns
            col_number = list(range(len(columns)))
            data = data.values
            for col, number_index in zip(columns, col_number):
                col_cache_data = data_cache.get_value(str(col))
                if col_cache_data is None:
                    col_cache_data = FIFOData(window)
                    data_cache.set_value(str(col), col_cache_data)
                col_cache_data.update(data[:, number_index])
                stream_filter_cache.set_value(str(col), index)

    def filter_disorder_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """ "
        filter the data of field, which latest index in cache is more than newest index of input data
        """

        disorder_cols = []
        index = data.index[-1]
        for col, series in data.items():
            latest_index = self.stream_filter_cache.get_value(str(col))
            if latest_index is not None and latest_index >= index:
                disorder_cols.append(col)

        if len(disorder_cols) == len(data.columns):
            raise NoNewDataError("no new data for detection")

        if disorder_cols:
            data.drop(columns=disorder_cols, inplace=True)

        return data


def get_latest_index(columns: pd.Index) -> pd.Timestamp:
    """
    get latest index for all columns of dataframe through counting the maximum
    in batch detection, the latest indexes of all columns must be the same.
    """
    stream_filter_cache = CacheSet().get_cache(con.STREAM_FILTER_CACHE)
    latest_indexes = [
        stream_filter_cache.get_value(str(col))
        for col in columns
        if stream_filter_cache.get_value(str(col)) is not None
    ]
    if not latest_indexes:
        latest_index = None
    else:
        latest_index = max(latest_indexes)
    return latest_index


def get_latest_multi_timestamps_data(data: pd.DataFrame, tag) -> pd.DataFrame:
    stream_filter_cache = CacheSet().get_cache(con.STREAM_FILTER_CACHE)
    latest_index = stream_filter_cache.get_value(tag)
    stream_filter_cache.set_value(tag, np.max(data.index))
    if latest_index is not None and latest_index >= np.max(data.index):
        raise NoNewDataError("no new data for detection")
    return data
