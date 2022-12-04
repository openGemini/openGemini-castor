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

from __future__ import absolute_import, division
from abc import abstractmethod, ABC
from typing import Hashable, Dict, Union, Tuple

import numpy as np
import pandas as pd

from ...utils.logger import logger
from ..cache.cache import CacheSet
from ...utils import const as con
from ...utils.common import FIFOData, get_bound, TimeSeriesType


class SuppressorPipeline:
    def __init__(self, name, params):
        self.name = name
        self.params = params
        self.suppressor_dict = {
            "ContinuousAnomalySuppressor": ContinuousAnomalySuppressor,
            "TransientAnomalySuppressor": TransientAnomalySuppressor,
            "VariationRatioSuppressor": VariationRatioSuppressor,
            "LowerBoundSuppressor": LowerBoundSuppressor,
        }
        self.pipe = self._construct_pipe()

    def _construct_pipe(self):
        pipe = []
        if self.params is not None:
            for key, value in self.params.items():
                if key in self.suppressor_dict.keys():
                    pipe.append(self.suppressor_dict.get(key)(self.name, value))
        return pipe

    def suppress(self, time_series: TimeSeriesType) -> TimeSeriesType:

        labels = time_series.get(con.LABEL)
        ori_data = time_series.get(con.ORIGIN)
        for pipe in self.pipe:
            if isinstance(pipe, LabelSuppressor):
                labels = pipe.suppress(labels)
            elif isinstance(pipe, NumberSuppressor):
                labels = pipe.suppress(labels, ori_data)
        time_series[con.LABEL] = labels
        return time_series

    def set_name(self, name: str) -> None:
        self.name = name
        for pipe in self.pipe:
            pipe.set_name(name)


class LabelSuppressor(ABC):
    def __init__(self, name, params):
        self.name = name
        self.params = params

    @abstractmethod
    def suppress(self, label_df: pd.DataFrame):
        pass

    def set_name(self, name: str) -> None:
        self.name = name


class ContinuousAnomalySuppressor(LabelSuppressor):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.gap = self.params[con.GAP]
        try:
            self.gap = pd.Timedelta(self.gap)
        except ValueError as e:
            logger.error("%s, invalid gap parameter in ContinuousAnomalySuppressor", e)
            raise e
        self.cache = CacheSet().get_cache(con.SUPPRESS_CACHE)
        self.cache_name = self.name + "_" + self.__class__.__name__

    def suppress(self, label_df: pd.DataFrame):
        if not self.gap:
            return label_df
        target_columns = label_df.columns[label_df.any()]

        start_index = label_df.index[0]
        if target_columns.empty:
            return label_df
        target_label_df: pd.DataFrame = label_df.loc[:, target_columns]
        last_anomaly_index_dict = self._get_cache_values(target_columns)
        total_suppress_anomaly_len = 0
        all_suppress_anomaly_titles = []
        for title, value in target_label_df.items():
            cache_title_name = self.cache_name + str(title)
            last_anomaly_index = last_anomaly_index_dict.get(cache_title_name)
            last_anomaly_index, suppress_anomaly_len = self._suppress_single(
                label_series=value, last_anomaly_index=last_anomaly_index
            )

            if suppress_anomaly_len:
                total_suppress_anomaly_len += suppress_anomaly_len
                all_suppress_anomaly_titles.append(str(title))

            last_anomaly_index_dict[cache_title_name] = last_anomaly_index

        self._update_cache_values(last_anomaly_index_dict)
        label_df.loc[:, target_columns] = target_label_df
        if total_suppress_anomaly_len:
            logger.debug(
                "In [%s] algorithm, [%s] totally suppress continuous anomalies: %s",
                self.name,
                ", ".join(all_suppress_anomaly_titles),
                total_suppress_anomaly_len,
            )

        return label_df.loc[start_index:, :]

    def _suppress_single(
        self, label_series: pd.Series, last_anomaly_index: pd.Timestamp
    ) -> Tuple[pd.Timestamp, int]:

        suppress_anomaly_len = 0
        indexes = label_series.index[label_series]
        for index in indexes:
            if last_anomaly_index is not None and (
                index - last_anomaly_index <= self.gap
            ):
                label_series[index] = False
                suppress_anomaly_len += 1
            else:
                last_anomaly_index = index

        return last_anomaly_index, suppress_anomaly_len

    def _get_cache_values(self, columns):
        return {
            self.cache_name + str(col): self.cache.get_value(self.cache_name + str(col))
            for col in columns
        }

    def _update_cache_values(self, sub_cache_dict):
        self.cache.update(sub_cache_dict)

    def set_name(self, name: str) -> None:
        self.name = name
        self.cache_name = self.name + self.__class__.__name__


class TransientAnomalySuppressor(LabelSuppressor):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.window = self.params[con.WINDOW]
        self.anomalies = self.params["anomalies"]
        self.cache = CacheSet().get_cache(con.SUPPRESS_CACHE)
        self.cache_name = self.name + "_" + self.__class__.__name__

    def _update_cache_values_for_normal_columns(self, df_label, target_columns):
        titles = df_label.columns
        data = df_label.values
        transpose = data.transpose()

        for col, value in zip(titles, transpose):
            if col not in target_columns:
                self._update_cache(value, col)

    def _get_value_with_cache(self, data: np.array, title):
        """
        concat data and cache
        the structure of cache is dict: {self.name + str(title): np.array}
        """
        his_data = self.cache.get_value(self.cache_name + str(title))
        if his_data is not None:
            return np.append(his_data.get_filling_data(), data)
        else:
            return data

    def suppress(self, label_df: pd.DataFrame) -> pd.DataFrame:
        if self.anomalies <= 1 or self.window <= 1:
            return label_df
        target_columns = label_df.columns[label_df.any()]
        self._update_cache_values_for_normal_columns(label_df, target_columns)
        if target_columns.empty:
            return label_df
        start_index = label_df.index[0]
        indexes = label_df.index
        target_label_df = label_df.loc[:, target_columns]
        target_result_label_list = []
        before_suppress: pd.Series = target_label_df.sum()
        for title, data in target_label_df.items():
            concat_data = self._get_value_with_cache(data.values, title)
            target_result_label_list.append(self._suppress_single(concat_data, indexes))
            self._set_cache(concat_data, title)

        target_result_label = pd.concat(target_result_label_list, axis=1)
        target_result_label.columns = target_columns

        after_suppress: pd.Series = target_result_label.loc[start_index:].sum()

        diff_suppress = before_suppress - after_suppress
        total_suppress_anomaly_len = diff_suppress.sum()
        diff_index = diff_suppress.index[diff_suppress.astype(bool)]
        label_df.loc[start_index:, target_columns] = target_result_label
        if total_suppress_anomaly_len:
            logger.debug(
                "In [%s] algorithm, [%s] totally suppress transient anomalies: %s",
                self.name,
                ", ".join(map(str, diff_index)),
                total_suppress_anomaly_len,
            )

        return label_df.loc[start_index:, :]

    def _suppress_single(self, data: np.array, indexes):
        roll_data = np.convolve(data, np.ones(self.window, dtype=int))[
            : -self.window + 1
        ]
        result = pd.Series(data & (roll_data >= self.anomalies))[-len(indexes) :]
        result.index = indexes
        return result

    def _update_cache(self, label_np: np.array, title: Hashable):
        col_data = self._get_cache(self.cache_name + str(title))
        col_data.update(label_np)

    def _set_cache(self, label_np: np.array, title: Hashable):
        col_data = self._get_cache(self.cache_name + str(title))
        col_data.set_data(label_np)

    def _get_cache(self, key):
        data = self.cache.get_value(key)
        if data is None:
            data = FIFOData(self.window + 1, array_type=bool)
            self.cache.set_value(key, data)
        return data

    def set_name(self, name: str) -> None:
        self.name = name
        self.cache_name = self.name + self.__class__.__name__


class NumberSuppressor(ABC):
    def __init__(self, name, params):
        self.name = name
        self.params = params

    @abstractmethod
    def suppress(self, label_df: pd.DataFrame, ori_data: pd.DataFrame):
        pass

    def set_name(self, name: str) -> None:
        self.name = name


class VariationRatioSuppressor(NumberSuppressor):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.history_length = self.params["history_length"]
        self.threshold = self.params["threshold"]

    def suppress(self, label_df: pd.DataFrame, ori_data: pd.DataFrame) -> pd.DataFrame:
        target_columns = label_df.columns[label_df.any()]

        if target_columns.empty:
            return label_df

        if len(ori_data) <= 1:
            logger.info(
                "in variation ratio suppressing, empty history data is encountered"
            )
            return label_df

        target_label_df: pd.DataFrame = label_df.loc[:, target_columns]

        try:
            ori_data = ori_data[target_columns]
        except KeyError as e:
            logger.info("%s, target_columns not exist in original data", e)
            return label_df

        total_suppress_anomaly_len = 0
        all_suppress_anomaly_titles = []
        for col, value in target_label_df.items():
            anomaly_len = self._variation_portion_suppress(value, ori_data, col)
            if anomaly_len:
                total_suppress_anomaly_len += anomaly_len
                all_suppress_anomaly_titles.append(str(col))

        if total_suppress_anomaly_len:
            logger.debug(
                "In [%s] algorithm, [%s] totally suppress variation ratio anomalies: %s",
                self.name,
                ", ".join(all_suppress_anomaly_titles),
                total_suppress_anomaly_len,
            )

        label_df.loc[:, target_columns] = target_label_df
        return label_df

    @staticmethod
    def _variation_portion(history: np.ndarray, target: np.float64) -> np.ndarray:
        # history: (time, dim), max, min array: (1, dim)
        max_array = np.max(history)
        min_array = np.min(history)

        return np.max(
            (
                np.abs(target - max_array) / (np.abs(max_array) + 1e-9),
                np.abs(target - min_array) / (np.abs(min_array) + 1e-9),
            )
        )

    def _variation_portion_suppress(
        self, label_series: pd.Series, ori_data: pd.DataFrame, title
    ) -> int:
        """
        this function takes in the pandas dataframe consisting of original data
        anomaly score, and label along with other columns.

        it suppresses anomaly label if the original data's point's relative variation
        percentage corresponding to that label is smaller than a threshold.
        """
        # data pandas dataframe, (time, columns: [original data, label])
        # for data columns, the columns title are tuples with "value" in them
        anomaly_indexes = label_series.index[label_series].values
        pos_indexes_bool = [
            ori_data_index in anomaly_indexes
            for ori_data_index in ori_data.index.values
        ]
        pos_indexes = np.where(pos_indexes_bool)[0]

        anomaly_length = 0
        ori_series = ori_data[title].values
        for pos_index, time_index in zip(pos_indexes, anomaly_indexes):
            try:

                history = ori_series[
                    max(pos_index - self.history_length, 0) : pos_index
                ]

                target = ori_series[pos_index]
                if history.size > 0:
                    variation = self._variation_portion(history, target)
                    if variation < self.threshold:
                        label_series[time_index] = False
                        anomaly_length += 1
                else:
                    logger.info(
                        "in variation ratio suppressing, empty history data is encountered"
                    )
            except KeyError as e:
                logger.info("%s, time index not exist in original data", e)
        return anomaly_length


class LowerBoundSuppressor(NumberSuppressor):
    """
    suppress anomalies with original value lower than upper bound or higher than lower bound
    anomalies whose original value lies in the region between upper and lower bound shall be
    suppressed
    """

    def __init__(self, name, params):
        super().__init__(name, params)
        self.lb_scalar = self.params.get(con.LOWER_BOUND)
        self.lb_dict = self.params.get(con.LOWER_BOUND_KV)
        self.ub_scalar = self.params.get(con.UPPER_BOUND)
        self.ub_dict = self.params.get(con.UPPER_BOUND_KV)

    @staticmethod
    def _get_bound(
        target_columns: pd.Index,
        bound_dict: Dict,
        bound_scalar: Union[float, int, list],
    ):
        if bound_dict:
            bound = get_bound(
                threshold_dict=bound_dict,
                title=target_columns,
                threshold_scalar=bound_scalar,
            )
        else:
            if bound_scalar is not None:
                if not isinstance(bound_scalar, list):
                    bound_scalar = [bound_scalar] * len(target_columns)
                bound = dict(zip(target_columns, bound_scalar))

            else:
                bound = {}
        return bound

    def suppress(self, label_df: pd.DataFrame, ori_data: pd.DataFrame):

        target_columns = label_df.columns[label_df.any()]
        if target_columns.empty:
            return label_df

        target_label_df: pd.DataFrame = label_df.loc[:, target_columns]
        before_suppress: pd.Series = target_label_df.sum()
        try:
            ori_data = ori_data[target_columns]
        except KeyError as e:
            logger.info("%s, target_columns not exist in original data", e)
            return label_df

        lb = self._get_bound(
            target_columns=target_columns,
            bound_dict=self.lb_dict,
            bound_scalar=self.lb_scalar,
        )
        ub = self._get_bound(
            target_columns=target_columns,
            bound_dict=self.ub_dict,
            bound_scalar=self.ub_scalar,
        )

        compare_ori_data = ori_data.loc[target_label_df.index, target_columns]
        if lb and ub:
            target_label_df &= (compare_ori_data <= lb) | (compare_ori_data >= ub)
        elif lb:
            target_label_df &= compare_ori_data <= lb
        else:
            target_label_df &= compare_ori_data >= ub
        after_suppress: pd.Series = target_label_df.sum()

        diff_suppress = before_suppress - after_suppress
        total_suppress_anomaly_len = diff_suppress.sum()
        diff_index = diff_suppress.index[diff_suppress.astype(bool)]
        if total_suppress_anomaly_len:
            logger.debug(
                "In [%s] algorithm, [%s] totally suppress lower bound anomalies: %s",
                self.name,
                ", ".join(map(str, diff_index)),
                total_suppress_anomaly_len,
            )
        label_df.loc[:, target_columns] = target_label_df

        return label_df
