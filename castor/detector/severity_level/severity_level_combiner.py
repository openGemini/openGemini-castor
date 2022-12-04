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

# Determining the anomaly severity level by multiple severity level module

from __future__ import absolute_import
from functools import reduce

import numpy as np

from .severity_level import SeverityLevelByAlgo, SeverityLevelByHistoryAnomaly
from ...utils import const as con
from ...utils.common import TimeSeriesType


class SeverityLevelCombiner:
    def __init__(self, name: str, params: dict):
        """
        :param name: series key
        :param params: parameters to select method and give the special method parameters
        """
        self.name = name
        self.params = params
        self.severity_level_dict = {
            con.ALGO: SeverityLevelByAlgo,
            con.HIS_ANOMALY: SeverityLevelByHistoryAnomaly,
        }
        self.pipe = self._construct_pipe()

    def run(self, time_series: TimeSeriesType, kwargs: dict) -> TimeSeriesType:
        """
        combine different methods to determining the anomaly severity level.
        add anomaly level information into time_series, presents anomaly severity level for different field
        If the param of the module is not exist, there are no values of anomalyLevel will be added into time_series.

        :param time_series: label results of anomaly.
        :param kwargs: other information.

        :return: time_series, which is a dict of detection information.
        The values of anomalyLevel in time_series are added in this function.
        There are 3 levels of anomaly severity. The higher the level, the more severe the anomaly is.
            one example of the element for anomalyLevel key in time_series:
                'anomalyLevel':                        field1    field2
                            time
                            2020-01-01 10:21:00           0.0        1.0
                            2020-01-01 10:37:15           0.0        0.0
                            2020-01-01 10:55:30           0.0 ,      0.85
        """
        if not self.pipe:
            return time_series
        labels = time_series.get(con.LABEL)
        indexes = labels.index
        columns = labels.columns
        labels_np = labels.values
        detect_result_columns_with_anomaly = np.any(labels_np, axis=0)
        columns = columns[detect_result_columns_with_anomaly]
        levels = labels.copy().astype(float) - 1
        if not columns.empty:
            labels_np = labels_np[:, detect_result_columns_with_anomaly].copy()
            transpose = labels_np.transpose()
            for col, value in zip(columns, transpose):
                indexes_tmp = indexes[value]
                kwargs["field"] = str(col)
                level_result = reduce(
                    lambda x, y: np.maximum(
                        x.run(indexes_tmp, kwargs), y.run(indexes_tmp, kwargs)
                    ),
                    self.pipe,
                )
                levels.loc[indexes_tmp, col] = level_result
            time_series[con.LEVEL] = levels

        return time_series

    def _construct_pipe(self):
        """
        construct custom pipe with params
        """
        pipe = []
        if self.params is not None:
            for key, value in self.params.items():
                if key in self.severity_level_dict.keys():
                    pipe.append(self.severity_level_dict.get(key)(self.name, value))
        return pipe
