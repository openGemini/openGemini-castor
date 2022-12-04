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

from .stream_filter.get_latest_data_module import LatestData, get_latest_index
from .thresholder.thresholder import ThresholderModule
from ..utils import common, const as con
from ..utils.common import TimeSeriesType


class DIFFERENTIATEAD:
    def __init__(self, name, hyper_parameters):
        self.name = name
        self._hyper_params = hyper_parameters
        self.thresholder = ThresholderModule(
            self.name, hyper_parameters.get(con.DYNAMIC_THRESHOLD)
        )
        common.ALGO_WINDOW.append(self._hyper_params.get(con.WINDOW))
        self.latest_data = LatestData()

    @staticmethod
    def fit(data: pd.DataFrame) -> None:
        return None

    def detect(self, time_series: TimeSeriesType) -> TimeSeriesType:
        # get anomaly scores
        data = time_series.get(con.ORIGIN)
        latest_index = get_latest_index(data.columns)
        data = self.latest_data.get_data(
            data, latest_index, self._hyper_params.get(con.WINDOW)
        )
        score = self._get_score(data)
        time_series[con.LABEL] = self.thresholder.thresholder(score)
        return time_series

    def _get_score(self, s):
        window_list = list(range(1, self._hyper_params[con.WINDOW] + 1))
        score_multi_dim = sum((s.diff(i).abs() for i in window_list)).dropna()
        return score_multi_dim

    def dump_model(self):
        pass

    def load_model(self, model_params):
        pass

    def set_name(self, name: str) -> None:
        self.thresholder.set_name(name)
        self.name = name
