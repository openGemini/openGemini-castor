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

import pandas as pd

from .stream_filter.get_latest_data_module import LatestData, get_latest_index
from ..utils import const as con, common
from ..utils.common import TimeSeriesType


class ValueChangeAD:
    def __init__(self, name, hyper_parameters):
        self.name = name + con.VALUE_CHANGE_AD
        self._hyper_params = hyper_parameters
        common.ALGO_WINDOW.append(self._hyper_params.get(con.WINDOW))
        self.latest_data = LatestData()

    @staticmethod
    def fit(self, data: pd.DataFrame) -> None:
        return None

    def detect(self, time_series: TimeSeriesType) -> TimeSeriesType:
        data = time_series.get(con.ORIGIN)
        latest_index = get_latest_index(data.columns)
        data = self.latest_data.get_data(
            data, latest_index, self._hyper_params.get(con.WINDOW)
        )
        labels = ~data.eq(data.shift()).iloc[self._hyper_params.get(con.WINDOW) :]
        time_series[con.LABEL] = labels
        return time_series

    def set_name(self, name: str) -> None:
        self.name = name + con.VALUE_CHANGE_AD
