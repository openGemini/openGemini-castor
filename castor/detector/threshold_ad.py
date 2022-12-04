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

from typing import Union

import numpy as np
import pandas as pd

from .stream_filter.get_latest_data_module import LatestData, get_latest_index
from ..utils import const as con, common
from ..utils.common import TimeSeriesType, get_bound


class ThresholdAD:
    def __init__(self, name, hyper_parameters):
        self.name = name + con.THRESHOLD_AD
        self._hyper_params = hyper_parameters
        self.ub_scalar = self._hyper_params.get(con.UPPER_BOUND)
        self.lb_scalar = self._hyper_params.get(con.LOWER_BOUND)
        self.ub_dict: Union[dict, None] = self._hyper_params.get(con.UPPER_BOUND_KV)
        self.lb_dict: Union[dict, None] = self._hyper_params.get(con.LOWER_BOUND_KV)
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
        ub, lb = self._get_bound(data)
        label = self._get_label(data, ub, lb)
        time_series[con.LABEL] = label
        return time_series

    def set_name(self, name: str) -> None:
        self.name = name + con.THRESHOLD_AD

    def _get_bound(self, data: pd.DataFrame):
        ub = (
            get_bound(
                threshold_dict=self.ub_dict,
                title=data.columns,
                threshold_scalar=self.ub_scalar,
            )
            if self.ub_dict
            else self.ub_scalar
        )
        lb = (
            get_bound(
                threshold_dict=self.lb_dict,
                title=data.columns,
                threshold_scalar=self.lb_scalar,
            )
            if self.lb_dict
            else self.lb_scalar
        )
        return ub, lb

    def _get_label(self, data, ub, lb):
        if None not in (ub, lb):
            label = np.logical_or(data > ub, data < lb)
        elif ub is not None:
            label = data > ub
        elif lb is not None:
            label = data < lb
        else:
            label = (data * 0).astype(bool)
        return label
