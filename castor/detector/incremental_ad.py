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

import numpy as np

from .threshold_ad import ThresholdAD
from ..utils import const as con
from ..utils.exceptions import ValueNotEnoughError


class IncrementalAD(ThresholdAD):
    def __init__(self, name, hyper_parameters):
        self.window_size = hyper_parameters.get("window_size")
        self.window_number = hyper_parameters.get("window_number")
        hyper_parameters[con.WINDOW] = (self.window_number + 1) * self.window_size - 1

        super().__init__(name, hyper_parameters)
        self.name = name + con.INCREMENTAL_AD

    def set_name(self, name: str) -> None:
        self.name = name + con.INCREMENTAL_AD

    def _get_label(self, data, ub, lb):
        if len(data) < self.window_size * (self.window_number + 1):
            raise ValueNotEnoughError(
                "in incremental detection, %s with sampling freq: %s"
                "does not have enough value for detection"
                % (self.name, data.index.inferred_freq)
            )
        incremental_ub, incremental_lb, start_index = self._incremental_bound(
            data, self.window_size, self.window_number, ub, lb
        )
        if incremental_ub is not None and incremental_lb is not None:
            label = np.logical_or(incremental_ub, incremental_lb)
        elif incremental_ub is not None:
            label = incremental_ub
        elif incremental_lb is not None:
            label = incremental_lb
        else:
            label = data.iloc[start_index:, :] * 0
        return label.astype(bool)

    @staticmethod
    def _incremental_bound(data, window_size, window_number, ub, lb):
        label_ub = None
        label_lb = None
        data_max = data.rolling(window_size).max().diff(window_size)
        data_min = data.rolling(window_size).min().diff(window_size)
        start_index = window_size * (window_number + 1) - 1
        sampler = np.array([0 - i * window_size for i in range(window_number)])
        if ub is not None:
            label_ub = data.copy()
            for i in range(start_index, len(data)):
                label_ub.iloc[i, :] = np.logical_and(
                    data_max.iloc[sampler + i, :].min(axis=0) > 0,
                    data_min.iloc[sampler + i, :].min(axis=0) > 0,
                )
            label_ub = np.logical_and(
                label_ub.iloc[start_index:, :],
                ((data - ub).rolling(window_size).min() > 0).iloc[start_index:, :],
            )
        if lb is not None:
            label_lb = data.copy()
            for i in range(start_index, len(data)):
                label_lb.iloc[i, :] = np.logical_and(
                    data_max.iloc[sampler + i, :].max(axis=0) < 0,
                    data_min.iloc[sampler + i, :].max(axis=0) < 0,
                )
            label_lb = np.logical_and(
                label_lb.iloc[start_index:, :],
                ((data - lb).rolling(window_size).max() < 0).iloc[start_index:, :],
            )

        return label_ub, label_lb, start_index
