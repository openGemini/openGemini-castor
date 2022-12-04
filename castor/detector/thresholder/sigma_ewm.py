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

import numpy as np
import pandas as pd

from ..cache.cache import CacheSet
from ...utils import const as con
from .sigma import SigmaBase


class SigewmThresholder(SigmaBase):
    def __init__(self, name, **params):
        super().__init__(name, **params)
        self._window = params.get(con.WINDOW, 100)
        self.cache = CacheSet().get_cache(con.SIGMA_EWM_THRESHOLD_CACHE)

    def get_cache_values(self, columns, tmp_data):
        # his_status stores history (mu, sigma, length counter)
        # obtain cache by (col + name) key, and concat them into array
        result = np.zeros((len(columns), 3))

        for i, col in enumerate(columns):
            col_cache = self.cache.get_value(self.name + "_" + str(col))
            if col_cache is not None:
                result[i] = list(col_cache)
            else:
                result[i] = [tmp_data[i], 0, 0]
        return result[:, 0], result[:, 1], result[:, 2]

    def update_cache_values(self, columns, ema, emvar, counter):
        for i, col in enumerate(columns):
            self.cache.set_value(
                self.name + "_" + str(col), (ema[i], emvar[i], counter[i])
            )

    def _get_threshold(self, data: pd.DataFrame) -> (np.ndarray, np.ndarray):
        """
        get threshold for different columns
        update ema, emvar, counter from cache
        threshold: np.ndarray
        ema: np.ndarray-1d, counter: np.ndarray-1d, counter: np.ndarray-1d
        """
        # get ema, emvar, counter cache for dataframe columns. If the cache is None, get value from first row data.
        tmp_data = data.iloc[0, :].values
        ema, emvar, counter = self.get_cache_values(data.columns, tmp_data)

        upper_threshold = np.empty_like(data.values)
        lower_threshold = np.empty_like(data.values)
        alpha = 2 / float(self._window + 1)
        for s in range(len(data)):
            delta = data.iloc[s, :].values - ema
            ema = ema + alpha * delta
            emvar = (1 - alpha) * (emvar + alpha * delta**2)
            counter = np.minimum(np.array([self._window] * len(counter)), counter + 1)
            tmp = self._sigma * np.sqrt(emvar)
            upper_threshold[s, :] = np.where(
                counter == self._window,
                ema + tmp,
                float("inf"),
            )
            lower_threshold[s, :] = np.where(
                counter == self._window,
                ema - tmp,
                float("-inf"),
            )
        self.update_cache_values(data.columns, ema, emvar, counter)
        return upper_threshold, lower_threshold
