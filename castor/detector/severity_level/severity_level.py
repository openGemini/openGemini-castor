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

# Determining the anomaly severity level

from __future__ import absolute_import
from abc import ABC

import numpy as np
import pandas as pd

from ..cache.cache import CacheSet
from ...utils.logger import logger
from ...utils import const as con


class SeverityLevelBase(ABC):
    """
    Base class of determining anomaly severity level.
    """

    def __init__(self, name: str, params: dict):
        """
        :param name: series key
        :param params: parameter to define anomaly severity level method.
        """
        self.name = name
        self.params = params

    def run(self, anomaly_indexes: pd.DatetimeIndex, kwargs: dict) -> np.array:
        """Determine anomaly severity level and return the anomaly severity level results.
        The results retain the anomaly label of True, consisting of field name and anomaly severity level.

        :param anomaly_indexes: anomaly label results
            example: [t1, t2]

        :param kwargs: other information to determine the the anomaly severity level.

        :return: a dict of anomaly severity level for different field.
            example: [level1, level2]
        """


class SeverityLevelByAlgo(SeverityLevelBase):
    """determining the anomaly severity according to detection algorithm"""

    def run(self, anomaly_indexes: pd.DatetimeIndex, kwargs: dict) -> np.array:
        algo = kwargs.get(con.ALGO)
        value = self.params.get(algo, 0)
        severity_level_result = value * np.ones(len(anomaly_indexes))
        return severity_level_result


class SeverityLevelByHistoryAnomaly(SeverityLevelBase):
    """determining the anomaly severity according to frequency of history anomaly."""

    def __init__(self, name: str, params: dict):
        super().__init__(name, params)
        self.cache_name = self.name + "_" + self.__class__.__name__
        self.cache = CacheSet().get_cache(con.SEVERITY_LEVEL_CACHE)
        self.gap = self.params[con.GAP]
        try:
            self.gap = pd.Timedelta(self.gap)
        except ValueError as e:
            logger.error("%s, invalid gap parameter in ContinuousAnomalySuppressor", e)
            raise e

    def run(self, anomaly_indexes: pd.DatetimeIndex, kwargs: dict) -> np.array:
        field_name = kwargs.get("field")
        severity_level_result = np.zeros(len(anomaly_indexes))

        last_index = self.cache.get_value(self.cache_name + field_name)
        if last_index is None or anomaly_indexes[0] - last_index > self.gap:
            severity_level_result[0] = 1
        severity_level_result[1:] = np.where(np.diff(anomaly_indexes) > self.gap, 1, 0)
        self.cache.set_value(self.cache_name + field_name, anomaly_indexes[-1])

        return severity_level_result
