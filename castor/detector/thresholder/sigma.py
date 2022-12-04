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
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np


class SigmaBase(ABC):
    def __init__(self, name, **params):
        self.name = name
        self._sigma = params.get("sigma", 4)

    def threshold(self, score: pd.DataFrame) -> pd.DataFrame:
        return self._sigma_detector(data=score)

    @abstractmethod
    def _get_threshold(self, data: pd.DataFrame) -> (np.ndarray, np.ndarray):
        pass

    def _sigma_detector(self, data: pd.DataFrame) -> pd.DataFrame:
        # data is required to be the individual score of individual columns
        upper_threshold, lower_threshold = self._get_threshold(data)
        label = (data > upper_threshold) | (data < lower_threshold)
        return label


class SigmaThresholder(SigmaBase):
    def _get_threshold(self, data: pd.DataFrame) -> (np.ndarray, np.ndarray):
        """
        get threshold
        threshold: np.ndarray
        """
        mean = np.mean(data, axis=0)
        std = np.std(data, axis=0)
        upper_threshold = mean + self._sigma * std
        lower_threshold = mean - self._sigma * std

        return upper_threshold, lower_threshold
