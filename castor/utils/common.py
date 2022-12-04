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
from typing import Dict
import re

import pandas as pd
import numpy as np


def get_freq(freq: str) -> int:
    """
    Get frequency of time series.
    -------
    """
    # freq不是字符串，freq为0
    if not isinstance(freq, str):
        return 0
    # freq includes "S"/"10S" , "T"/"5T" or "H"/"5H"
    frequency_map = dict(
        [
            ["S", 1000],
            ["T", 60 * 1000],
            ["M", 60 * 1000],
            ["H", 3600 * 1000],
            ["D", 24 * 3600 * 1000],
        ]
    )
    _pattern = re.compile("^[0-9]{0,10}[STMHD]$")
    # 没有匹配，freq为0
    if re.match(_pattern, freq) is None:
        return 0

    if freq[:-1] == "":
        return frequency_map.get(freq[-1])
    else:
        return int(freq[:-1]) * frequency_map.get(freq[-1])


def ava(ema, data, window=10):
    alpha = 2 / float(window + 1)
    ema = ema + alpha * (data - ema)
    return ema


def monotonic_concatenate(original, adder):
    # monotonic_conatenate ensures that the conatenation is monotonic in index
    # also, this function deletes all duplicated index
    # when there are duplications and non-monotonic parts, the function keeps the
    # index of the original.
    # original and adder's index both have to be monotonic
    if original.index[0] in adder.index:
        adder = adder.drop(index=original.index[0])
    if original.index[-1] in adder.index:
        adder = adder.drop(index=original.index[-1])
    return pd.concat(
        (
            adder.loc[: original.index[0], :],
            original,
            adder.loc[original.index[-1] :, :],
        ),
        axis=0,
    )


def get_bound(title: pd.Index, threshold_dict: dict, threshold_scalar):
    return {key: threshold_dict.get(key, threshold_scalar) for key in title}


ALGO_WINDOW = []


class FIFOData:
    def __init__(self, max_len, array_type="float"):
        """
        The data in the array is ordered by the time
        The data may be less than array length if the data cache is not enough.
        The data_length means the real length of data
        """
        self._data = np.empty(max_len, dtype=array_type)
        self._length = 0
        self.size = max_len

    def get_data(self) -> np.array:
        return self._data

    def get_length(self) -> int:
        return self._length

    def update(self, data):
        data_length = len(data)
        free_length = self.size - self._length
        if self.size <= data_length:
            self._length = 0
        elif data_length > free_length:
            shift_length = data_length - free_length
            self._data[0 : self._length - shift_length] = self._data[
                shift_length : self._length
            ]
            self._length -= shift_length
        data_length = min(data_length, self.size)
        self._data[self._length : self._length + data_length] = data[-data_length:]
        self._length += data_length

    def get_filling_data(self):
        return self._data[: self._length]

    def set_data(self, data):
        data_length = len(data)
        if data_length < self.size:
            self._data[0:data_length] = data
            self._length = data_length
        else:
            self._data = data[-self.size :]
            self._length = self.size

    def is_full(self):
        return self._length >= self.size


class Singleton(object):
    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls(*args, **kwargs)
        return self._instance[self._cls]


TimeSeriesType = Dict[str, pd.DataFrame]
