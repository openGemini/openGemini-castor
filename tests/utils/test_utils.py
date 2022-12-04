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

import pytest
import pandas as pd
import numpy as np

from castor.utils.common import monotonic_concatenate, get_freq


a = np.ones((10, 2))
a_df = pd.DataFrame(
    a, index=pd.date_range(start="2021-02-01", freq="1d", periods=len(a))
)
b = np.ones((20, 1))
b_df = pd.DataFrame(
    b, index=pd.date_range(start="2021-01-03", freq="1d", periods=len(b))
)

dataset = [(a_df, b_df)]

freqdat = [
    ["S", 1000],
    ["5S", 5000],
    ["T", 60 * 1000],
    ["5T", 60 * 1000 * 5],
    ["M", 60 * 1000],
    ["5M", 60 * 1000 * 5],
    ["H", 3600 * 1000],
    ["5H", 3600 * 1000 * 5],
    ["D", 24 * 3600 * 1000],
    ["5D", 24 * 3600 * 1000 * 5],
    ["12345678901D", 0],
    ["123423", 0],
    ["SSS", 0],
    [111, 0],
]


@pytest.mark.parametrize("data", dataset)
class TestUtils:
    def test_concate(self, data):
        original, adder = data
        result = monotonic_concatenate(original, adder)
        assert result.index.is_monotonic_increasing
        assert not result.index.duplicated().any()


@pytest.mark.parametrize("freqdat", freqdat)
class TestFreq:
    def test_get_freq(self, freqdat):
        assert freqdat[1] == get_freq(freqdat[0])
