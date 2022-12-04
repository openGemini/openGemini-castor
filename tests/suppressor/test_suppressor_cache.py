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
from typing import Tuple
import os
import json

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from castor.detector.suppressor.suppressor import SuppressorPipeline
from castor.utils import logger as llogger
from castor.utils import const as con
from castor.detector.cache.cache import CacheSet
from castor.detector.cache.organize_cache import clear_cache

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
TESTS_PATH = os.path.split(CURRENT_PATH)[0]
CONF_PATH = os.path.join(
    Path(__file__).parents[2], "demo", "demo_conf", "detect_base.yaml"
)
DATA_PATH = os.path.join(TESTS_PATH, "data")
with open(os.path.join(TESTS_PATH, "conf", "test.json")) as f:
    tests_params = json.load(f)
llogger.basic_config(level="DEBUG")

suppress_module = ["transient", "continuous"]
if_anomaly = [True, False]

params = {
    "transient": {
        "cache_length": 30,
        "TransientAnomalySuppressor": {con.WINDOW: 5, "anomalies": 3},
    },
    "continuous": {
        "cache_length": 30,
        "ContinuousAnomalySuppressor": {con.GAP: "10D"},
    },
}

result = {"transient": {True: 1, False: 0}, "continuous": {True: 1, False: 0}}


def pandas_wrap(data: np.ndarray, data_type: str):
    if data_type == "detect_result":
        return pd.DataFrame(
            data,
            index=pd.date_range(start="2021-01-02", periods=len(data), freq="1D"),
            columns=["lacolumn"],
        )
    elif data_type == "ori_data":
        return pd.DataFrame(
            data,
            index=pd.date_range(start="2021-01-02", periods=len(data), freq="1D"),
            columns=["lacolumn"],
        )
    else:
        return None


def data_generation(
    module: str, status: bool, length: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    label = np.array([False] * length).reshape(-1, 1)
    ori_data = np.ones((length, 1))
    if module == "transient":
        label[50:60, 0] = True
        if status:
            label[62, 0] = True
        else:
            label[63, 0] = True
    elif module == "continuous":
        label[58, 0] = True
        if status:
            label[70, 0] = True
        else:
            label[63, 0] = True

    return pandas_wrap(ori_data, data_type="ori_data"), pandas_wrap(
        label, data_type="detect_result"
    )


@pytest.mark.usefixtures("env_ready")
class TestSuppressor:
    @pytest.fixture()
    def env_ready(self):
        self.tear_up()
        yield
        self.tear_down()

    def tear_up(self):
        pass

    def tear_down(self):
        clear_cache()

    @pytest.mark.parametrize("module", suppress_module)
    @pytest.mark.parametrize("if_anomaly_bool", if_anomaly)
    def test_suppress(self, module, if_anomaly_bool):
        suppressor_name = {
            "transient": "TransientAnomalySuppressor",
            "continuous": "ContinuousAnomalySuppressor",
        }
        cache_length = {"transient": 1, "continuous": 1}
        cache = CacheSet().get_cache(con.SUPPRESS_CACHE)
        data, detect_results = data_generation(module, if_anomaly_bool, 100)
        suppressor = SuppressorPipeline(name="Gemini", params=params.get(module))
        _ = suppressor.suppress(
            {con.LABEL: detect_results.iloc[:30, :], con.ORIGIN: data.iloc[:30, :]}
        )
        _ = suppressor.suppress(
            {con.LABEL: detect_results.iloc[30:60, :], con.ORIGIN: data.iloc[30:60, :]}
        )
        print(f'result: {"Gemini" + suppressor_name.get(module) + "lacolumn"}')

        cache_result = [col for col in cache.keys() if str(col).startswith("Gemini")]
        assert len(cache_result) == cache_length.get(module)
        detect_results = suppressor.suppress(
            {con.LABEL: detect_results.iloc[60:, :], con.ORIGIN: data.iloc[60:, :]}
        )
        print(f"detect_result: {detect_results}")
        print(f"SuppressCache: {CacheSet().get_cache(con.SUPPRESS_CACHE)._cache}")
        assert sum(detect_results.get(con.LABEL).iloc[:, 0]) == result.get(module).get(
            if_anomaly_bool
        )
