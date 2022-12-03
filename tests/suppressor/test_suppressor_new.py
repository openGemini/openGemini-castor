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

from castor.detector.suppressor.suppressor import (
    VariationRatioSuppressor,
    ContinuousAnomalySuppressor,
    TransientAnomalySuppressor,
    LowerBoundSuppressor,
    LabelSuppressor,
    SuppressorPipeline,
)
from castor.utils import logger as llogger, const as con
from castor.detector.cache.organize_cache import clear_cache

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
TESTS_PATH = os.path.split(CURRENT_PATH)[0]
CONF_PATH = os.path.join(
    Path(__file__).parents[2], "demo", "demo_conf", "detect_base.yaml"
)
DATA_PATH = os.path.join(TESTS_PATH, "data")
with open(os.path.join(TESTS_PATH, "conf", "test.json")) as f:
    tests_params = json.load(f)
llogger.basic_config(level="DEBUG")

suppress_module = ["transient", "continuous", "percentage", "lower_bound"]
if_anomaly = [True, False]

suppressor_dict = {
    "transient": TransientAnomalySuppressor,
    "continuous": ContinuousAnomalySuppressor,
    "percentage": VariationRatioSuppressor,
    "lower_bound": LowerBoundSuppressor,
    "mix": SuppressorPipeline,
}

params = {
    "transient": {con.WINDOW: 5, "anomalies": 3},
    "continuous": {con.GAP: "6D"},
    "percentage": {"threshold": 0.05, "history_length": 50},
    "lower_bound": {con.UPPER_BOUND: [20]},
    "mix": {
        "cache_length": 100,
        "LowerBoundSuppressor": {con.UPPER_BOUND: 20},
        "VariationRatioSuppressor": {"threshold": 0.05, "history_length": 10},
        "TransientAnomalySuppressor": {con.WINDOW: 5, "anomalies": 3},
        "ContinuousAnomalySuppressor": {con.GAP: "10D"},
    },
}

result = {
    "transient": {True: 8, False: 0},
    "continuous": {True: 2, False: 1},
    "percentage": {True: 1, False: 0},
    "lower_bound": {True: 1, False: 0},
    "mix": {True: 2, False: 2},
}


def pandas_wrap(data: np.ndarray, data_type: str):
    if data_type == "detect_result":
        return pd.DataFrame(
            data,
            index=pd.date_range(start="2021-01-02", periods=len(data), freq="1D"),
            columns=[con.LABEL],
        )
    elif data_type == "ori_data":
        return pd.DataFrame(
            data,
            index=pd.date_range(start="2021-01-02", periods=len(data), freq="1D"),
            columns=[con.LABEL],
        )
    else:
        return None


def data_generation(
    module: str, status: bool, length: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    label = np.array([False] * length).reshape(-1, 1)
    score = np.ones((length, 1))
    ori_data = np.ones((length, 1))
    if module == "transient":
        if status:
            label[50:60, 0] = True
        else:
            label[50, 0] = True
    elif module == "continuous":
        if status:
            label[[50, 60], 0] = True
        else:
            label[[50, 55], 0] = True
    elif module == "percentage":
        label[50, 0] = True
        if status:
            ori_data[50, 0] = 20
        else:
            ori_data[50, 0] = 1.01
    elif module == "lower_bound":
        label[50, 0] = True
        if status:
            ori_data[50, 0] = 21
        else:
            ori_data[50, 0] = 10
    elif module == "mix":
        label[20:23, 0] = True
        label[40:43, 0] = True
        label[45:48, 0] = True
        label[60:63, 0] = True
        label[75, 0] = True
        label[80, 0] = True
        label[[90, 92, 93], 0] = True
        ori_data[20:40, 0] = 2
        ori_data[40:45, 0] = 100
        ori_data[45:60, 0] = 200
        ori_data[60:, 0] = 201
        ori_data[[75, 79, 90, 92, 93], 0] = 400

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

    @staticmethod
    def tear_down():
        clear_cache()

    @pytest.mark.parametrize("module", suppress_module)
    @pytest.mark.parametrize("if_anomaly_bool", if_anomaly)
    def test_suppress(self, module, if_anomaly_bool):
        data, detect_results = data_generation(module, if_anomaly_bool, 100)
        suppressor = suppressor_dict.get(module)(
            name="Gemini", params=params.get(module)
        )
        if isinstance(suppressor, LabelSuppressor):
            detect_results = suppressor.suppress(label_df=detect_results)
        else:
            detect_results = suppressor.suppress(label_df=detect_results, ori_data=data)
        assert sum(detect_results.get(con.LABEL)) == result.get(module).get(
            if_anomaly_bool
        )

    @pytest.mark.parametrize("if_anomaly_bool", if_anomaly)
    def test_mix_suppress(self, if_anomaly_bool):
        module = "mix"
        data, detect_results = data_generation(module, if_anomaly_bool, 100)
        suppressor = suppressor_dict.get(module)(
            name="Gemini", params=params.get(module)
        )
        detect_results = suppressor.suppress(
            {con.LABEL: detect_results, con.ORIGIN: data}
        )
        assert sum(detect_results.get(con.LABEL).iloc[:, 0]) == result.get(module).get(
            if_anomaly_bool
        )
