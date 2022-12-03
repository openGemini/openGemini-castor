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

import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_series_equal

from castor.utils import logger as llogger
from castor.detector.cache.cache import CacheSet
from castor.detector.severity_level.severity_level import (
    SeverityLevelByAlgo,
    SeverityLevelByHistoryAnomaly,
)
from castor.detector.severity_level.severity_level_combiner import (
    SeverityLevelCombiner,
)
from castor.utils import const as con
from castor.detector.cache.organize_cache import clear_cache


llogger.basic_config()
params = {
    con.ALGO: {"ThresholdAD": 1, "Incremental": 0, "DIFFERENTIATEAD": 0.85},
    "his_anomaly": {con.GAP: "2D"},
}
name = "severity_level"
indexes = pd.date_range(start="2022-6-10", freq="T", periods=6)


class TestSeverityLevel:
    def tear_up(self):
        self.data = {"0": indexes[[0, 2, 5]], "2": indexes[[2]]}
        algo_result = {"0": [0.85, 0.85, 0.85], "2": [0.85]}
        his_freq_result = {"0": [1, 0, 0], "2": [0]}
        self.result = {con.ALGO: algo_result, "his_anomaly": his_freq_result}
        self.severity_level_class = {
            con.ALGO: SeverityLevelByAlgo,
            "his_anomaly": SeverityLevelByHistoryAnomaly,
        }
        init_severity_cache()

    @pytest.fixture()
    def env_ready(self):
        self.tear_up()
        yield
        clear_cache()

    @pytest.mark.usefixtures("env_ready")
    def test_severity_level_by_algo(self):
        param = con.ALGO
        severity_level_determiner = self.severity_level_class.get(param)(
            name, params.get(param)
        )
        actual = severity_level_determiner.run(
            self.data.get("0"), kwargs={con.ALGO: "DIFFERENTIATEAD"}
        )
        result = self.result.get(param).get("0")
        assert (actual == np.array(result)).all()

        actual = severity_level_determiner.run(
            self.data.get("2"), kwargs={con.ALGO: "DIFFERENTIATEAD"}
        )
        result = self.result.get(param).get("2")
        assert (actual == np.array(result)).all()

    @pytest.mark.usefixtures("env_ready")
    def test_severity_level_by_his_anomaly(self):
        param = con.HIS_ANOMALY
        severity_level_determiner = self.severity_level_class.get(param)(
            name, params.get(param)
        )
        actual = severity_level_determiner.run(
            self.data.get("0"), kwargs={"field": "0"}
        )
        result = self.result.get(param).get("0")

        assert (actual == np.array(result)).all()

        actual = severity_level_determiner.run(
            self.data.get("2"), kwargs={"field": "2"}
        )
        result = self.result.get(param).get("2")
        llogger.logger.info(f"result: {result}, {actual}")
        assert (actual == np.array(result)).all()


def init_severity_cache():
    cache_value = {
        "0": pd.to_datetime("2022-6-07 11:20:00"),
        "1": pd.to_datetime("2022-6-09 23:20:00"),
        "2": pd.to_datetime("2022-6-09 10:20:00"),
    }
    for col, cache_value_col in cache_value.items():
        CacheSet().get_cache("SeverityLevelCache").set_value(
            name + "_" + SeverityLevelByHistoryAnomaly.__name__ + col, cache_value_col
        )


class TestSeverityLevelCombiner:
    def tear_up(self):
        data = [
            [True, False, False, False],
            [False, False, False, False],
            [True, False, True, False],
            [False, False, False, False],
            [False, False, False, False],
            [True, False, False, False],
        ]
        self.data = pd.DataFrame(
            data, index=indexes, columns=[str(i) for i in range(4)]
        )
        result = [
            [1, -1, -1, -1],
            [-1, -1, -1, -1],
            [0.85, -1, 0.85, -1],
            [-1, -1, -1, -1],
            [-1, -1, -1, -1],
            [0.85, -1, -1, -1],
        ]
        self.result = pd.DataFrame(
            result, index=indexes, columns=[str(i) for i in range(4)], dtype="float64"
        )

        init_severity_cache()

    @pytest.fixture()
    def env_ready(self):
        self.tear_up()
        yield
        clear_cache()

    @pytest.mark.usefixtures("env_ready")
    def test_severity_level_combiner(self):
        severity_level_combiner = SeverityLevelCombiner(name, params)
        actual_kv = severity_level_combiner.run(
            {con.LABEL: self.data}, {con.ALGO: "DIFFERENTIATEAD"}
        )
        for actual, value in zip(self.result.items(), actual_kv.get(con.LEVEL).items()):
            assert_series_equal(value[1].asfreq("T"), actual[1].asfreq("T"))

    @pytest.mark.usefixtures("env_ready")
    def test_severity_level_combiner_with_none_param(self):
        severity_level_combiner = SeverityLevelCombiner(name, None)
        actual_kv = severity_level_combiner.run(
            {con.LABEL: self.data}, {con.ALGO: "DIFFERENTIATEAD"}
        )
        assert con.LEVEL not in actual_kv
