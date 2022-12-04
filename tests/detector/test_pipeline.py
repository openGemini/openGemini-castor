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
import os
import json

import pytest
import pandas as pd
import numpy as np

from castor.utils.base_functions import load_params_from_yaml
from castor.detector.pipeline_detector import PipelineDetector
from castor.utils import const as con
from castor.utils import logger as llogger
from castor.detector.cache.organize_cache import clear_cache

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
TESTS_PATH = os.path.split(CURRENT_PATH)[0]
CONF_PATH = os.path.join(TESTS_PATH, "conf")
DATA_PATH = os.path.join(TESTS_PATH, "data")
with open(os.path.join(TESTS_PATH, "conf", "test.json")) as f:
    tests_params = json.load(f)
llogger.basic_config(level="DEBUG")

algo = [
    con.DIFFERENTIATE_AD,
    con.BATCH_DIFFERENTIATE_AD,
    con.INCREMENTAL_AD,
    con.THRESHOLD_AD,
    con.VALUE_CHANGE_AD,
]


def curve_generator(n_point, n_dim):
    Gx = np.ones((n_point, n_dim))
    Gx[:300, 0] = np.zeros_like(Gx[:300, 0])
    Gx[-680:, 0] = np.zeros_like(Gx[-680:, 0])

    return Gx


@pytest.fixture(params=tests_params.get("ALL_MODELS"))
def params(request):
    return request.param


class TestPipeline:
    @pytest.fixture()
    def env_ready_fit_detect(self, params):
        self.tear_up_fit_detect(params)
        yield
        self.tear_down()

    def tear_up_fit_detect(self, params):
        self.config = os.path.join(CONF_PATH, params.get("conf_file"))
        self.params = load_params_from_yaml(config_file=self.config)
        self.data_file = os.path.join(DATA_PATH, params.get("data_file"))
        df = pd.read_csv(self.data_file, index_col="time", parse_dates=True)
        df.index = df.index.tz_localize(None)
        self.df = df.to_frame() if isinstance(df, pd.Series) else df
        self.len_df = self.df.shape[0]
        self.log_file = params.get("log_file")
        self.model_size = params.get("model_size")

    def tear_down(self):
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        if os.path.exists(self.model_file):
            os.remove(self.model_file)

    @pytest.mark.usefixtures("env_ready_fit_detect")
    @pytest.mark.parametrize("algorithm", algo)
    @pytest.mark.important
    @pytest.mark.run(order=1)
    def test_fit_detect(self, params, algorithm):
        self.model_file = algorithm
        self.fit_task(algorithm)
        self.detect_task(algorithm)

    def fit_task(self, algorithm):
        # fit model
        model = PipelineDetector(algo=[algorithm], params=self.params)
        model.fit(self.df[0:500])

        # dump model
        model.dump_model_file(self.model_file)
        if algorithm not in con.NON_TRAINABLE_AD:
            assert os.path.isfile(self.model_file)
            model_size = os.path.getsize(self.model_file) // 1024
            assert model_size <= self.model_size
        clear_cache()

    def detect_task(self, algorithm):

        model = PipelineDetector(algo=[algorithm], params=self.params)
        model.load_model_file(self.model_file)
        # check model parameters
        assert hasattr(model, "_params") and model._params is not None
        assert model.algo[0] == algorithm
        assert model._params.get(con.ANOMALY_SUPPRESS) is not None
        test_x = self.df
        self.predicted = model.run(test_x)
        # check columns
        assert con.LEVEL in self.predicted[0]
        assert con.ORIGIN in self.predicted[0]

        clear_cache()
