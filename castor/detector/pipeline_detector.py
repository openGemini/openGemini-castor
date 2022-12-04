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
from typing import List

import pandas as pd

from ..utils.base_functions import load_model_file_from_disk, dump_model_file_to_disk
from ..preprocessing.processing import PreProcess
from .pipeline.pipeline import Pipeline
from ..utils import common, const as con
from .stream_filter.get_latest_data_module import LatestData
from .cache.organize_cache import record_status_cache, remove_status_cache_with_symbol
from ..utils.common import TimeSeriesType


class PipelineDetector:
    def __init__(self, algo: List[str], params: dict = None):
        self.freq = None
        self.algo = algo
        self.pipe = []
        self._params = params
        self.preprocess_module = PreProcess(
            self._params.get(con.DATA_VALIDATE), self._params.get(con.DATA_PREPROCESS)
        )
        self.name_algorithm = []
        common.ALGO_WINDOW.clear()
        self._construct_pipe()
        self.max_window = max(common.ALGO_WINDOW) if common.ALGO_WINDOW else 0
        self.latest_data = LatestData()

    def _construct_pipe(self):
        for ind, sub_algo in enumerate(self.algo):
            self.pipe.append(
                Pipeline(
                    algo=sub_algo,
                    name=str(ind),
                    params=self._params,
                )
            )
            self.name_algorithm.append(str(ind))

    def fit(self, data: pd.DataFrame):
        data = self.preprocess_module.validate_preprocess(data, flag="fit")
        for sub_pipe in self.pipe:
            sub_pipe.fit(data)

    def run(self, data: pd.DataFrame) -> List[TimeSeriesType]:
        """
        detect anomaly by multiple algorithm
        :param data: the data to detect
        :return: a list of TimeSeriesType. One element presents data information for one algorithm
            example:
                [
                    {
                        'anomalyScore':                        field1    field2
                                    time
                                    2020-01-01 10:21:00           2.0        1
                                    2020-01-01 10:37:15           0.0        0
                                    2020-01-01 10:55:30           0.0 ,      0
                        'anomalyLevel':                        field1    field2
                                    ...
                     },
                     {
                        'anomalyScore':  ...
                     },
                     ....
                ]
        """
        record_status_cache(list(data.columns))
        data = self.preprocess_module.validate_preprocess(data, flag="detect")
        data = self.latest_data.filter_disorder_data(data)

        results = []
        for sub_pipe in self.pipe:
            time_series = {con.ORIGIN: data}
            algo_result = sub_pipe.run(time_series)
            results.append(algo_result)
        self.latest_data.update(self.max_window, data)
        remove_status_cache_with_symbol()
        self.freq = data.index.inferred_freq
        return results

    def fit_run(self, data: pd.DataFrame) -> List[TimeSeriesType]:
        self.fit(data)
        result = self.run(data)
        return result

    def dump_model_file(self, model_file):
        model_params = dict()
        for ind, sub_pipe in enumerate(self.pipe):
            sub_model_dict = sub_pipe.dump_model()
            if sub_model_dict:
                model_params[ind] = sub_model_dict
        dump_model_file_to_disk(model_file=model_file, model_params=model_params)

    def load_model_file(self, model_file):
        model_params = load_model_file_from_disk(model_file)
        for ind, sub_pipe in enumerate(self.pipe):
            sub_pipe.load_model(model_params.get(ind))
