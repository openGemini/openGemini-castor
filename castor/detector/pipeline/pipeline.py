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

from ...utils import const as con
from ..suppressor.suppressor import SuppressorPipeline
from ..incremental_ad import IncrementalAD
from ..threshold_ad import ThresholdAD
from ..value_change_ad import ValueChangeAD
from ..differentiate_ad import DIFFERENTIATEAD
from ..severity_level.severity_level_combiner import SeverityLevelCombiner
from ...utils.exceptions import ValueNotEnoughError
from ...utils.common import TimeSeriesType
from ...utils.exceptions import ParameterError
from ...utils.logger import logger
from ..cache.cache import CacheSet


class Pipeline:
    def __init__(
        self,
        algo,
        params,
        name,
    ):
        self.name = name
        self._params = params
        self.algo = algo
        self.pipeline = self._construct_pipe()

    def _construct_pipe(self):

        if self.algo in con.NON_TRAINABLE_AD:
            return NonTrainablePipelineAD(
                params=self._params, name=self.name, algo=self.algo
            )
        elif self.algo in con.TRAINABLE_AD:
            return TrainablePipelineAD(
                params=self._params, name=self.name, algo=self.algo
            )
        else:
            raise ValueError(
                "%s is not found in trainable or non_trainable detection algorithm"
                % self.algo
            )

    def fit(self, data: pd.DataFrame):
        self.pipeline.fit(data)

    def run(self, time_series: TimeSeriesType) -> TimeSeriesType:
        return self.pipeline.run(time_series)

    def load_model(self, model_params: dict):
        self.pipeline.load_model(model_params)

    def dump_model(self):
        return self.pipeline.dump_model()

    def set_name(self, name: str) -> None:
        self.name = name
        self.pipeline.set_name(name)


class BaseModel(ABC):
    def __init__(
        self,
        algo,
        params,
        name,
    ):
        super().__init__()
        self.name = name
        self._params = params
        self.detector_dict = {}
        self.check_parameter(algo)
        self.detector = self.get_detector(algo)
        suppressor_param = (
            self._params[con.ANOMALY_SUPPRESS].get(algo)
            if algo in self._params[con.ANOMALY_SUPPRESS]
            else self._params[con.ANOMALY_SUPPRESS].get("common")
        )
        self.suppressor = SuppressorPipeline(self.name, suppressor_param)
        self.severity_level_combiner = SeverityLevelCombiner(
            name, self._params.get(con.SEVERITY_LEVEL)
        )
        self.algo = algo

    @abstractmethod
    def get_detector(self, algo):
        pass

    def check_parameter(self, algo):
        if not (algo in self._params):
            raise ParameterError("Parameter for %s is missing" % algo)

    def run(self, time_series: TimeSeriesType) -> TimeSeriesType:
        """
        detect anomaly
        TimeSeriesType is the structure to restore detection information of data
        :param time_series: a dict of data information
        :return: a dict of data information after detection
            example:
                {
                        'anomalyScore':                        field1    field2
                                    time
                                    2020-01-01 10:21:00           2.0        1
                                    2020-01-01 10:37:15           0.0        0
                                    2020-01-01 10:55:30           0.0 ,      0
                        'anomalyLevel':                        field1    field2
                                    ...
                }
        """
        try:
            time_series = self.detector.detect(time_series)
        except ValueNotEnoughError as error:
            err_info_str = "%s algorithm catch exception: %s" % (self.algo, error)
            logger.info(err_info_str)
            error_info_cache = CacheSet().get_cache(con.ERROR_INFO)
            error_info_cache.update({self.name: err_info_str})
            return time_series
        time_series = self.suppressor.suppress(time_series)
        time_series = self.severity_level_combiner.run(
            time_series, {con.ALGO: self.algo}
        )

        return time_series

    def set_name(self, name: str) -> None:
        self.name = name
        self.detector.set_name(name)
        self.suppressor.set_name(name)


class NonTrainablePipelineAD(BaseModel):
    """
    the pipeline which doesn't need to fit model before detect.
    """

    def get_detector(self, algo):
        self.detector_dict = {
            con.THRESHOLD_AD: ThresholdAD,
            con.INCREMENTAL_AD: IncrementalAD,
            con.DIFFERENTIATE_AD: DIFFERENTIATEAD,
            con.VALUE_CHANGE_AD: ValueChangeAD,
            con.BATCH_DIFFERENTIATE_AD: DIFFERENTIATEAD,
        }

        detector = self.detector_dict.get(algo)(self.name, self._params.get(algo))
        return detector

    def fit(self, data: pd.DataFrame):
        pass

    def load_model(self, model_params: dict):
        pass

    @staticmethod
    def dump_model():
        return None


class TrainablePipelineAD(BaseModel):
    """
    the pipeline which needs to fit model before detect.
    """

    def get_detector(self, algo):
        self.detector_dict = {}
        detector_algo = (
            self._params.get(algo)
            if self._params.get(algo) is not None
            else self._params
        )
        detector = self.detector_dict.get(algo)(self.name, detector_algo)
        return detector

    def fit(self, data: pd.DataFrame):
        self.detector.fit(data)

    def load_model(self, model_params: dict):
        self.detector.load_model(model_params.get("detector"))

    def dump_model(self):
        return {"detector": self.detector.dump_model()}
