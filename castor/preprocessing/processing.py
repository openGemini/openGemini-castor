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
from __future__ import division
from typing import Optional, Union

import numpy as np
import pandas as pd
from adtk.data import validate_series

from ..utils import const as con
from ..transform.smoothing import peak_smoothing_with_quantile, smoothing
from ..utils.exceptions import ValueMissError


class PreProcess:
    """
    数据合法性校验&数据预处理
    """

    def __init__(self, validate_params: dict, preprocess_params: dict):
        self._validate_params = validate_params
        self._preprocess_params = preprocess_params

    def validate_preprocess(self, s: pd.DataFrame, flag):
        # data validation
        s = self.validate(
            ts=s,
            flag=flag,
            miss_max_rate=self._validate_params.get("miss_max_rate", 0),
        )

        # data preprocess
        s = self.preprocess(
            ts=s,
            flag=flag,
            interval=self._preprocess_params.get("interval"),
            p1=self._preprocess_params.get("p1", 0),
            p2=self._preprocess_params.get("p2", 1),
            window=self._preprocess_params.get(con.WINDOW, 1),
            agg=self._preprocess_params.get("agg", "median"),
        )

        return s

    @staticmethod
    def validate(
        ts: Union[pd.Series, pd.DataFrame],
        flag: Optional[str] = "fit",
        miss_max_rate: Optional[float] = None,
    ) -> Union[pd.Series, pd.DataFrame]:
        """Validate time series.

        validate process will check some feature critical issues that may need
        fixing them.

        this process will divided into the following steps:

        Step1: this issues will be checked and automatically fixed by adtk method:
        - Time index is not monotonically increasing;
        - Time index contains duplicated time stamps (fix by keeping first values)
        - (optional) Time index attribute `freq` is missed;
        - (optional) Time series include categorical (non-binary) label columns
          (to fix by converting categorical labels into binary metrics).

        Step2: this issues will check and fixed by our method

        Issues will be checked and raise error include:
        - miss data struct and critical key words
            - Wrong type of time series object (must be pandas Series or DataFrame)
            - Wrong type of time index object (must be pandas DatetimeIndex).
        - train data no enough
        - too many missing values

        Parameters
        ----------
        ts: pandas Series or DataFrame
            Time series to be validated.
        flag: str, optional
            The flag of fitting or predicting. Default: "fit"
        miss_max_rate: float
            The max rate of missing value.

        Returns
        -------
        pandas Series or DataFrame
            Validated time series.

        """

        # check adtk to remove the duplicate data and sort by timestamp
        ts = validate_series(ts)

        data_length = len(ts)

        # check missing values
        miss_max_size = max(pd.isnull(ts).sum(axis=0))
        if (miss_max_size / data_length if data_length != 0 else 1) > miss_max_rate:
            msg = "Data_Validate {}: miss max size={}, miss_max_rate={}".format(
                flag, miss_max_size, miss_max_rate
            )
            raise ValueMissError(msg)

        return ts

    def preprocess(
        self,
        ts: Union[pd.Series, pd.DataFrame],
        flag: str = None,
        interval: str = "asitis",
        p1: float = 0.001,
        p2: float = 0.999,
        window: int = 1,
        agg: str = "median",
    ) -> Union[pd.Series, pd.DataFrame]:
        """
        preprocess time series.

        step1: smooth the extreme value: 5% and 95% multi-quntile values are used,
               and the extreme values exceeding the extreme value is taken.
        step2: smooth process: take mean or median with smooth window, and if the
               data is in stable, the mean is used. Otherwise, the median is used.

        Parameters
        ----------
        ts: pd.Series, pd.DataFrame
            Time series to be preprocessed.

        flag: str, optional
            The field to identify fit or predict. Default: "fit"

        interval: str, optional
            The resample interval, number in minutes or "asitis". Default: "asitis".

        p1: float, optional
            The low quantile. Default: 0.001.

        p2: float, optional
            The high quantile. Default: 0.999.

        window: int, optional
            The size of smooth window. Default: 1.
        agg: str, optional
            The aggregate function. Default: "median"

        Returns
        -------
        pd.Series, pd.DataFrame
            Time series preprocessed.

        """

        s = self.resample(ts, interval)

        # Replace NaN with zero and infinity with large finite numbers
        if isinstance(s, pd.Series):
            s = pd.Series(data=np.nan_to_num(s.values), index=s.index, name=s.name)
        else:
            s = pd.DataFrame(
                data=np.nan_to_num(s.values), index=s.index, columns=s.columns
            )

        # Smooth extremes by percentile  5% 95%
        # Check stable of the data series by adf
        if flag in ["fit"]:
            s = peak_smoothing_with_quantile(s, p1=p1, p2=p2)
            s = smoothing(df=s, method=agg, window=window)

        return s

    @staticmethod
    def resample(ts: pd.DataFrame, interval: str) -> pd.DataFrame:
        # if length of ts is 1, do nothing
        if ts.shape[0] == 1:
            return ts

        if ts.shape[1] == 1 and interval == "asitis":
            # single variate, and don't resample
            # some algorithms like the ones in adtk might go wrong if the
            # if the time intervals are not the same
            ts = ts.dropna()
        elif interval and interval != "asitis":
            ts = ts.resample(interval, label="left").mean()
        else:
            # Adaptive sample
            if len(ts) < 2:
                raise ValueError(
                    "the data is not enough, check the validity of the data"
                )
            res = (pd.Series(ts.index[1:] - ts.index[:-1])).value_counts()
            if res.empty:
                raise ValueError(
                    "the freq of data is unknown, check the validity of the data"
                )
            adaptive_interval = res.index[0]
            ts = ts.resample(adaptive_interval, label="left").mean()

        # Fill missing values
        ts = ts.interpolate(axis=0)
        ts = ts.fillna(method="bfill")
        ts = ts.fillna(method="ffill")
        return ts
