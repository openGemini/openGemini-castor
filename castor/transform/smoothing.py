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
from typing import Union

import numpy as np
import pandas as pd

from ..feature_extraction.bound import boxplot_bound


def peak_smoothing_with_quantile(
    data_series: Union[pd.Series, pd.DataFrame], p1: float = 0.05, p2: float = 0.95
) -> Union[pd.Series, pd.DataFrame]:
    """Smooth the data series by setting the upper and lower quantiles.
    Parameters
    ----------
    data_series: pd.Series, pd.DataFrame
        The data series.
    p1: float
        The lower quantile. Default: 0.05
    p2: float
        The upper quantile. Default: 0.95

    Returns
    -------
    data_series: pd.Series, pd.DataFrame
        The data series smoothed.
    """

    suc_rate_min = data_series.quantile(p1)
    suc_rate_max = data_series.quantile(p2)

    if isinstance(data_series, pd.Series):
        data = np.clip(data_series, suc_rate_min, suc_rate_max)
        data_series = pd.Series(index=data_series.index, data=data)
    else:
        data = np.clip(data_series, suc_rate_min.values, suc_rate_max.values, axis=1)
        data_series = pd.DataFrame(
            index=data_series.index, data=data, columns=data_series.columns
        )

    return data_series


def smoothing(
    df: Union[pd.Series, pd.DataFrame], method: str = "median", window: int = 1
) -> Union[pd.Series, pd.DataFrame]:
    """Smooth the data frame according the method.

    Parameters
    ----------
    df: pd.Series, pd.DataFrame
        The data series.
    method: str
        The processing method. Default: "median".
    window: int
        The window size. Default: 1

    Returns
    -------
    s: pd.Series, pd.DataFrame
        The data series smoothed.
    """

    df_head = df.iloc[: (window - 1)].copy()
    if method == "mean":
        df = df.rolling(window=window, center=False).mean()
    else:
        df = df.rolling(window=window, center=False).median()
    if isinstance(df, pd.Series):
        df = pd.Series(
            data=np.concatenate(
                (df_head.values, df.iloc[(window - 1) :].values), axis=0
            ),
            index=df.index,
        )
    else:
        df = pd.DataFrame(
            data=np.concatenate(
                (df_head.values, df.iloc[(window - 1) :].values), axis=0
            ),
            index=df.index,
            columns=df.columns,
        )
    df.fillna(method="bfill", inplace=True)

    return df
