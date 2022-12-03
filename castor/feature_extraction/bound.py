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

from __future__ import absolute_import, division
from typing import Tuple, Union

import numpy as np


def boxplot_bound(
    q1: float = 0.25, q3: float = 0.75, c: float = 1.5, bias: float = 0.01
) -> Tuple[float, float]:
    """Calculate the upper and lower bounds according to the box-plot

    Parameters
    ----------
    q1: float
        The lower quartile. Default: 0.25.
    q3: float
        The upper quartile. Default: 0.75.
    c: float
        The coefficient of the quartile range. Default: 1.5.
    bias: float
        The coefficient of the degree of the upper and lower bounds.

    Returns
    -------
    lower_bound: float
        The lower limits.
    upper_bound: float
        The upper limits.
    """

    iqr = q3 - q1
    if (q1 - c * iqr) > 0:
        lower_bound = (q1 - c * iqr) * (1 - (bias / 2 if bias > 0.3 else bias))
    else:
        lower_bound = (q1 - c * iqr) * (1 + bias)

    upper_bound = (q3 + c * iqr) * (1 + bias)

    return lower_bound, upper_bound


def ksigma_bound(
    mean: Union[float, np.array],
    std: Union[float, np.array],
    k: float = 3.0,
    bias: float = 0.01,
) -> Tuple[float, float]:
    """ "Calculate the upper and lower limits according to the k-sigma.

    Parameters
    ----------
    mean: float or np.array, optional
        The mean of the data.
    std: float or np.array, optional
        The std of the data.
    k: float
        The coefficient of k-sigma. Default: 3.
    bias: float
        The coefficient of the degree of the upper and lower limits.

    Returns
    -------
    lower_bound: float
        The lower limits.
    upper_bound: float
        The upper limits.
    """

    if mean - k * std > 0:
        lower_bound = (mean - k * std) * (1 - (bias / 2 if bias > 0.3 else bias))
    else:
        lower_bound = (mean - k * std) * (1 + bias)

    upper_bound = (mean + k * std) * (1 + bias)

    return lower_bound, upper_bound
