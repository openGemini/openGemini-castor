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

import numpy as np
import pandas as pd

from .sigma_ewm import SigewmThresholder
from .sigma import SigmaThresholder
from ...utils import const as con


class ThresholderModule:
    def __init__(self, name, params):
        thresholder_dict = {
            con.SIGEWM_THRESHOLDER: SigewmThresholder,
            con.SIGMA_THRESHOLDER: SigmaThresholder,
        }
        self.name = name
        self.threshold_choice = params.get("CHOICE")
        self.thresholder_core = thresholder_dict.get(self.threshold_choice)(
            name=self.name, **params.get(self.threshold_choice)
        )

    def thresholder(self, score_multidim: pd.DataFrame = None):
        tmp = np.around(score_multidim.values, 5)
        score_multidim = pd.DataFrame(
            tmp, index=score_multidim.index, columns=score_multidim.columns
        )
        return self.thresholder_core.threshold(score=score_multidim)

    def set_name(self, name: str) -> None:
        self.name = name
