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

ABSOLUTE_PATH = os.path.split(os.path.realpath(__file__))[0] + "/"
CASTOR_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/"

# all of detectors
DIFFERENTIATE_AD = "DIFFERENTIATEAD"
BATCH_DIFFERENTIATE_AD = "BatchDIFFERENTIATEAD"
INCREMENTAL_AD = "IncrementalAD"
VALUE_CHANGE_AD = "ValueChangeAD"
THRESHOLD_AD = "ThresholdAD"

DETECTOR_FOR_FIT = {
    BATCH_DIFFERENTIATE_AD,
    DIFFERENTIATE_AD,
    INCREMENTAL_AD,
    VALUE_CHANGE_AD,
    THRESHOLD_AD,
}

DETECTOR_FOR_DETECT = {
    BATCH_DIFFERENTIATE_AD,
    DIFFERENTIATE_AD,
    INCREMENTAL_AD,
    VALUE_CHANGE_AD,
    THRESHOLD_AD,
}

DETECTOR_FOR_FIT_DETECT = {
    BATCH_DIFFERENTIATE_AD,
    DIFFERENTIATE_AD,
    INCREMENTAL_AD,
    VALUE_CHANGE_AD,
    THRESHOLD_AD,
}

NON_TRAINABLE_AD = [
    BATCH_DIFFERENTIATE_AD,
    DIFFERENTIATE_AD,
    INCREMENTAL_AD,
    VALUE_CHANGE_AD,
    THRESHOLD_AD,
]

TRAINABLE_AD = []

DATA_VALIDATE = "Data_Validate"
DATA_PREPROCESS = "Data_Preprocess"
ANOMALY_SUPPRESS = "Anomaly_Suppress"
DYNAMIC_THRESHOLD = "DYNAMIC_THRESHOLD"
SEVERITY_LEVEL = "Severity_Level"
SIGEWM_THRESHOLDER = "SigewmThresholder"
SIGMA_THRESHOLDER = "SigmaThresholder"

ALGO = "algo"

WINDOW = "window"

UPPER_BOUND = "upper_bound"
UPPER_BOUND_KV = "upper_bound_dict"

LOWER_BOUND = "lower_bound"
LOWER_BOUND_KV = "lower_bound_dict"

GAP = "gap"
HIS_ANOMALY = "his_anomaly"


SCORE = "anomalyScore"
ORIGIN = "originalValue"
TYPE = "triggerType"
FREQ = "freq"
LEVEL = "anomalyLevel"
LABEL = "anomalyLabel"
GENERATE_TIME = "generateTime"

DATA_CACHE = "DataCache"
SIGMA_EWM_THRESHOLD_CACHE = "SigewmThresholderCache"
STREAM_FILTER_CACHE = "StreamFilterCache"
SUPPRESS_CACHE = "SuppressCache"
SEVERITY_LEVEL_CACHE = "SeverityLevelCache"
ERROR_INFO = "ErrorInfo"

KV_PARAM_KEY = {UPPER_BOUND_KV, LOWER_BOUND_KV}

CLEAR_CACHE_INTERVAL = 60 * 60 * 24 * 7

CLEAR_MODEL_INTERVAL = 60 * 60 * 24 * 7

MODEL_EXPIRING_TIME = 60 * 60 * 24 * 7
