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

from . import errorcode as ec


class ValueNotEnoughError(ValueError):
    """This exception is triggered when the training and detection data is
    insufficient.
    """

    def __init__(self, error_msg):
        self.error_code = str(ec.DATA_NOT_ENOUGH)
        self.error_msg = self.error_code + error_msg
        ValueError.__init__(self, error_msg)


class NoNewDataError(ValueError):
    """This exception is triggered when the data is not new data."""

    def __init__(self, error_msg):
        self.error_code = str(ec.NO_NEW_DATA_ERROR)
        self.error_msg = self.error_code + error_msg
        ValueError.__init__(self, error_msg)


class ValueMissError(ValueError):
    """This exception is triggered when the training and detection data miss
    too much.
    """

    def __init__(self, error_msg):
        self.error_code = str(ec.DATA_MISS_ERROR)
        self.error_msg = self.error_code + error_msg
        ValueError.__init__(self, error_msg)


class ParameterError(Exception):
    """This exception is trigger when param is missing when the param is necessary"""

    def __init__(self, error_msg):
        self.error_code = str(ec.PARAMETER_ERROR)
        self.error_msg = self.error_code + error_msg
        Exception.__init__(self, error_msg)


class PeriodException(Exception):
    """This exception is triggered when checking period."""

    def __init__(self, error_msg):
        self.error_code = str(ec.PERIOD_CHECK_ERROR)
        self.error_msg = self.error_code + error_msg
        Exception.__init__(self, error_msg)


class PeakException(Exception):
    """When judging whether the point in init data is peak,
    this exception is triggered when too much points are equal to max number."""

    def __init__(self, error_msg):
        self.error_code = str(ec.PEAK_JUDGE_ERROR)
        self.error_msg = self.error_code + error_msg
        Exception.__init__(self, error_msg)


class NoModelFileError(Exception):
    """This exception is triggered when model is not exist in special path."""

    def __init__(self, error_msg):
        self.error_code = str(ec.NO_MODEL_FILE_ERROR)
        self.error_msg = self.error_code + error_msg
        Exception.__init__(self, error_msg)
