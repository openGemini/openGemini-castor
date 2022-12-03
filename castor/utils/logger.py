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
import logging

logger = logging.getLogger("castor")
log_filename = "/log/castor/castor.log"


def set_log_filename(filename):
    global log_filename
    log_filename = filename


def basic_config(level="DEBUG"):
    global logger
    level_ = logging.INFO
    if level == "CRITICAL":
        level_ = logging.CRITICAL
    elif level == "ERROR":
        level_ = logging.ERROR
    elif level == "WARNING":
        level_ = logging.WARNING
    elif level == "INFO":
        level_ = logging.INFO
    elif level == "DEBUG":
        level_ = logging.DEBUG
    logger.setLevel(level_)
    logging.basicConfig(
        filename=log_filename,
        level=level_,
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    )
