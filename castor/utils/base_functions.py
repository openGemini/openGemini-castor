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
import pickle as pkl
from typing import Optional
import stat
import copy

import yaml

from ..utils import const as con
from ..utils.logger import logger


def _load_param_from_yaml_string(config_string):
    params = dict()
    try:
        params = yaml.safe_load(config_string)
    except Exception as err:
        logger.error("load parameters failed: %s!", err)
    return params


def _load_param_from_yaml_file(config_file):
    params = dict()
    try:
        config_file = os.path.realpath(config_file)
        with open(config_file, "r") as f:
            params = yaml.safe_load(f)
    except Exception as err:
        logger.error("load parameters failed: %s!", err)
    return params


def load_params_from_yaml(
    config_file: Optional[str] = None, config_string: Optional[str] = None
) -> dict:
    params = dict()
    if config_string:
        params = _load_param_from_yaml_string(config_string)
    elif config_file:
        params = _load_param_from_yaml_file(config_file)
    return params


def dump_model_file_to_disk(model_file, model_params):
    if model_params:
        try:
            flags = os.O_WRONLY | os.O_CREAT
            modes = stat.S_IWUSR | stat.S_IRUSR
            with os.fdopen(os.open(model_file, flags, modes), "wb") as f:
                pkl.dump(model_params, f)
            logger.info("dump model %s successfully!", model_file)
        except Exception as err:
            logger.error("dump model %s failed: %s!", model_file, err)


def load_model_file_from_disk(model_file):
    model_params = dict()
    try:
        with open(model_file, "rb") as f:
            model_params = pkl.load(f)
        logger.info("load model %s successfully!", model_file)
    except FileNotFoundError as err:
        logger.info("No model %s, %s", model_file, err)
    except Exception as err:
        logger.error("load model %s failed: %s!", model_file, err)
    return model_params


def get_mapped_params(meta, field_ids, params) -> dict:
    filed_id_str_map = {
        key: meta.get_meta_data_by_id(key)[2]
        for key in field_ids
        if meta.get_meta_data_by_id(key) is not None
    }
    mapped_params = copy.deepcopy(params)
    _map_string_to_filed_id_for_param(
        params, mapped_params, field_ids, filed_id_str_map
    )
    return mapped_params


def _map_string_to_filed_id_for_param(params, new_params, filed_ids, filed_id_str_map):
    for param_key, param in params.items():
        if param_key in con.KV_PARAM_KEY:
            new_params[param_key] = {
                field_id: param.get(filed_id_str_map.get(field_id).decode())
                for field_id in filed_ids
                if param.get(filed_id_str_map.get(field_id).decode()) is not None
            }
        elif isinstance(param, dict):
            _map_string_to_filed_id_for_param(
                param, new_params[param_key], filed_ids, filed_id_str_map
            )
