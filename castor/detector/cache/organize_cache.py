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

from ...utils.logger import logger
from ...utils.globalSymbol import Symbol
from ...detector.cache.cache import KeyCache, CacheSet


def remove_status_cache_with_symbol():
    symbol = Symbol()
    del_symbol = symbol.get_symbol("del_cache")
    if del_symbol:
        cache_key = KeyCache()
        cache_set = CacheSet()
        for values in cache_set.values():
            if len(values) > 0:
                values.remove_values_skip_keys(cache_key.get_key())
        symbol.set_symbol("del_cache", False)
        cache_key.clear()
        logger.info("remove caches that hasn't appeared in a period")


def record_status_cache(measurement: Union[list, str]) -> None:
    cache = KeyCache()
    if isinstance(measurement, list):
        for value in measurement:
            cache.add_key(str(value))
    else:
        cache.add_key(str(measurement))


def clear_cache():
    cache_key = KeyCache()
    cache_set = CacheSet()
    cache_set.clear()
    cache_key.clear()
