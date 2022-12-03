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

from ...utils import const as con
from ...utils.common import Singleton


@Singleton
class KeyCache(object):
    def __init__(self):
        self._cache_key = set()

    def get_key(self):
        return self._cache_key

    def add_key(self, key):
        self._cache_key.add(key)

    def remove_values_by_key(self, keys: set) -> None:
        self._cache_key -= keys

    def clear(self):
        self._cache_key = set()


class KeyValueCache:
    def __init__(self):
        self._cache = dict()

    def get_value(self, key, default=None):
        return self._cache.get(key, default)

    def clear(self):
        self._cache = dict()

    def update(self, cache_dict: dict):
        self._cache.update(cache_dict)

    def remove_values_skip_keys(self, keys):
        for key in list(self.keys()):
            if key not in keys:
                self._cache.pop(key)

    def remove_values_by_keys(self, keys: list):
        for key in keys:
            self._cache.pop(key, None)

    def set_value(self, key, data):
        self._cache[key] = data

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()

    def __len__(self) -> int:
        return len(self._cache)


class PostfixCache(KeyValueCache):
    def remove_values_skip_keys(self, keys):
        for key in list(self.keys()):
            if all(not key.endswith(value) for value in keys):
                self._cache.pop(key)

    def remove_values_by_keys(self, keys: list):
        for key in list(self.keys()):
            if any(key.endswith(value) for value in keys):
                self._cache.pop(key)


@Singleton
class CacheSet:
    def __init__(self, cache: dict = None):
        self._cache = {
            con.DATA_CACHE: KeyValueCache(),
            con.SIGMA_EWM_THRESHOLD_CACHE: PostfixCache(),
            con.STREAM_FILTER_CACHE: KeyValueCache(),
            con.SUPPRESS_CACHE: PostfixCache(),
            con.SEVERITY_LEVEL_CACHE: PostfixCache(),
            con.ERROR_INFO: PostfixCache(),
        }
        if cache is not None:
            self._cache.update(cache)

    def get_cache(self, cache_type):
        return self._cache.get(cache_type)

    def clear(self):
        for cache in self._cache.values():
            cache.clear()

    def keys(self):
        return self._cache.keys()

    def add_cache(self, cache_type, cache_dict: dict):
        if cache_type not in self._cache.keys():
            cache = KeyValueCache()
            cache.update(cache_dict)
            self._cache[cache_type] = cache

    def values(self):
        return self._cache.values()

    def items(self):
        return self._cache.items()
