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
import threading

from ..utils.common import Singleton


@Singleton
class Symbol(object):
    def __init__(self):
        self.thread_lock = threading.Lock()
        self.symbol = {
            "del_cache": False,
            "del_model": False,
            "stop": False,
            "run": False,
        }
        self.options = {}

    def get_symbol(self, key):
        self.thread_lock.acquire()
        ret = self.symbol.get(key)
        self.thread_lock.release()
        return ret

    def set_symbol(self, key, symbol):
        self.thread_lock.acquire()
        if self.symbol.get(key) is not None:
            self.symbol[key] = symbol
        self.thread_lock.release()

    def get_option(self, key):
        self.thread_lock.acquire()
        ret = self.options.get(key)
        self.thread_lock.release()
        return ret

    def set_option(self, key, option):
        self.thread_lock.acquire()
        self.options[key] = option
        self.thread_lock.release()

    def clear_all(self):
        self.thread_lock.acquire()
        self.symbol = {
            "del_cache": False,
            "del_model": False,
            "stop": False,
            "run": False,
        }
        self.options = {}
        self.thread_lock.release()


@Singleton
class Stats(object):
    def __init__(self):
        self.thread_lock = threading.Lock()
        self.duration = 0
        self.number_run = 0

    def get_stats(self):
        self.thread_lock.acquire()
        duration = self.duration
        number_run = self.number_run
        self.thread_lock.release()
        return duration, number_run

    def set_stats(self, duration, number_run):
        self.thread_lock.acquire()
        self.duration = duration
        self.number_run = number_run
        self.thread_lock.release()
