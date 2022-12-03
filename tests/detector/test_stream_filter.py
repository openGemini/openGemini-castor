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

import pytest
import pandas as pd
import numpy as np

from castor.detector.stream_filter.get_latest_data_module import (
    LatestData,
    get_latest_index,
)
from castor.utils import logger as llogger
from castor.utils.logger import logger
from castor.utils.exceptions import ValueNotEnoughError, NoNewDataError
from castor.detector.cache.organize_cache import clear_cache


llogger.basic_config()


class TestStreamFilter:
    def tear_up(self):
        self.total_data_length = 30
        self.total_dim = 10
        self.data = np.stack(
            [[i for i in range(self.total_data_length)]] * self.total_dim, axis=1
        )
        self.data_df = pd.DataFrame(
            self.data,
            columns=[i for i in range(10)],
            index=pd.date_range(
                start="2022-08-24", periods=self.total_data_length, freq="T"
            ),
        )
        self.window = 7
        self.max_window = 10

    @pytest.fixture()
    def env_ready(self):
        self.tear_up()
        yield
        clear_cache()

    @pytest.mark.usefixtures("env_ready")
    def test_stream_filter(self):
        latest_data = LatestData()

        # data is not enough data, when len(cache data + new data) < window + 1 and len(data) < window + 1
        latest_data.update(self.max_window, self.data_df.iloc[4:6, :])
        latest_index = get_latest_index(self.data_df.columns)
        data_1 = self.data_df.iloc[3:8]
        try:
            latest_data.get_data(data_1, latest_index, window=self.window)
        except ValueNotEnoughError as error:
            logger.info("catch exception: %s", error)
        else:
            assert False
        latest_data.update(self.max_window, data_1)

        # get result from data_df, when len(old data) < window and
        # len(cache data + new data) < window + 1 and len(data_df) > window + 1 and
        data_2 = self.data_df.iloc[2:10]
        latest_index = get_latest_index(self.data_df.columns)
        assert pd.to_datetime("2022-08-24 00:07:00") == latest_index
        result = latest_data.get_data(data_2, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:02:00", periods=8, freq="T")
        )
        latest_data.update(self.max_window, data_2)

        # get result from cache data and data_df, when len(old data) < window and
        # len(cache data + new data) >= window + 1 and len(cache data + new data) > len(data_df)
        data_3 = self.data_df.iloc[6:12]
        latest_index = get_latest_index(self.data_df.columns)
        assert latest_index == pd.to_datetime("2022-08-24 00:09:00")
        result = latest_data.get_data(data_3, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:04:00", periods=8, freq="T")
        )
        latest_data.update(self.max_window, data_3)

        # get result from cache data and data_df, when len(old data) < window and
        # len(cache data + new data) >= window + 1 and len(cache data) >= window and len(old data) < window
        data_4 = self.data_df.iloc[8:14]
        latest_index = get_latest_index(self.data_df.columns)
        assert latest_index == pd.to_datetime("2022-08-24 00:11:00")
        result = latest_data.get_data(data_4, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:05:00", periods=9, freq="T")
        )
        latest_data.update(self.max_window, data_4)

        # get result from data_df, when len(old data) >= window
        data_5 = self.data_df.iloc[1:17]
        latest_index = get_latest_index(self.data_df.columns)
        assert latest_index == pd.to_datetime("2022-08-24 00:13:00")
        result = latest_data.get_data(data_5, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:07:00", periods=10, freq="T")
        )
        latest_data.update(self.max_window, data_5)

        # no new data, when latest_index >= (max index of data_df)
        data_5 = self.data_df.iloc[8:17]
        latest_index = get_latest_index(self.data_df.columns)
        assert latest_index == pd.to_datetime("2022-08-24 00:16:00")
        try:
            data_5 = latest_data.filter_disorder_data(data_5)
            latest_data.get_data(data_5, latest_index, window=self.window)
            assert False
        except NoNewDataError as error:
            logger.info("catch exceptions: %s", error)

        # disorder data
        data_disorder = self.data_df.iloc[7:20, [1, 2]]
        latest_index = get_latest_index([1, 2])
        assert latest_index == pd.to_datetime("2022-08-24 00:16:00")
        result = latest_data.get_data(data_disorder, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:10:00", periods=10, freq="T")
        )
        assert all(result.columns == data_disorder.columns)
        latest_data.update(self.max_window, result)

        # input data contains disorder data
        data_disorder = self.data_df.iloc[8:19]
        data_disorder = latest_data.filter_disorder_data(data_disorder)
        latest_index = get_latest_index(data_disorder.columns)
        assert latest_index == pd.to_datetime("2022-08-24 00:16:00")
        result = latest_data.get_data(data_disorder, latest_index, window=self.window)
        assert all(
            result.index
            == pd.date_range(start="2022-08-24 00:10:00", periods=9, freq="T")
        )
        assert all(result.columns == [0, 3, 4, 5, 6, 7, 8, 9])
        latest_data.update(self.max_window, data_disorder)
