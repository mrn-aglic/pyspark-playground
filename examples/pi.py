#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
import logging


class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO


class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.ERROR


class WarnFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.WARNING


# 로거 생성
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# 핸들러 생성
info_handler = logging.StreamHandler()
error_handler = logging.StreamHandler()
warn_handler = logging.StreamHandler()

# 필터 추가
info_handler.addFilter(InfoFilter())
error_handler.addFilter(ErrorFilter())
warn_handler.addFilter(WarnFilter())

date_format = "%y/%m/%d %H:%M:%S"
# 포맷 설정
info_format = logging.Formatter("%(asctime)s INFO %(message)s", datefmt=date_format)
error_format = logging.Formatter("%(asctime)s ERROR %(message)s", datefmt=date_format)
warn_format = logging.Formatter("%(asctime)s WARN %(message)s", datefmt=date_format)

log_format = "%(asctime)s %(levelname)s %(message)s"

info_handler.setFormatter(info_format)
error_handler.setFormatter(error_format)
warn_handler.setFormatter(warn_format)

# 핸들러 추가
logger.addHandler(info_handler)
logger.addHandler(error_handler)
logger.addHandler(warn_handler)

if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x**2 + y**2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    )
    print("Pi is roughly %f" % (4.0 * count / n))

    logging.info("KHCLogger: Step 1: 0:00:02")
    logging.info("KHCLogger: Step 2: 0:00:05")
    logging.info("KHCLogger: Step 3: 0:00:07")
    logging.info("KHCLogger: Step 4: 0:00:10")
    logging.info("KHCLogger: Step 5: 0:00:12")

    logging.error(
        "asdasdasdkjalsdjlaksdjaklsjdkaldjkalsdjklasdjaklsjdklajsdaklsdjaklsjdklajsd"
    )
    logging.warning(
        "asdasdasdkjalsdjlaksdjaklsjdkaldjkalsdjklasdjaklsjdklajsdaklsdjaklsjdklajsd"
    )

    spark.stop()
