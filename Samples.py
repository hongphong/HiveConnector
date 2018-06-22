#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'

# !/usr/bin/python
#
# Copyright 2015 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from BaseMongoDb import BaseMongoDb
from RDbSql import *
from PRedis import PRedis


def test_connect_mongodb():
    """
    Connect to MongoDb server
    :return:
    """

    # connect default with main config
    # you can config other host_name in Config.Main.MONGODB_CONNECTION
    con = BaseMongoDb(host_name='main')
    database = con.get_db("db name")
    collection = database['collection name']
    rs = collection.find({})


def test_connect_hive():
    """
    Connect to Hive Server 2
    :return:
    """
    # connect default with main config
    # you can config other host_name in Config.Main.SQLACHEMY_DATABASE
    db = HiveSql(host_name='hive')
    rs = db.execute(query=" select * from {database}.{table}", replace_sql={
        'database': 'your database',
        'table': 'your table'
    })


def test_connect_spark():
    """
    Connect to Spark Thrift Server
    :return:
    """
    # connect default with main config
    # you can config other host_name in Config.Main.SQLACHEMY_DATABASE
    db = SparkSql(host_name='spark')
    rs = db.execute(query=" select * from {database}.{table}", replace_sql={
        'database': 'your database',
        'table': 'your table'
    })


def test_connect_redis():
    """
    Connect to Redis server
    :return:
    """
    rs = PRedis()

    # get key
    rs.get(key="")

    # get hash
    rs.hget(name="", key="")

    # set hash with expire time
    rs.hset(name="", key="", expire=100)

    # get all value of hash and convert to dict
    rs.hgetalltodict(name="")


if __name__ == '__main__':
    pass
