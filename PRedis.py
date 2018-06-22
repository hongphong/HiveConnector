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

from Config import Main as config
import redis
import hashlib
import re
import pickle
import time


class PRedis(object):
    STORE_EXPIRE_TIME_HSET = 'HSET_EXPIRE'
    host = ''
    port = ''
    database = 0

    redis_instance = None

    def __init__(self, **kwargs):
        self.host = config.REDIS_CONNECTION['host']
        self.port = int(config.REDIS_CONNECTION['port'])
        self.database = kwargs.get('database', config.REDIS_CONNECTION['default_db'])
        self.redis_instance = redis.StrictRedis(host=self.host, port=self.port, db=self.database,
                                                max_connections=100)

    @staticmethod
    def convert_long_text_to_key(text):
        if text == '':
            raise ValueError('Text is invalid when convert this text to a key that used on Redis')
        key = str(int(hashlib.md5(text).hexdigest(), 16))
        return key

    def get_redis(self):
        return self.redis_instance

    def get(self, key, default=None):
        if not self.get_redis():
            return False
        value = self.get_redis().get(key)
        if value:
            value = pickle.loads(value)
        else:
            if value is None:
                return default
        return value

    def set(self, key, value, expire=60 * 10):
        if not self.get_redis():
            return False
        p_mydict = pickle.dumps(value)
        self.get_redis().set(key, p_mydict)
        self.get_redis().expire(key, expire)
        return self

    def set_database(self, db=0):
        self.database = db
        self.redis_instance = redis.StrictRedis(host=self.host, port=self.port, db=db)
        return self

    def hexists(self, name, key):
        if not self.get_redis():
            return False
        return self.get_redis().hexists(name=name, key=key)

    def hget(self, name, key, default=None):
        if not self.get_redis():
            return False
        value = self.get_redis().hget(name=name, key=key)
        timenow = time.time()
        exp = self.redis_instance.hget(name=self.STORE_EXPIRE_TIME_HSET, key=name + key)
        if exp and timenow - float(exp) >= 0:
            self.redis_instance.hdel(self.STORE_EXPIRE_TIME_HSET, name + key)
            self.delete(key)
            return None
        if value:
            value = pickle.loads(value)
        else:
            if value is None:
                return default
        return value

    def hgetalltodict(self, name):
        return {k: self.hget(name=name, key=k) for k in self.get_redis().hkeys(name=name)}

    def hset(self, name, key, value, expire=None):
        if not self.get_redis():
            return False
        p_mydict = pickle.dumps(value)
        if expire:
            self.redis_instance.hset(name=self.STORE_EXPIRE_TIME_HSET, key=name + key, value=time.time() + int(expire))
        return self.get_redis().hset(name=name, key=key, value=p_mydict)

    def delete(self, *name):
        if not self.get_redis():
            return False
        return self.get_redis().delete(*name)
