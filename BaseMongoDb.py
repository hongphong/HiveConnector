# library to connect mongodb base on pymongo
# @author Phong Pham Hong <phongbro1805@gmail.com>

__author__ = 'phongphamhong'

from Config import Main as config
from pymongo import MongoClient, IndexModel
from bson.son import SON
from Utility import convert_time, generate_batches
import json
from Utility import current_time_with_tz, convert_time, convert_utc_to_timzone, timeit, timeit_with_show_params, \
    static_property
import time
from SystemLog import SystemLog
import pprint
import re
import datetime
import urllib


class BaseMongoDb(object):
    password = config.MONGODB_CONNECTION['main']['password']
    user = config.MONGODB_CONNECTION['main']['user']
    authentication_database = config.MONGODB_CONNECTION['main']['auth_db']
    max_intances = 11
    instance = {}
    config_host = config.MONGODB_CONNECTION['main']

    __host_name = ''

    @static_property
    def string_connect(self):
        return self.create_string_connect(config.MONGODB_CONNECTION['main'])

    @static_property
    def mongoclient(self):
        key = BaseMongoDb.string_connect
        if key in BaseMongoDb.instance and isinstance(BaseMongoDb.instance.get(key), MongoClient):
            return BaseMongoDb.instance[key]
        BaseMongoDb.instance[key] = MongoClient(BaseMongoDb.string_connect, connect=False)
        return BaseMongoDb.instance[key]

    @static_property
    def database(self):
        return getattr(BaseMongoDb.mongoclient, config.MONGODB_CONNECTION['main']['database'])

    def __init__(self, host_name='main'):
        self.__host_name = host_name if host_name else 'main'
        self.config_host = config.MONGODB_CONNECTION.get(self.__host_name)

    def create_engine(self):
        mg_cf = config.MONGODB_CONNECTION.get(self.__host_name)
        self.config_host = mg_cf
        if mg_cf:
            SystemLog.print_log('[MONGODB CONFIG] Config for mongodb is \n %s' % {
                'host': mg_cf['host']
            })
            string_connect = BaseMongoDb.create_string_connect(mg_cf)
            if len(BaseMongoDb.instance) > self.max_intances:
                BaseMongoDb.instance = {}
            BaseMongoDb.instance[string_connect] = MongoClient(string_connect
                                                               )
        else:
            raise BaseException('Cannot get config for mongodb. Given host_name is ' % self.__host_name)

    def get_engine(self):
        mg_cf = config.MONGODB_CONNECTION.get(self.__host_name)
        self.config_host = mg_cf
        if mg_cf is None:
            raise ValueError(
                'Config for mongodb is in valid, please check host_name on main.py. Value of given host_name is : %s' % self.__host_name)
        key = BaseMongoDb.create_string_connect(mg_cf)
        if key not in BaseMongoDb.instance:
            self.create_engine()
        return BaseMongoDb.instance.get(key)

    def get_db(self, db_name=''):
        return getattr(self.get_engine(), db_name)

    def get_list_databases(self):
        """
        Get list databases
        :return:
        """
        return self.get_engine().database_names()

    @staticmethod
    def db(db_name=''):
        """
        this function is deprecated
        :param db_name:
        :return:
        """
        return getattr(BaseMongoDb.mongoclient, db_name)

    @staticmethod
    def create_string_connect(cf):
        if cf.get('uri'):
            uri = cf['uri']
        else:
            user = cf.get('user', config.MONGODB_CONNECTION['main']['user'])
            password = cf.get('password', config.MONGODB_CONNECTION['main']['password'])
            host = cf.get('host', config.MONGODB_CONNECTION['main']['host'])
            port = cf.get('port', config.MONGODB_CONNECTION['main']['port'])

            uri = "mongodb://" + ((
                                      user + ':' + password + '@') if user else '') + \
                  host + ":" + port + "/"
        options = cf.get('options')
        if options:
            uri = "%s/%s%s" % (uri.rstrip("/"), ('?' if uri.find('?') < 0 else '&'), urllib.urlencode(options))
        return uri

    @staticmethod
    def reconnect(**kwargs):
        database = kwargs.get('database', config.MONGODB_CONNECTION['main']['database'])
        if kwargs.get('host', ''):
            BaseMongoDb.user = kwargs.get('user', config.MONGODB_CONNECTION['main']['user'])
            BaseMongoDb.password = kwargs.get('password', config.MONGODB_CONNECTION['main']['password'])
            host = kwargs.get('host', config.MONGODB_CONNECTION['main']['host'])
            port = kwargs.get('port', config.MONGODB_CONNECTION['main']['port'])

            BaseMongoDb.mongoclient = MongoClient(BaseMongoDb.create_string_connect(
                host=host,
                port=port
            ),
                connect=False)
        BaseMongoDb.database = getattr(BaseMongoDb.mongoclient, database)
        return BaseMongoDb

    @staticmethod
    def run_function(function_name, param):
        param_string = ''
        for k in param:
            spe = ',' if param_string else '';
            param_string = param_string + spe + json.dumps(k)
            BaseMongoDb.database.eval(function_name + '(' + param_string + ')')

    @staticmethod
    def index_model(*args, **kwargs):
        return IndexModel(*args, **kwargs)

    @staticmethod
    def make_match_time_range(field="ClickTime", start_time='', stop_time='', convert_time_type='datetime',
                              get_only_value=False, set_not_equal_stop_time=False):
        field = field.replace('$', '')
        match = {}
        if start_time:
            match = {
                field: {"$gte": convert_time(start_time, convert_time_type=convert_time_type)}
            }
        stop_time = stop_time if stop_time else start_time
        if stop_time:
            convert = convert_time(stop_time)
            if start_time == stop_time or ([convert.hour, convert.minute, convert.second] == [0, 0, 0]):
                convert = convert_time(convert, end_day=True)
            match[field].update({"$lte": convert_time(convert, convert_time_type=convert_time_type)})
            if set_not_equal_stop_time:
                del match[field]['$lte']
                match[field].update({"$lt": convert_time(convert + datetime.timedelta(days=1), reset_time=True,
                                                         convert_time_type=convert_time_type)})
        if get_only_value:
            return match[field]
        return match

    @staticmethod
    def make_match_time_not_in_range(field="ClickTime", start_time='', stop_time='', convert_time_type='datetime'):
        field = field.replace('$', '')
        match = {
            "$or": []
        }
        if start_time:
            match["$or"].append({field: {"$lt": convert_time(start_time, convert_time_type=convert_time_type)}})
        stop_time = stop_time if stop_time else start_time
        if stop_time:
            convert = convert_time(stop_time)
            if start_time == stop_time or ([convert.hour, convert.minute, convert.second] == [0, 0, 0]):
                convert = convert_time(convert, end_day=True)
            match["$or"].append({field: {"$gt": convert_time(convert, convert_time_type=convert_time_type)}})
        return match

    @staticmethod
    @timeit
    def insert_batches(**kwargs):
        collection = kwargs.get('collection')
        data = kwargs.get('data')
        callback = kwargs.get('callback')
        callback_list = kwargs.get('callback_list')
        size = kwargs.get('size', 30000)
        SystemLog.print_log("[INSERT BATCH MONGODB INFO] collection %s size %s " % (collection, size))

        def cb_df(k):
            if not k:
                return False
            if '_id' in k:
                del k['_id']
            if callback:
                k = callback(k)
            if k:
                return k
            return False

        for ins in generate_batches(data, size, callback_item=cb_df, remove_false_value=True, log_object=SystemLog):
            st = time.time()
            if callback_list:
                callback_list(ins)
            if ins:
                collection.insert_many(ins, ordered=False)
            SystemLog.print_log('[INSERT BATCH WITH SIZE %s: %s' % (len(ins), time.time() - st))
            del ins[:]

    @staticmethod
    def current_time_with_tz(timezone):
        return convert_time(current_time_with_tz(tz=timezone, reset_time=False).replace(tzinfo=None).__str__())

    @staticmethod
    def sort_dict(sort_data={}):
        return SON([(k, v) for k, v in sort_data.iteritems()])

    @staticmethod
    def fine_one(collection, query):
        r = collection.find_one(query)
        if r:
            return r
        return {}

    """
        This method used for transform collection with format:
          row 1:{
                A:A,
                B:B,
                C:C
          }
          row 2:{
                A:A,
                D:D,
                E:E
          }
        Transform with a given group by param:
          row 1:{
                A:A,
                B:B,
                C:C,
                D:D,
                E:E
          }
    """

    @staticmethod
    @timeit_with_show_params
    def transform_with_migrate_row(collection=None, **kwargs):
        old_name = collection.name
        new_collection_name = old_name + '_transforming_row'
        new_collection = collection.database[new_collection_name]
        fields = kwargs.get('fields', {})
        group_by = kwargs.get('group_by', {})
        match = kwargs.get('match', {})
        final_match = kwargs.get('final_match', {})
        if bool(fields) is False or bool(collection) is None:
            raise KeyError('Fields must be set')

        group = {
            '_id': group_by,
            'migrage_all': {'$push': '$$ROOT'}
        }

        project = {f: {"$arrayElemAt": ["$migrage_all." + f, 0]} for f in fields}
        project.update({k: "$_id." + k for k, v in group_by.iteritems()})
        aggregate = [
            {
                '$match': match
            },
            {
                "$group": group
            },
            {
                "$project": project
            },

        ]
        if final_match:
            aggregate.append({'$match': final_match})
        aggregate.append({"$out": new_collection.name})
        collection.aggregate(aggregate, allowDiskUse=True)
        # reconnect collection
        new_collection = collection.database[new_collection_name]

        if new_collection.count() > 0:
            collection.drop()
            new_collection.rename(old_name)
        else:
            new_collection.drop()
        return collection.database[old_name]

    """
        list database name
    """

    @staticmethod
    def list_database():
        return BaseMongoDb.mongoclient.database_names()

    @staticmethod
    def get_max_min_on_collection(field, collection, match={}):
        """
            Get max and min value on a collection
        """
        aggregate = [
            {
                '$match': match
            }
        ]
        max_value = collection.aggregate(aggregate + [
            {
                '$group': {
                    '_id': None,
                    'max': {'$max': '$%s' % field}
                }
            }
        ])

        max_value = list(max_value)
        max_value = max_value[0]['max'] if max_value else None

        min_value = collection.aggregate(aggregate + [
            {
                '$group': {
                    '_id': None,
                    'min': {'$min': '$%s' % field}
                }
            }
        ])
        min_value = list(min_value)
        min_value = min_value[0]['min'] if min_value else None

        return {
            'max': max_value,
            'min': min_value
        }

    @staticmethod
    def print_aggregate(aggregate):
        def dashrepl(matchobj):
            str6 = matchobj.group(6).strip()
            str7 = matchobj.group(7).strip()
            str5 = matchobj.group(5)
            if len(str5) == 1:
                str5 = '0' + str5

            str4 = matchobj.group(4)
            if len(str4) == 1:
                str4 = '0' + str4

            str3 = matchobj.group(3)
            if len(str3) == 1:
                str3 = '0' + str3

            str2 = matchobj.group(2)
            if len(str2) == 1:
                str2 = '0' + str2

            if str5 == '59':
                str6 = '59.999999'
            else:
                str6 = ('00:000' if str6 == '' else str6.replace(', ', ''))

            if str6 == '00.000' or str6 == '59.999999':
                str7 = ''
            else:
                str7 = ('' if str7 == '' else '.' + str7.replace(', ', ''))

            return 'ISODate("' + matchobj.group(
                1) + '-' + str2 + '-' + str3 + 'T' + str4 + ':' + str5 + ':' + str6 + str7 + 'Z")'

        str = pprint.pformat(aggregate)
        str = str.replace(' None', 'null')
        str = str.replace("u'", "'")
        str = str.replace(' True', 'true')
        str = str.replace(' False', 'false')
        str = re.sub(
            'datetime\.datetime\(([0-9]{4}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2})(|, [0-9]{1,2})(|, [0-9]{1,7})\)',
            dashrepl, str)
        return str

    @staticmethod
    def convert2unicode(mydict):
        for k, v in mydict.iteritems():
            if isinstance(v, str):
                mydict[k] = unicode(v, errors='replace')
            elif isinstance(v, dict):
                BaseMongoDb.convert2unicode(v)
        return mydict
