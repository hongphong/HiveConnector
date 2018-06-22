__author__ = 'phongphamhong'

from sqlalchemy.sql import text
from sqlalchemy.engine import create_engine
from Config import Main as main
from Utility import Singleton, timeit, debug_tool, timeit_with_show_params
from SystemLog import SystemLog
import copy
import traceback
import re
import time
import random

"""
#
# @author Phong Pham Hong
# Base class that used to query with db
# @document: http://docs.sqlalchemy.org/en/rel_1_0/core/dml.html
#
"""


class RDbSqlBase(object):
    engine = None
    connector = None
    string_connection = ''
    username = ''
    password = ''
    database = ''
    host = ''
    port = None
    query_string = ""
    params = []
    count_instances = 0

    # config connection that get from main config
    connection_config = {}

    instance = {}

    def __init__(self, **kwargs):
        load_config = kwargs.get('load_config')
        connection_name = kwargs.get('connection_name')
        if load_config:
            self.connection_config = load_config
        elif connection_name and main.SQLACHEMY_DATABASE.get(connection_name):
            self.connection_config = main.SQLACHEMY_DATABASE.get(connection_name)
        self.system_log = None
        self.create_str_connect(**kwargs)
        system_log = kwargs.get('system_log', SystemLog)
        if system_log:
            self.system_log = system_log

    def print_log(self, text):
        if self.system_log:
            self.system_log.print_log('CONNECTION: %s LOG: %s' % (self.get_string_connection(), text))

    def create_str_connect(self, **kwargs):
        connection_config = copy.copy(self.connection_config)
        self.username = kwargs.get('user', connection_config['user'])
        self.password = kwargs.get('password', connection_config['password'])
        self.host = kwargs.get('host', connection_config['host'])
        self.port = kwargs.get('port', connection_config.get('port'))
        self.database = kwargs.get('database', connection_config['database'])
        return self

    def get_string_connection(self):
        return 'mysql+mysqlconnector://' + self.username + ':' + self.password + '@' + self.host + '/' + self.database

    def create_key_singeleton(self):
        return self.__class__.__name__ + ':' + self.get_string_connection()

    """
     * @return mysql.connector.connect
    """

    def create_engine(self, **kwargs):
        from sqlalchemy import create_engine
        self.create_str_connect(**kwargs)
        key = self.get_string_connection()
        RDbSqlBase.instance[key] = create_engine(key)
        return self

    def get_engine(self, **kwargs):
        key = self.get_string_connection()
        if key not in RDbSqlBase.instance or kwargs.get('reload', False):
            self.system_log.print_log('[CREATE CONNECTION] Class: %s Info: %s' % (self.__class__.__name__, key))
            self.create_engine()
        return RDbSqlBase.instance[key]

    def check_query_valid(self, q):
        q = q.replace('\n', ' ')
        q = re.sub('[\s+]', ' ', q)
        if q.strip(' ') == '':
            return False
        return True

    """"
    # @params query: query string
    #         params: params of query
    # @example: query(query="select * from table where primary_key=:key",params={'key':1})
    # @return RDbSqlBase.connector.cursor()
    """

    def query(self, **kwargs):
        self.query_string = kwargs.get('query', '')
        self.params = kwargs.get('params', [])
        if self.query_string:
            db = self.get_engine().connect()
            query = text(self.query_string)
            result = db.execute(query, self.params)
            db.close()
            return result

        return []

    def query_one(self, **kwargs):
        q = self.query(**kwargs)
        to_object = kwargs.get('to_object', False)
        for k in q:
            if to_object:
                return k
            return dict(k)
        return {}

    """
        call store producer
        @example:
            call_proc(query="proc name", params=[list params])
    """

    @timeit_with_show_params
    def call_proc(self, **kwargs):
        self.query_string = kwargs.get('query', '')
        self.params = kwargs.get('params', [])
        if self.query_string:
            connection = self.get_engine().raw_connection()
            cursor = connection.cursor()
            cursor.callproc(self.query_string, self.params)
            connection.commit()
            return True
        return False

    """
        * insert many
        :param
        +table : table name
        +columns: list columns
        +data: list data
        @example:
            insert_many(
                table='tableA',
                columns=["name","age"],
                data=[["phong","18"],[]...]
            )
    """

    @timeit
    def insert_many(self, **kwargs):
        raw = self.get_engine().raw_connection()
        table = kwargs["table"]
        columns = kwargs.get("columns", [])
        data = kwargs["data"]
        if not columns:
            cols = self.execute(query="SHOW COLUMNS FROM %s" % table)
            columns = [k[0] for k in cols]
        insertstring = ','.join(('%s' for k in range(0, len(columns))))
        stmt = "INSERT INTO " + table + " (" + ','.join(columns) + ") VALUES (" + insertstring + ")"
        if kwargs.get('cast_to_string', True):
            xstr = lambda s: None if s is None else (str(s) if s else "")
            data = [(map(xstr, item)) if type(item) is list else (map(xstr, item.values())) for item in data]

        raw.cursor().executemany(stmt, data)
        raw.commit()
        raw.close()
        del data
        return True

    """
        insert one row
        @example:
            insert_one(
                table='tableA'
                data='{"name":"phong","age":18"}'
            )
    """

    @timeit
    def insert_one(self, **kwargs):
        table = kwargs["table"]
        data = kwargs["data"]
        if type(data) is not dict:
            raise ValueError("data must be a dict")

        return self.insert_many(
            table=table,
            columns=data.keys(),
            data=[data]
        )

    """
        * count query
    """

    def count(self, **kwargs):
        query = kwargs["query"] if "query" in kwargs else ""
        params = kwargs["params"] if "params" in kwargs else {}
        return list(self.query(query=query, params=params))[0][0]

    def call_proc_with_result(self, **kwargs):
        self.query_string = kwargs.get('query', '')
        self.params = kwargs.get('params', [])
        if self.query_string:
            connection = self.get_engine().raw_connection()
            cursor = connection.cursor()
            from Libs.Utility import debug_tool
            cursor.callproc(self.query_string, self.params)
            iterator = cursor.stored_results()
            result = []
            for item in iterator:
                data = self.parsing_data(item=item)
                result.append(data)
            cursor.close()
            connection.commit()
            return result
        return None

    def parsing_data(self, **kwargs):
        item = kwargs.get("item", False)
        if item:
            data = item._rows
            columns = item.column_names
            result = []
            if data:
                for row in data:
                    result.append(dict(zip(columns, item._connection.converter.row_to_python(
                        row, item.description))))
            return result

            # query in Mysql

    def execute(self, **kwargs):
        self.query_string = kwargs.get('query', '')
        if self.query_string:
            db = self.get_engine().connect()
            query = text(self.query_string)
            return db.execute(query)

            # query in Mysql


class DbMySql(RDbSqlBase):
    """
    Connect to Mysql
    """

    connection_config = main.SQLACHEMY_DATABASE['mysql']


class HiveSql(RDbSqlBase):
    """
    Connect to hive
    """

    connection_config = main.SQLACHEMY_DATABASE['hive']
    cursor_instance = {}

    def __init__(self, **kwargs):
        host_name = kwargs.get('host_name', 'hive')
        system_log = kwargs.get('system_log')
        # random host
        if kwargs.get('random_host', True):
            self.random_hive_host()
        elif host_name and main.SQLACHEMY_DATABASE.get(host_name):
            self.connection_config = main.SQLACHEMY_DATABASE[host_name]

        self.system_log = None
        if system_log:
            self.system_log = system_log
            # system_log.print_log('[HIVE INFO] HIVE INFO %s' % self.connection_config)
        return super(HiveSql, self).__init__(**kwargs)

    def random_hive_host(self):
        key = random.randint(0, 1)
        key = 'hive%s' % ('_%s' % key if key > 0 else '')

        if main.SQLACHEMY_DATABASE.get(key):
            self.connection_config = main.SQLACHEMY_DATABASE.get(key)
        else:
            self.connection_config = main.SQLACHEMY_DATABASE['hive']
        return self

    def get_string_connection(self):
        return 'hive://' + self.host + ':' + self.port + '/default'

    def create_engine(self, **kwargs):
        self.create_str_connect(**kwargs)
        key = self.create_key_singeleton()
        engine = create_engine(self.get_string_connection(),
                               connect_args={'auth': self.connection_config.get('auth', 'NONE')}).connect()
        RDbSqlBase.instance[key] = engine
        return self

    def get_engine(self, **kwargs):
        """
        :param kwargs:
        :return impala.dbapi.connect:
        """
        key = self.create_key_singeleton()
        ins = RDbSqlBase.instance.get(key)
        if key not in RDbSqlBase.instance or kwargs.get('reload', False) or (ins and ins.closed):
            self.print_log('[HIVE EXECUTE] create connect and add to singleton with connect info: %s' % key)
            self.create_engine()
        return RDbSqlBase.instance[key]

    def query(self, **kwargs):
        kwargs['close_connection'] = False
        return self.execute(**kwargs)

    @timeit
    def execute(self, **kwargs):
        """"
            # @params query: query string
            #         params: params of query
            # @example: query(query="select * from table where primary_key=:key",params={'key':1})
            # @return RDbSqlBase.connector.cursor()
            """
        self.query_string = kwargs.get('query', '')
        self.params = kwargs.get('params', [])
        is_log_query = kwargs.get('is_log_query', True)
        replace_sql = kwargs.get('replace_sql', {})

        result = []
        # replace template attribution
        for item in replace_sql:
            self.query_string = self.query_string.replace('{%s}' % item, ('%s' % replace_sql[item]))

        if self.query_string:
            db = self.get_engine()
            if db.closed:
                db = self.get_engine(reload=True)
            query_string = self.query_string.split(';')

            if len(query_string) == 1:
                query_string = self.query_string.replace(';', ' ')
                if is_log_query:
                    self.print_log('[HIVE EXECUTE] execute query: \n %s' % query_string)
                if self.check_query_valid(query_string):
                    result = db.execute(text(query_string), self.params)

            else:
                for q in query_string:
                    if self.check_query_valid(q):
                        if is_log_query:
                            self.print_log('[HIVE EXECUTE] execute query: \n %s' % q)
                        result.append(db.execute(text(q), self.params))
            # close connection after query
            if kwargs.get('close_connection', self.connection_config.get('auto_close_connection', True)):
                self.close_connection()

        return result

    @timeit
    def insert_many(self, **kwargs):
        """
                * insert many
                :param
                +table : table name
                +columns: list columns
                +data: list data
                @example:
                    insert_many(
                        table='tableA',
                        columns=["name","age"],
                        data=[["phong","18"],[]...]
                    )
            """
        raw = self.get_engine()
        cursor = self.get_cursor()
        table = kwargs["table"]
        columns = kwargs["columns"]
        data = kwargs["data"]
        partition = kwargs.get('partition')

        insertstring = ','.join(('%s' for k in range(0, len(columns))))
        stmt = "INSERT OVERWRITE TABLE " + table
        if partition:
            stmt += " PARTITION (%s) " % partition
        stmt += " (" + ','.join(kwargs["columns"]) + ")"
        stmt += " VALUES (" + insertstring + ")"
        debug_tool(stmt)
        if kwargs.get('cast_to_string', False):
            xstr = lambda s: None if s is None else (str(s) if s else "")
            data = [(map(xstr, item)) if type(item) is list else (map(xstr, item.values())) for item in data]

        cursor.executemany(stmt, data)
        raw.commit()
        del data
        return True

    @timeit
    def execute_many(self, **kwargs):
        """
        execute many quries
        :param kwargs:
        :return:
        """
        return self.execute(**kwargs)

    def close_connection(self):
        self.print_log("[HIVE EXECUTE] Close connection: %s " % self.get_string_connection())
        db = self.get_engine()
        db.detach()
        db.close()

    def close_app(self):
        try:
            key = self.create_key_singeleton()
            if key in RDbSqlBase.instance and not RDbSqlBase.instance[key].closed:
                self.print_log("[HIVE EXECUTE] Close Hive App on Yarn")
                self.close_connection()
                del RDbSqlBase.instance[key]
        except BaseException as e:
            self.print_log('Cannot close hive app because: \n %s' % self.system_log.print_tracekback())
            pass

    def __del__(self):
        self.print_log("[HIVE EXECUTE] Destroy object...")
        try:
            self.close_app()
        except BaseException as e:
            self.print_log('Cannot del this object because: \n %s' % self.system_log.print_tracekback())
        del self


class SparkSql(RDbSqlBase):
    """
    connect to Spark through Thrift Server
    """
    connection_config = main.SQLACHEMY_DATABASE['spark']
    __columns = []
    __time_sleep_retry = 30

    def __init__(self, **kwargs):

        host_name = kwargs.get('host_name', 'spark')
        system_log = kwargs.get('system_log')
        if host_name and main.SQLACHEMY_DATABASE.get(host_name):
            self.connection_config = main.SQLACHEMY_DATABASE[host_name]
        self.system_log = None
        if system_log:
            self.system_log = system_log
            # system_log.print_log('[SPARK INFO]SPARK INFO %s' % self.connection_config)
        return super(SparkSql, self).__init__(**kwargs)

    def get_string_connection(self):
        return 'hive://%s:%s/%s' % (self.host, self.port, self.database)

    def create_engine(self, **kwargs):
        self.create_str_connect(**kwargs)
        key = self.get_string_connection()
        engine = create_engine(key)
        RDbSqlBase.instance[key] = engine
        return self

    def execute(self, **kwargs):
        """"
            # @params query: query string
            #         params: params of query
            # @example: query(query="select * from table where primary_key=:key",params={'key':1})
            # @return RDbSqlBase.connector.cursor()
            """
        is_log_query = kwargs.get('is_log_query', False)
        replace_sql = kwargs.get('replace_sql', {})
        number_retry = kwargs.get('number_retry', 0)
        self.query_string = kwargs.get('query', '')
        self.params = kwargs.get('params', [])

        self.__columns = []
        result = []

        # replace template attribution
        for item in replace_sql:
            self.query_string = self.query_string.replace('{%s}' % item, ('%s' % replace_sql[item]))

        if self.query_string:
            # retry connect
            try:
                db = self.get_engine(reload=bool(number_retry)).connect()
                query_string = self.query_string.split(';')
                if len(query_string) == 1:
                    if is_log_query:
                        self.print_log('[SPARK EXECUTE] execute query: \n %s' % self.query_string)
                    result = db.execute(text(self.query_string), self.params)
                else:
                    for q in query_string:
                        if self.check_query_valid(q):
                            if is_log_query:
                                self.print_log('[SPARK EXECUTE] execute query: \n %s' % q)
                            result.append(db.execute(text(q), self.params))
                db.close()
                number_retry = 0
            except BaseException as e:
                traceback_mes = traceback.format_exc()
                detect = ['TTransportException', 'Broken pipe', 'TSocket.py', 'Connection reset', '%s' % self.host]
                raise_error = True
                for k in detect:
                    if traceback_mes.find(k) >= 0:
                        raise_error = False
                        break

                if raise_error:
                    raise ValueError("Error when query data on SparkSQL:\n Message: %s\n Traceback: %s'n" % (
                        e.message, traceback_mes))
                else:
                    self.print_log(
                        "[SPARK EXECUTE] Spark connection need to retry. Traceback message is: \n %s" % traceback_mes)
                    number_retry += 1
                    if number_retry >= 4:
                        raise ValueError(
                            "[SPARK EXECUTE] Error when connect to Spark Thrift: %s. \n Traceback: \n%s" % (
                                e.message, traceback.format_exc()))
                    self.print_log(
                        "[SPARK EXECUTE] Retry connection after sleeping %s(s)...Number of retry: %s" % (
                            self.__time_sleep_retry, number_retry))
                    time.sleep(self.__time_sleep_retry)
                    kwargs['number_retry'] = number_retry
                    return self.execute(**kwargs)

        return result

    def query(self, **kwargs):
        callback_result = kwargs.get('callback_result', lambda x: x)
        result = self.execute(**kwargs)
        skip_result = kwargs.get('skip_result', 0)
        n = 0
        for item in result:
            n += 1
            if skip_result > 0 and n <= skip_result:
                continue
            rs = {k.lower(): v for k, v in dict(item).iteritems()}
            yield callback_result(rs)

    def query_one(self, **kwargs):
        rs = self.query(**kwargs)
        rs = list(rs)
        return rs[0] if rs else None

    @timeit
    def execute_many(self, **kwargs):
        """
        execute many quries
        :param kwargs:
        :return:
        """
        self.query_string = kwargs.get('query', [])

        if self.query_string:
            for q in self.query_string:
                self.execute(query=q)

    @timeit
    def execute_from_text(self, **kwargs):
        """
        execute many quries with sql text (includes symbol ;)
        :param kwargs:
        :return:
        """
        self.query_string = kwargs.get('query', '')
        query = self.query_string.split(';')
        params = kwargs.get('params', {})
        return self.execute(query=self.query_string, params=params)

    def get_columns(self):
        return self.__columns
