"""
"""
__author__ = 'phongphamhong'
import os


class Main():
    MACHINE_NAME = '192.168.4.174'
    """
        Define config for some serivces
    """
    """
    DEV BEGIN
    """
    MONGODB_CONNECTION = {
        'main': {
            'host': 'x.x.x.x',
            "port": "27017",
            'database': "xxx",
            'user': "",
            'password': "",
            "auth_db": "",
            "options": {
                "serverSelectionTimeoutMS": 10 * 1000
            }

        },
        'other_host': {
            'host': 'x.x.x.x',
            "port": "27017",
            'database': "xxx",
            'user': "",
            'password': "",
            "auth_db": "",
            "options": {
                "serverSelectionTimeoutMS": 10 * 1000
            }

        }

    }

    ENGINE_STORE_DATA = [
        "hive",
        # "mysql"
    ]

    SQLACHEMY_DATABASE = {
        'mysql': {
            'host': 'x.x.x.x',
            'database': "xxxx",
            'user': "x",
            'password': "x",
        },

        'spark': {
            'host': 'x.x.x.x',
            'port': '10016',
            'user': 'x',
            'database': 'x',
            'password': '',
            # 'auth': 'NOSASL'
        },

        'hive': {
            'host': "xxxx",
            'database': "default",
            'user': "root",
            'password': "hadoop",
            'port': '10000',
            'auto_close_connection': False,
            'auth': 'NOSASL'
        }

    }

    """
        config mail to sent error log
    """
    MAIL_ERROR_CONFIG = {
        'mailhost': 'host_mail',
        'port': 25,
        'from': "xxx@xxx.com",
        'from_noti': "xxx@xxx.com",
        'to': [
            "someone@gmail.com"
        ],
        'credentials': [],
    }
