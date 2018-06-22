__author__ = 'phongphamhong'

from Utility import current_time_utc, current_time_with_tz, debug_tool, convert_utc_to_timzone
from Config import Main as config
from Logger import logger, logging

import traceback
import time
import smtplib
from email.mime.text import MIMEText
import os
import inspect
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

from bson.objectid import ObjectId


class SystemLog(object):
    STATUS_SUCCESS = 1
    STATUS_ERROR = 0

    LOGS_FOR_OBJECT = 'object'
    LOGS_FOR_REPORT = 'report'
    LOGS_FOR_SUBMIT_URL_TAGS = 'submit_urltags'

    DATABASE_LOGS_NAME = 'metrixa_schedule_job'
    TABLE_LOGS_PREFIX = 'log'

    DEBUG_MODE = False

    database = None
    table = None
    status = STATUS_ERROR
    message = ''
    ads_service = ''
    account_id = ''
    object_type = ''
    object_id = ''
    log_start = ''
    log_end = ''
    current_file_log = None

    def __init__(self, ads_service, account_id='', timezone=''):
        self.ads_service = ads_service
        self.account_id = str(account_id)
        self.database = SystemLog.get_database()
        self.log_start = self.get_time_now()
        self.timezone = timezone

    @staticmethod
    def get_database():
        from Libs.Db.BaseMongoDb import BaseMongoDb
        return BaseMongoDb.db(SystemLog.DATABASE_LOGS_NAME)

    @staticmethod
    def get_collection(collection=''):
        if collection:
            collection = str(collection)
            return SystemLog.get_database()[collection]

    def init_table_log(self):
        return self.get_collection('_'.join([self.ads_service, self.TABLE_LOGS_PREFIX]))

    def prepare_default_logs(self):
        default = {
            'status': self.status,
            'account_id': str(self.account_id),
            'start_time': self.log_start,
            'end_time': self.log_end,
            'start_time_timezone': self.log_start if self.timezone == '' else convert_utc_to_timzone(tz=self.timezone,
                                                                                                     date_time=self.log_start)
        }
        return {k: v for k, v in default.items() if v}

    def insert_logs(self, data={}):
        self.log_end = self.get_time_now()
        ins = self.prepare_default_logs()

        ins.update(data)
        if 'account_id' in ins:
            ins['account_id'] = str(ins['account_id'])

        SystemLog.print_log(data)
        return self.init_table_log().insert([ins])

    def update_logs(self, job_id, data={}):
        self.log_end = self.get_time_now()

        if 'account_id' in data:
            data['account_id'] = str(data['account_id'])
        list_del = ['job_id', 'start_time', 'start_time_timezone']
        for i in list_del:
            if i in data:
                del data[i]
        data['end_time'] = current_time_utc()
        return self.init_table_log().update({
            'job_id': job_id
        },
            {
                '$set': data
            }
        )

    @staticmethod
    def create_unique_job_id():
        import uuid

        return str(uuid.uuid4())

    @staticmethod
    def create_unique_job_object_id():
        return ObjectId()

    @staticmethod
    def mkdir(directory):
        directory = os.path.dirname(directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def create_path_file_handle_log(file, after_fix=''):
        after_fix = '_' + str(after_fix) if after_fix else ''
        file = file + '_' if file else ''
        log_path = config.LOG_JOB_PATH + file + current_time_utc().strftime(
            '%Y_%m_%d').__str__() + after_fix + '.log'
        return log_path

    @staticmethod
    def set_file_handle_log(file, after_fix='', reset_file=False):
        log_path = SystemLog.create_path_file_handle_log(file=file, after_fix=after_fix)
        SystemLog.mkdir(log_path)
        if reset_file:
            f = open(log_path, 'w')
            f.close()
        for hdlr in logger.handlers[:]:  # remove all old handlers
            logger.removeHandler(hdlr)

        hdlr = logging.FileHandler(log_path)
        logger.addHandler(hdlr)

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        formatter.converter = time.gmtime
        hdlr.setFormatter(formatter)
        SystemLog.current_file_log = log_path

        return log_path

    @staticmethod
    def set_email_handle_log(**kwargs):
        pass
        return
        subject = kwargs.get('subject', '')
        fromadds = kwargs.get('fromadd', config.MAIL_ERROR_CONFIG['from'])
        smtp_handler = logging.handlers.SMTPHandler(
            mailhost=(config.MAIL_ERROR_CONFIG['mailhost'], config.MAIL_ERROR_CONFIG['port']),
            fromaddr=fromadds,
            toaddrs=config.MAIL_ERROR_CONFIG['to'],
            # credentials=(),
            subject=subject)
        smtp_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(smtp_handler)

    @staticmethod
    def get_time_now():
        return current_time_utc(reset_time=False)

    @staticmethod
    def convert_message(mes):
        if type(mes) is list:
            mes = '\n'.join(mes)
        return str(mes)

    @staticmethod
    def print_log(text, header=""):
        if header:
            header = header + " \n"
        if SystemLog.DEBUG_MODE is True:
            func = inspect.currentframe().f_back.f_code
            logging.info("%s: file %s.%s() at line %i" % (
                '[DEBUG INFO]',
                os.path.basename(func.co_filename),
                func.co_name,
                func.co_firstlineno
            ))
        logger.info(header + SystemLog.convert_message(text))

    @staticmethod
    def print_error(text, header=""):
        if header:
            header = header + " \n"
        logger.error(header + SystemLog.convert_message(text))

    @staticmethod
    def print_tracekback():
        return traceback.format_exc()

    @staticmethod
    def send_email(**kwargs):
        """
            send email when meet errors
        :return:
        """
        # me == the sender's email address
        # you == the recipient's email address
        subject = kwargs.get('subject', '')
        content = kwargs.get('content', '')
        attach_file = kwargs.get('attact_file', None)
        text_subtype = kwargs.get('text_subtype', '')
        from_str = kwargs.get('from_who', config.MAIL_ERROR_CONFIG['from'])
        if kwargs.get('from_type') == 'from_download_job':
            from_str = config.MAIL_ERROR_CONFIG['from_download_job']
        elif kwargs.get('from_type') == 'from_convtrack_job':
            from_str = config.MAIL_ERROR_CONFIG['from_convtrack_job']
        elif kwargs.get('from_type') == 'from_link_account':
            from_str = config.MAIL_ERROR_CONFIG['from_link_account']

        msg = MIMEText(content, text_subtype)
        msg['Subject'] = subject
        msg['From'] = from_str
        to = kwargs.get('to', config.MAIL_ERROR_CONFIG['to'])
        if len(to) == 0:
            return
        msg['To'] = ','.join(to)
        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        smtplib.SMTP_SSL_PORT
        if attach_file:
            m_msg = MIMEMultipart()
            m_msg['From'] = msg['From']
            m_msg['Subject'] = msg['Subject']
            m_msg['To'] = msg['To']
            m_msg.attach(msg)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attach_file, "rb").read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(attach_file))
            m_msg.attach(part)
            msg = m_msg

        s = smtplib.SMTP()
        s.connect(host=config.MAIL_ERROR_CONFIG['mailhost'], port=config.MAIL_ERROR_CONFIG['port'])
        if config.MAIL_ERROR_CONFIG['credentials']:
            s.login(user=config.MAIL_ERROR_CONFIG['credentials'][0],
                    password=config.MAIL_ERROR_CONFIG['credentials'][1])
        s.sendmail(from_str, to, msg.as_string())
        s.quit()
