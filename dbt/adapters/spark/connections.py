from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.spark.connection_methods import ConnectionMethod
from dbt.adapters.spark.spark_shell_types import SparkShellType
from dbt.adapters.spark.thrift_connection_wrapper import ThriftConnectionWrapper
from dbt.adapters.spark.shell_connection import ShellConnection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.compat import NUMBERS
import dbt.exceptions

from TCLIService.ttypes import TOperationState as ThriftState
from thrift.transport import THttpClient
from pyhive import hive
from datetime import datetime

import base64
import time

# adding organization as a parameter, as it is required by Azure Databricks
# and is different per workspace.
SPARK_CONNECTION_URL = '''
    https://{host}:{port}/sql/protocolv1/o/{organization}/{cluster}
    '''.strip()

SPARK_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'method': {
            'enum': ConnectionMethod.all_methods(),
        },
        'host': {
            'type': 'string'
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'user': {
            'type': 'string'
        },
        'organization': {
            'type': 'string'
        },
        'cluster': {
            'type': 'string'
        },
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'token': {
            'type': 'string',
        },
        'connect_timeout': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 60,
        },
        'connect_retries': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 60,
        },
        'spark_shell': {
            'type': 'object',
            'properties': {
                'shell_type': {
                    'enum': SparkShellType.all_types(),
                },
                'cmd': {
                    'type': 'string'
                }
            }
        }
    },
    'required': ['method', 'host', 'database', 'schema'],
}


class SparkCredentials(Credentials):
    SCHEMA = SPARK_CREDENTIALS_CONTRACT

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('database', kwargs.get('schema'))

        # coercing org to a string since it is unknown whether
        # Azure Databricks will always keep it numeric
        if 'organization' in kwargs:
            kwargs['organization'] = str(kwargs['organization'])
        else:
            kwargs['organization'] = '0'

        super(SparkCredentials, self).__init__(*args, **kwargs)

    @property
    def type(self):
        return 'spark'

    def _connection_keys(self):
        return ('host', 'port', 'cluster', 'schema', 'organization')


class SparkConnectionManager(SQLConnectionManager):
    TYPE = 'spark'

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, 'status'):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.RuntimeException(msg)
            else:
                raise dbt.exceptions.RuntimeException(str(exc))

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds, required, method=None):
        if method is None:
            method = creds.method
        for key in required:
            if key not in creds:
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Spark".format(key, method))


    @classmethod
    def _open_http(cls, creds):
        cls.validate_creds(creds, ['token', 'host', 'port',
                                'cluster', 'organization'])

        conn_url = SPARK_CONNECTION_URL.format(**creds)
        transport = THttpClient.THttpClient(conn_url)

        raw_token = "token:{}".format(creds.token).encode()
        token = base64.standard_b64encode(raw_token).decode()
        transport.setCustomHeaders({
            'Authorization': 'Basic {}'.format(token)
        })

        return hive.connect(thrift_transport=transport)


    @classmethod
    def _open_thrift(cls, creds):
        cls.validate_creds(creds, ['host'])

        return hive.connect(host=creds.host,
                            port=creds.get('port'),
                            username=creds.get('user'))


    @classmethod
    def _open_spark_shell(cls, creds):
        cls.validate_creds(creds, ['spark_shell'])
        cls.validate_creds(creds['spark_shell'], ['shell_type', 'cmd'], method=creds.method)
        return ShellConnection(creds)


    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        creds = connection.credentials
        connect_retries = creds.get('connect_retries', 0)
        connect_timeout = creds.get('connect_timeout', 10)
        exc = None
        for i in range(1 + connect_retries):
            try:
                if creds.method == ConnectionMethod.HTTP:
                    conn = cls._open_http(creds)
                elif creds.method == ConnectionMethod.THRIFT:
                    conn = cls._open_thrift(creds)
                elif creds.method == ConnectionMethod.SPARK_SHELL:
                    conn = cls._open_spark_shell(creds)
                break
            except Exception as e:
                exc = e
                if getattr(e, 'message', None) is None:
                    raise

                message = e.message.lower()
                is_pending = 'pending' in message
                is_starting = 'temporarily_unavailable' in message

                warning = "Warning: {}\n\tRetrying in {} seconds ({} of {})"
                if is_pending or is_starting:
                    logger.warning(warning.format(e.message, connect_timeout,
                                                  i + 1, connect_retries))
                    time.sleep(connect_timeout)
                else:
                    raise
        else:
            raise exc

        if creds.method in [ConnectionMethod.HTTP, ConnectionMethod.THRIFT]:
            wrapped = ThriftConnectionWrapper(conn)
        elif creds.method == ConnectionMethod.SPARK_SHELL:
            wrapped = conn  # no need to wrap this conn

        connection.state = 'open'
        connection.handle = wrapped
        connection.method = creds.method
        return connection

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    def cancel(self, connection):
        connection.handle.cancel()

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ):
        connection = self.get_thread_connection()
        if connection.method in [ConnectionMethod.HTTP, ConnectionMethod.THRIFT]:
            return self.execute(sql, auto_begin, fetch)
        elif connection.method == ConnectionMethod.SPARK_SHELL:
            return connection.handle.execute(sql, fetch)
