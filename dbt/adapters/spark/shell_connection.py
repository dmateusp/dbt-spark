from subprocess import Popen, PIPE
import dbt
from dbt.logger import GLOBAL_LOGGER as logger
import pexpect
from dbt.exceptions import DatabaseException
import re

class ShellConnection():
    def __init__(self, creds):
        self.shell_type = creds.spark_shell['shell_type']
        self.shell_cmd = creds.spark_shell['cmd']
        self.session = pexpect.spawn(self.shell_cmd)
        #  Wait for shell to init
        logger.debug('Starting Spark shell with: ' + self.shell_cmd)
        self.session.expect('scala>')
        self._log_shell_output(self.session.before.decode('utf-8'))

    def _log_shell_output(self, last_output):
        logger.debug(
            'Spark shell output: \n\t' + last_output.replace('\n', '\n\t')
        )

    def _check_if_exception(self, last_output):
        last_output = re.sub(
            'spark\.sql\(""".*"\)',
            "",
            last_output,
            flags=re.S
        )
        if 'Exception' in last_output:
            raise DatabaseException(last_output.partition(';')[0].replace('\n', ''))
        elif 'error:' in last_output:
            raise DatabaseException(last_output)

    def shell_execute_sql(self, sql: str, fetch: bool):
        self.session.sendline(self.wrap_sql(sql, fetch))
        self.session.expect('scala>')
        last_output = self.session.before.decode('utf-8')
        self._log_shell_output(last_output)
        self._check_if_exception(last_output)

    def wrap_sql(self, sql: str, fetch: bool):
        wrapped = sql.replace('\n\n', '\n')
        if sql.strip().endswith(";"):
            wrapped = sql.strip()[:-1]

        wrapped = 'spark.sql("""{}""")'.format(wrapped)
        if fetch:
            wrapped = wrapped + '.coalesce(1).write.mode("overwrite").csv("/tmp/a.csv")'
        return wrapped

    def execute(
        self, sql: str, fetch: bool = False
    ):
        "A tuple of the status and the results"
        self.shell_execute_sql(sql, fetch)
        if fetch:
            table = dbt.clients.agate_helper.empty_table()
        else:
            table = dbt.clients.agate_helper.empty_table()
        return 'OK', table

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def close(self):
        self.session.close()
