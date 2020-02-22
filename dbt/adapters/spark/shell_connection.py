from subprocess import Popen
import dbt

class ShellConnection():
    def __init__(self, creds):
        self.shell_type = creds.spark_shell['shell_type']
        self.shell_cmd = creds.spark_shell['cmd']

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ):
        "A tuple of the status and the results"
        if fetch:
            Popen(self.shell_cmd, shell=True)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return 'OK', table
