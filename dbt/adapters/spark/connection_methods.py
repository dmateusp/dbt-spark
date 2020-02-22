from builtins import object


class ConnectionMethod(object):
    THRIFT = 'thrift'
    HTTP = 'http'
    SPARK_SHELL = 'spark-shell'

    _ALL_METHODS = set()

    @classmethod
    def all_methods(cls):
        if not cls._ALL_METHODS:
            cls._ALL_METHODS = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_METHODS
