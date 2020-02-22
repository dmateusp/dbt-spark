from builtins import object


class SparkShellType(object):
    SQL = 'sql'
    PYTHON = 'python'
    SCALA = 'scala'

    _ALL_TYPES = set()

    @classmethod
    def all_types(cls):
        if not cls._ALL_TYPES:
            cls._ALL_TYPES = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_TYPES
