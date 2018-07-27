class Config(object):
    DEBUG = False
    TESTING = False
    PRODUCTION = False

    LOGGING_LEVEL = "ERROR"
    LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    COUNTRY_LOCATION_ID = 1



class Production(Config):
    PRODUCTION = True


class Development(Config):
    DEBUG = True

    LOGGING_LEVEL = "DEBUG"


class Testing(Config):
    TESTING = True

    LOGGING_LEVEL = "WARNING"


