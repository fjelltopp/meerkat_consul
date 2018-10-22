class Config(object):
    DEBUG = False
    TESTING = False
    PRODUCTION = False

    LOGGING_LEVEL = "ERROR"
    LOGGING_FORMAT = '%(asctime)s - %(levelname)-7s - %(module)s:%(filename)s:%(lineno)d - %(message)s'

    COUNTRY_LOCATION_ID = 1



class Production(Config):
    PRODUCTION = True


class Development(Config):
    DEBUG = True

    LOGGING_LEVEL = "DEBUG"


class Testing(Config):
    TESTING = True

    LOGGING_LEVEL = "WARNING"


