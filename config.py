class Config(object):
    DEBUG = False
    TESTING = False
    PRODUCTION = False

    LOGGING_LEVEL = "ERROR"
    LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    COUNTRY_LOCATION_ID = 1

    DHIS2_CONFIG = {
        "url": "http://172.17.0.1:8085",
        "apiResource": "/api/27",
        "credentials": ('senyoni', 'admin'),
        "headers": {
            "Content-Type": "application/json",
            "Authorization": "Basic c2VueW9uaTphZG1pbg=="
        }
    }


class Production(Config):
    PRODUCTION = True


class Development(Config):
    DEBUG = True

    LOGGING_LEVEL = "INFO"


class Testing(Config):
    TESTING = True

    LOGGING_LEVEL = "WARNING"


