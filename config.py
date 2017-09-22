class Config(object):
    DEBUG = False
    TESTING = False
    PRODUCTION = False

    LOGGING_LEVEL = "ERROR"
    LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    COUNTRY_LOCATION_ID = 1

    DHIS2_CONFIG = {
        "url": "http://dhis2-web:8080",
        "apiResource": "/api/26/",
        "credentials": ('admin', 'district'),
        "headers": {
            "Content-Type": "application/json",
            "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
        }
    }


class Production(Config):
    PRODUCTION = True


class Development(Config):
    DEBUG = True

    LOGGING_LEVEL = "WARNING"


class Testing(Config):
    TESTING = True

    LOGGING_LEVEL = "WARNING"


