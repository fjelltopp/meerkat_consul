import os

import backoff
import requests

from meerkat_consul import app, logger
from meerkat_libs import authenticate

filename = os.environ.get('MEERKAT_AUTH_SETTINGS')
exec(compile(open(filename, "rb").read(), filename, 'exec'))

CONSUL_AUTH_USERNAME = os.environ.get('CONSUL_AUTH_USERNAME', 'consul-dev-user')
CONSUL_AUTH_PASSWORD = os.environ.get('CONSUL_AUTH_PASSWORD', 'password')
consul_auth_token_ = ''


def retry_message(i):
    logger.debug("Failed to authenticate. Retrying in " + str(i))

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      on_backoff=retry_message,
                      max_tries=8,
                      max_value=30)
@backoff.on_predicate(backoff.expo,
                      lambda x: x == '' or not x,
                      max_tries=13,
                      max_value=30)
def get_token():
    global consul_auth_token_
    consul_auth_token_ = authenticate(username=CONSUL_AUTH_USERNAME,
                                      password=CONSUL_AUTH_PASSWORD,
                                      current_token=consul_auth_token_)
    logger.debug("Got token from auth: %s", consul_auth_token_)
    return consul_auth_token_


def meerkat_headers():
    if not app.config['TESTING']:
        token = get_token()
        if token == None:
            token = "NONE"
        return {'Authorization': JWT_HEADER_PREFIX + token}
    else:
        return {'Authorization': JWT_HEADER_PREFIX + 'TESTING'}
