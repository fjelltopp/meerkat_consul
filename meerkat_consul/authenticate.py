import os

import backoff

from meerkat_consul import app
from meerkat_libs import authenticate

filename = os.environ.get('MEERKAT_AUTH_SETTINGS')
exec(compile(open(filename, "rb").read(), filename, 'exec'))

@backoff.on_predicate(backoff.expo, lambda x: x == '', max_tries=14)
def get_token():
    return authenticate('root','password')

if not app.config['TESTING']:
    headers = {'Authorization': JWT_HEADER_PREFIX + get_token()}
else:
    headers = {'Authorization': JWT_HEADER_PREFIX + 'TESTING'}


