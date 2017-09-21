import os
from meerkat_libs import authenticate

filename = os.environ.get('MEERKAT_AUTH_SETTINGS')
exec(compile(open(filename, "rb").read(), filename, 'exec'))

token = authenticate('root','password')
headers = {'Authorization': JWT_HEADER_PREFIX + token}


