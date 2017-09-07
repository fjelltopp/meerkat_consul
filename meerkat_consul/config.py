# Need this module to be importable without the whole of meerkat_auth config
# Directly load secret settings file from which to import required variables
# File must include JWT_COOKIE_NAME, JWT_ALGORITHM and JWT_PUBLIC_KEY variables
import calendar
import os
import time
import jwt

filename = os.environ.get('MEERKAT_AUTH_SETTINGS')
exec(compile(open(filename, "rb").read(), filename, 'exec'))

# We need to authenticate our tests using the dev/testing rsa keys
token_payload = {
    u'acc': {
        u'demo': [u'manager', u'registered'],
        u'jordan': [u'manager', u'registered'],
        u'madagascar': [u'manager', u'registered']
    },
    u'data': {u'name': u'Testy McTestface'},
    u'usr': u'testUser',
    u'exp': calendar.timegm(time.gmtime()) + 100000,  # Lasts for 1000 seconds
    u'email': u'test@test.org.uk'
}
token = jwt.encode(token_payload,
                   JWT_SECRET_KEY,
                   algorithm=JWT_ALGORITHM).decode("utf-8")

headers = {'Authorization': JWT_HEADER_PREFIX + token}
headers_non_authorised = {'Authorization': ''}


dhis2_config = {
    "url": "http://dhis2-web:8080",
    "apiResource": "/api/26/",
    "credentials": ('admin', 'district'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
    },
    "loggingLevel": "DEBUG",
    "forms": [
        {
            "name": "demo_case",
            "event_date": "pt./visit_date",
            "completed_date": "end",
            # "programId": "fgrH0jPDNEP", # optional
            "status": "COMPLETED"
        },
        {
            "name": "demo_alert",
            "date": "end"
        },
        {
            "name": "demo_register",
            "date": "intro./visit_date"
        }
    ]
}

COUNTRY_LOCATION_ID = 1
