import logging
import os
from json.decoder import JSONDecodeError

import backoff as backoff
import requests
from flask import Flask

app = Flask(__name__)
app.config.from_object(os.getenv('CONFIG_OBJECT', 'config.Development'))
app.config.from_pyfile(os.getenv('MEERKAT_CONSUL_SETTINGS'), silent=True)
logger = logging.getLogger("meerkat_consul")
if not logger.handlers:
    logging_format = app.config['LOGGING_FORMAT']
    logging_level_ = app.config['LOGGING_LEVEL']
    handler = logging.StreamHandler()
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    level = logging.getLevelName(logging_level_)

    logger.setLevel(level)
    logger.addHandler(handler)

    backoff_logger = logging.getLogger('backoff')
    backoff_logger.setLevel(logging_level_)
    backoff_logger.addHandler(handler)

api_url = os.environ.get('MEERKAT_API_URL', 'http://nginx/api')

from meerkat_consul.authenticate import meerkat_headers

@backoff.on_predicate(backoff.expo, max_tries=20, max_value=45)
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=20, max_value=45)
@backoff.on_exception(backoff.expo, JSONDecodeError, max_tries=20, max_value=45)
def wait_for_api_init():
    requests.get("{}/locations".format(api_url), headers=meerkat_headers()).json()
    return requests.get(api_url).text


@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=20, max_value=45)
@backoff.on_predicate(backoff.expo,
                      lambda x: x != 'WHO',
                      max_tries=20,
                      max_value=45)
def wait_for_api_start():
    return requests.get(api_url, headers=meerkat_headers()).text



wait_for_api_start()
wait_for_api_init()

from meerkat_consul.export import dhis2_export, export_form_fields

app.register_blueprint(dhis2_export)

export_form_fields()

@app.route('/')
def root():
    return '{"name":"meerkat_consul"}'


if __name__ == '__main__':
    # from meerkat_consul.export import ExportFormFields, ExportLocationTree
    # ExportLocationTree().get()
    # while(True):
    #     ExportFormFields().get()
    #
    app.run(host="0.0.0.0", debug=True, use_reloader=False)
