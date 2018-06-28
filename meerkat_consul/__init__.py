import logging
import os

import backoff as backoff
import requests
from flask import Flask
from flask_restful import Api

app = Flask(__name__)
app.config.from_object(os.getenv('CONFIG_OBJECT', 'config.Development'))

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


@backoff.on_predicate(backoff.expo, max_tries=8, max_value=30)
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=30)
def wait_for_api():
    return requests.get(api_url).text


wait_for_api()

from meerkat_consul.export import dhis2_export
app.register_blueprint(dhis2_export)


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
