import logging
import os

from flask import Flask

app = Flask(__name__)

from meerkat_consul.config import dhis2_config

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
handler = logging.StreamHandler()
formatter = logging.Formatter(FORMAT)
handler.setFormatter(formatter)
logger = logging.getLogger("meerkat_consul")
level_name = dhis2_config.get("loggingLevel", "ERROR")
level = logging.getLevelName(level_name)
logger.setLevel(level)
logger.addHandler(handler)

api_url = os.environ.get('MEERKAT_API_URL', 'http://nginx/api')




@app.route('/')
def root():
    return '{"name":"meerkat_consul"}'

if __name__ == '__main__':
    from meerkat_consul.export import locations
    locations()
    # app.run(host="0.0.0.0")
