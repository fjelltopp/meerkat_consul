import logging
import os

from flask import Flask
from flask_restful import Api

app = Flask(__name__)
api = Api(app)

from meerkat_consul.config import dhis2_config

logger = logging.getLogger("meerkat_consul")
if not logger.handlers:
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler = logging.StreamHandler()
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    level_name = dhis2_config.get("loggingLevel", "ERROR")
    level = logging.getLevelName(level_name)
    logger.setLevel(level)
    logger.addHandler(handler)

api_url = os.environ.get('MEERKAT_API_URL', 'http://nginx/api')

from meerkat_consul.export import ExportLocationTree, ExportFormFields, ExportEvent

api.add_resource(ExportLocationTree, "/dhis2/export/locationTree")
api.add_resource(ExportFormFields, "/dhis2/export/formFields")
api.add_resource(ExportEvent, "/dhis2/export/events")

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
