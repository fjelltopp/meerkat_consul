import json

from datetime import datetime
import requests
from flask_restful import abort, Resource

from meerkat_consul import app, dhis2_config, logger, api_url
from meerkat_consul.config import COUNTRY_LOCATION_ID, headers
from meerkat_consul.decorators import get, post, put
from meerkat_consul.dhis2 import NewIdsProvider

__codes_to_ids = {}
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]

dhis2_ids = NewIdsProvider(dhis2_api_url, dhis2_headers)

COUNTRY_PARENT = 'ImspTQPwCqd'  # for testing with demo DHIS2 server, country should have no parent


class ExportLocationTree(Resource):
    __codes_to_dhis2_ids = {}

    def get(self):
        location_tree = requests.get("{}/locationtree".format(api_url), headers=headers)
        country = location_tree.json()
        country_details = get("{}/location/{!r}".format(api_url, COUNTRY_LOCATION_ID)).json()

        dhis2_organisation_code = country_details["country_location_id"]
        __url = "{}/organisationUnits?filter=code:eq:{}".format(dhis2_api_url, dhis2_organisation_code)
        dhis2_country_resp = get(__url, headers=dhis2_headers)
        dhis2_country_details = dhis2_country_resp.json().get("organisationUnits", [])
        if dhis2_country_details:
            self.__abort_if_more_than_one(dhis2_country_details, dhis2_organisation_code)
            dhis2_parent_id = dhis2_country_details[0]["id"]
        else:
            dhis2_parent_id = self.__create_new_dhis2_organisation(country_details, COUNTRY_PARENT)

        child_locations = country["nodes"]
        self.__populate_child_locations(dhis2_parent_id, child_locations)

        return 'ok'

    @staticmethod
    def __abort_if_more_than_one(dhis2_country_details, dhis2_organisation_code):
        if len(dhis2_country_details) > 1:
            logger.error("Received more than one organisation for given code: %s", dhis2_organisation_code)
            abort(500)

    @staticmethod
    def __populate_child_locations(dhis2_parent_id, locations):
        for location in locations:
            loc_id = location["id"]
            location_details = requests.get("{}/location/{!r}".format(api_url, loc_id)).json()

            id = ExportLocationTree.__create_new_dhis2_organisation(location_details, dhis2_parent_id)
            location_code = location_details["country_location_id"]
            ExportLocationTree.__codes_to_dhis2_ids[location_code] = id

            child_locations = location["nodes"]
            ExportLocationTree.__populate_child_locations(id, child_locations)

    @staticmethod
    def __create_new_dhis2_organisation(location_details, dhis2_parent_id):
        name = location_details["name"]
        country_location_id = location_details["country_location_id"]
        # skip if organisation with given code already exists
        dhis2_resp = get("{}organisationUnits?filter=code:eq:{}".format(dhis2_api_url, country_location_id),
                         headers=dhis2_headers)
        dhis2_organisation = dhis2_resp.json().get("organisationUnits", [])
        if dhis2_organisation:
            logger.info("Location %s with code %s already exists in dhis2.", name, country_location_id)
            return dhis2_organisation[0]["id"]

        uid = dhis2_ids.pop()
        if location_details["start_date"]:
            opening_date = datetime.strptime(location_details["start_date"], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
        else:
            opening_date = "1970-01-01"
        json_dict = {
            "id": uid,
            "name": name,
            "shortName": name,
            "code": country_location_id,
            "openingDate": opening_date,
            "parent": {"id": dhis2_parent_id}
        }
        payload = json.dumps(json_dict)
        response = post("{}organisationUnits".format(dhis2_api_url), headers=dhis2_headers, data=payload)
        logger.info("Created location %s with response %d", name, response.status_code)
        logger.info(response.text)
        return uid


class ExportFormFields(Resource):
    dhis2_code_to_ids = {}

    def get(self):
        print(str(datetime.now().time()) + " Start!")
        dhis2_data_elements_res = get("{}dataElements?paging=False".format(dhis2_api_url), headers=dhis2_headers)
        dhis2_data_elements = dhis2_data_elements_res.json()['dataElements']
        for d in dhis2_data_elements:
            data_element_id = d["id"]
            if data_element_id not in ExportFormFields.dhis2_code_to_ids:
                data_element = get("{}dataElements/{}".format(dhis2_api_url, data_element_id),
                                   headers=dhis2_headers).json()
                ExportFormFields.dhis2_code_to_ids[data_element_id] = data_element.get('code')

        dhis2_codes_lookup = set(ExportFormFields.dhis2_code_to_ids.values())
        forms = requests.get("{}/export/forms".format(api_url), headers=headers).json()
        for form_name, field_names in forms.items():
            for field_name in field_names:
                if not field_name in dhis2_codes_lookup:
                    id = self.__update_data_elements(field_name)
                    ExportFormFields.dhis2_code_to_ids[id] = (field_name)

            rv = get("{}programs?filter=code:eq:{}".format(dhis2_api_url, form_name), headers=dhis2_headers)
            programs = rv.json().get('programs', [])
            program_payload = {
                'name': form_name,
                'shortName': form_name,
                'programType': 'WITHOUT_REGISTRATION'
            }
            if programs:
                # Update organisations
                program_id = programs[0]["id"]
                req = get("{}programs/{}".format(dhis2_api_url, program_id), headers=dhis2_headers)
                old_organisation_ids = req.json().get('organisationUnits', [])

                organisations = list(set(old_organisation_ids) | set(self.__get_all_operational_clinics()))
                program_payload["organisationUnits"] = [{"id": x} for x in organisations]
                payload_json = json.dumps(program_payload)
                # TODO: IDSchemes doesn't seem to work here
                req = put("{}programs{}?orgUnitIdScheme=CODE".format(dhis2_api_url, program_id), data=payload_json, headers=dhis2_headers)
                logger.info("Updated program %s with status %d", program_id, req.status_code)
            else:
                program_id = dhis2_ids.pop()
                old_organisation_ids = []

                organisations = list(set(old_organisation_ids) | set(self.__get_all_operational_clinics()))
                program_payload["organisationUnits"] = [{"id": x} for x in organisations]
                payload_json = json.dumps(program_payload)
                # TODO: IDSchemes doesn't seem to work here
                req = post("{}programs?orgUnitIdScheme=CODE".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
                logger.info("Updated program %s with status %d", program_id, req.status_code)
            # Update data elements
            data_element_keys = [{"dataElement": {"code": code}} for code in field_names]
            stage_payload = {
                "name": form_name,
                "code": form_name,
                "program": {
                    "code": form_name
                },
                "programStageDataElements": data_element_keys
            }
            json_stage_payload = json.dumps(stage_payload)
            res = post("{}programStages?orgUnitIdScheme=CODE&programIdScheme=CODE&dataElementIdScheme=CODE".format(api_url), data=json_stage_payload, headers=headers)
            logger.info("Created stage for program %s with status %d", form_name, res.status_code)

        print(str(datetime.now().time()) + " DONE!")

    @staticmethod
    def __get_all_operational_clinics():
        locations = requests.get("{}/locations".format(api_url), headers=headers).json()
        for location in locations.values():
            if location.get('case_report') != 0 and location.get('level') == 'clinic':
                yield location.get('country_location_id')

    @staticmethod
    def __update_data_elements(key):
        id = dhis2_ids.pop()
        json_payload = json.dumps({
            'id': id,
            'name': key,
            'shortName': key,
            'code': key,
            'domainType': 'TRACKER',
            'valueType': 'TEXT',
            'aggregationType': 'NONE'
        })
        post_res = post("{}dataElements".format(dhis2_api_url), data=json_payload, headers=dhis2_headers)
        json_res = post_res.json()
        logger.info("Created data element \"{}\"".format(key))
        return id


# class ExportProgram(Resource):
#
#     def get(self):
#         forms = requests.get("{}/export/forms".format(api_url), headers=headers).json()
#         for form_name, field_names in forms.items():

# example payload to be received from Meerkat Nest


upload_payload = {"token": "", "content": "record", "formId": "jor_evaluation", "formVersion": "",
                  "data": {"*meta-instance-id*": "uuid:32751c98-8390-4fa0-b0ed-1392d1ece3bc",
                           "*meta-model-version*": "null", "*meta-ui-version*": "null",
                           "*meta-submission-date*": "2017-09-06T13:56:02.777Z", "*meta-is-complete*": "true",
                           "*meta-date-marked-as-complete*": "2017-09-06T13:56:02.777Z",
                           "start": "2017-09-06T13:55:51.792Z", "end": "2017-09-06T13:55:59.703Z",
                           "today": "2017-09-06", "deviceid": "864422031325435", "subscriberid": "244121302512660",
                           "simid": "8935806150918576602", "phonenumber": "0449105968", "evaluation": "null",
                           "when": "other", "position": "data", "position_other": "null", "pre": "null",
                           "pre_yes": "null",
                           "pre_no": "null", "pre_yes_other": "null", "pre_no_other": "null", "pre_cd": "null",
                           "pre_clinical": "null", "pre_alerts": "null", "pre_online": "null", "pre_identify": "null",
                           "pre_report": "null", "pre_ncd": "null", "pre_ncd_yes": "null", "pre_ncd_other": "null",
                           "pre_time": "null", "post": "null", "post_yes": "null", "post_no": "null",
                           "post_yes_other": "null",
                           "post_no_other": "null", "post_cd": "null", "post_clinical": "null", "post_alerts": "null",
                           "post_online": "null", "post_identify": "null", "post_report": "null", "post_ncd": "null",
                           "post_ncd_yes": "null", "post_ncd_other": "null", "post_time": "null", "comments": "Bdke",
                           "instanceID": "uuid:32751c98-8390-4fa0-b0ed-1392d1ece3bc"},
                  "uuid": "uuid:32751c98-8390-4fa0-b0ed-1392d1ece3bc"}
messages = {'Messages': [
    {
        'MessageId': 'test-message-id-1',
        'ReceiptHandle': 'test-receipt-handle-1',
        'MD5OfBody': 'test-md5-1',
        'Body': json.dumps(upload_payload),
        'Attributes': {
            'test-attribute': 'test-attribute-value'
        }
    },
    {
        'MessageId': 'test-message-id-2',
        'ReceiptHandle': 'test-receipt-handle-2',
        'MD5OfBody': 'test-md5-2',
        'Body': json.dumps(upload_payload),
        'Attributes': {
            'test-attribute': 'test-attribute-value'
        }
    }
]}
