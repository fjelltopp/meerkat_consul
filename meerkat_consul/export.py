import json
from collections import defaultdict
from json import JSONDecodeError

from datetime import datetime
import requests
from flask_restful import abort, Resource, reqparse

from meerkat_consul import dhis2_config, logger, api_url
from meerkat_consul.config import COUNTRY_LOCATION_ID, headers
from meerkat_consul.decorators import get, post, put
from meerkat_consul.dhis2 import NewIdsProvider

__codes_to_ids = {}
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]

dhis2_ids = NewIdsProvider(dhis2_api_url, dhis2_headers)

COUNTRY_PARENT = 'ImspTQPwCqd'  # for testing with demo DHIS2 server, country should have no parent


class ExportLocationTree(Resource):
    def post(self):
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
        # ExportLocationTree.__codes_to_dhis2_ids[dhis2_organisation_code] = dhis2_parent_id
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
            # ExportLocationTree.__codes_to_dhis2_ids[location_code] = id

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

    def post(self):
        forms = requests.get("{}/export/forms".format(api_url), headers=headers).json()
        for form_name, field_names in forms.items():
            for field_name in field_names:
                if not Dhis2CodesToIdsCache.has_data_element_with_code(field_name):
                    self.__update_data_elements(field_name)

            rv = get("{}programs?filter=code:eq:{}".format(dhis2_api_url, form_name), headers=dhis2_headers)
            programs = rv.json().get('programs', [])
            program_payload = {
                'name': form_name,
                'shortName': form_name,
                'code': form_name,
                'programType': 'WITHOUT_REGISTRATION'
            }
            if programs:
                # Update organisations
                program_id = programs[0]["id"]
                program_payload["id"] = program_id
                req = get("{}programs/{}".format(dhis2_api_url, program_id), headers=dhis2_headers)
                old_organisation_ids = [x["id"] for x in req.json().get('organisationUnits', [])]

                organisations = list(
                    set(old_organisation_ids) | set(ExportFormFields.get_all_operational_clinics_as_dhis2_ids()))
                program_payload["organisationUnits"] = [{"id": x} for x in organisations]
                payload_json = json.dumps(program_payload)
                # TODO: IDSchemes doesn't seem to work here
                req = put("{}programs/{}".format(dhis2_api_url, program_id), data=payload_json, headers=dhis2_headers)
                logger.info("Updated program %s (id:%s) with status %d", form_name, program_id, req.status_code)

            else:
                program_id = dhis2_ids.pop()
                program_payload["id"] = program_id
                old_organisation_ids = []

                organisations = list(
                    set(old_organisation_ids) | set(ExportFormFields.get_all_operational_clinics_as_dhis2_ids()))
                program_payload["organisationUnits"] = [{"id": x} for x in organisations]
                payload_json = json.dumps(program_payload)
                # TODO: IDSchemes doesn't seem to work here
                req = post("{}programs".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
                logger.info("Created program %s (id:%s) with status %d", form_name, program_id, req.status_code)
            # Update data elements
            data_element_keys = [{"dataElement": {"id": Dhis2CodesToIdsCache.get_data_element_id(code)}} for code in
                                 field_names]
            # Update data elements
            stages = get("{}programStages?filter=code:eq:{}".format(dhis2_api_url, form_name),
                         headers=dhis2_headers).json()
            stage_payload = {
                "name": form_name,
                "code": form_name,
                "program": {
                    "id": program_id
                },
                "programStageDataElements": data_element_keys
            }
            if stages.get("programStages"):
                stage_id = stages.get("programStages")[0]["id"]
                json_stage_payload = json.dumps(stage_payload)
                res = put("{}programStages/{}".format(dhis2_api_url, stage_id), data=json_stage_payload,
                          headers=dhis2_headers)
                logger.info("Updated stage for program %s with status %d", form_name, res.status_code)
            else:
                stage_id = dhis2_ids.pop()
                stage_payload["id"] = stage_id
                json_stage_payload = json.dumps(stage_payload)
                res = post("{}programStages".format(dhis2_api_url), data=json_stage_payload, headers=dhis2_headers)
                logger.info("Created stage for program %s with status %d", form_name, res.status_code)


    @staticmethod
    def get_all_operational_clinics_as_dhis2_ids():
        locations = requests.get("{}/locations".format(api_url), headers=headers).json()
        for location in locations.values():
            if location.get('case_report') != 0 and location.get('level') == 'clinic':
                yield Dhis2CodesToIdsCache.get_organisation_id(location.get('country_location_id'))

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
        logger.info("Created data element \"{}\" with status {!r}".format(key, post_res.status_code))
        return id


# example payload to be received from Meerkat Nest

upload_payload = {'token': '', 'content': 'record', 'formId': 'demo_register', 'formVersion': '',
                  'data': {
                      'end': '2017-08-31T00:00:00',
                      'index': '48',
                      'start': '2017-08-24T00:00:00',
                      'deviceid': 'random',
                      'intro./module': 'two',
                      'SubmissionDate': 'values',
                      'meta/instanceID': 'random',
                      'surveillance./afp': 'bar',
                      'surveillance./measles': 'two',
                      'consult./consultations': 'random',
                      'consult./ncd_consultations': 'matters',
                      'consult./consultations_refugee': 'bar',
                      'clinic': '5',
                      'district': '5',
                      'region': '2'
                  },
                  'uuid': '8cc2e81a-988b-11e7-8b9b-507b9dab1486'
                  }
messages = {'Messages': [
    {
        'MessageId': 'test-message-id-1',
        'ReceiptHandle': 'test-receipt-handle-1',
        'MD5OfBody': 'test-md5-1',
        'Body': upload_payload,
        'Attributes': {
            'test-attribute': 'test-attribute-value'
        }
    },
    {
        'MessageId': 'test-message-id-2',
        'ReceiptHandle': 'test-receipt-handle-2',
        'MD5OfBody': 'test-md5-2',
        'Body': upload_payload,
        'Attributes': {
            'test-attribute': 'test-attribute-value'
        }
    }
]}


class ExportEvent(Resource):
    def post(self):
        event_payload_array = []
        try:
            json_request = json.loads(reqparse.request.get_json())
        except JSONDecodeError:
            abort(400, messages="Unable to parse posted JSON")
        for message in json_request['Messages']:
            case = message['Body']
            program = case['formId']
            uuid = case['data'].get('meta/instanceID')[-11:]
            event_id = uuid_to_dhis2_uid(uuid)
            data_values = [{'dataElement': Dhis2CodesToIdsCache.get_data_element_id(i), 'value': v} for i, v in
                           case['data'].items()]
            logger.info("Creating event with id %s", event_id)
            event_payload = {
                'event': event_id,
                'program': Dhis2CodesToIdsCache.get_program_id(program),
                'orgUnit': Dhis2CodesToIdsCache.get_organisation_id('unique_code_1'),
                'eventDate': '1970-01-01',
                'completedDate': '2017-09-13',
                'dataValues': data_values,
                'status': 'COMPLETED'
            }
            event_payload_array.append(event_payload)
        events_payload = {"events": event_payload_array}
        event_res = post("{}events?importStrategy=CREATE_AND_UPDATE".format(dhis2_api_url), headers=dhis2_headers, data=json.dumps(events_payload))
        logger.info("Send batch of events with status: %d", event_res.status_code)
        logger.info(event_res.json().get('message'))


def uuid_to_dhis2_uid(uuid):
    result = uuid[-11:]
    # DHIS2 uid needs to start with a character
    if result[0].isdigit():
        result = 'X' + result[1:]
    return result


class Dhis2CodesToIdsCache():
    caches = defaultdict(dict)

    @staticmethod
    def get_organisation_id(organisation_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('organisationUnits', organisation_code)

    @staticmethod
    def get_data_element_id(data_element_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('dataElements', data_element_code)

    @staticmethod
    def get_program_id(program_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('programs', program_code)

    @staticmethod
    def get_and_cache_value(dhis2_resource, dhis2_code):
        cache = Dhis2CodesToIdsCache.caches[dhis2_resource]
        if not cache.get(dhis2_code):
            rv = get("{url}/{resource_path}?filter=code:eq:{code}".format(
                url=dhis2_api_url,
                resource_path=dhis2_resource,
                code=dhis2_code),
                headers=dhis2_headers)
            dhis2_objects = rv.json().get(dhis2_resource)
            if len(dhis2_objects) == 0:
                raise ValueError("{} with code {} not found".format(dhis2_resource, dhis2_code))
            elif len(dhis2_objects) != 1:
                logger.error("Found more then one dhis2 {} for code: {}".format(dhis2_resource, dhis2_code))
            cache[dhis2_code] = dhis2_objects[0]["id"]
        return cache.get(dhis2_code)

    @staticmethod
    def has_data_element_with_code(dhis2_code):
        try:
            Dhis2CodesToIdsCache.get_and_cache_value('dataElements', dhis2_code)
        except ValueError:
            return False
        return True

