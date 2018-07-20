import json
from collections import defaultdict
from json import JSONDecodeError

import backoff as backoff
from datetime import datetime
import requests
from flask import Blueprint, jsonify
from flask_restful import abort, reqparse

from meerkat_consul import logger, api_url, app
from meerkat_consul.auth_client import auth
from meerkat_consul.authenticate import headers, refresh_auth_token
from meerkat_consul.decorators import get, post, put, async
from meerkat_consul.dhis2 import NewIdsProvider, transform_to_dhis2_code

__codes_to_ids = {}
dhis2_config = app.config['DHIS2_CONFIG']
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]

dhis2_ids = NewIdsProvider(dhis2_api_url, dhis2_headers)

COUNTRY_PARENT = 'ImspTQPwCqd'  # for testing with demo DHIS2 server, country should have no parent
COUNTRY_LOCATION_ID = app.config['COUNTRY_LOCATION_ID']

dhis2_export = Blueprint('export', __name__, url_prefix='/dhis2/export')

@dhis2_export.route('/hello')
@auth.authorise()
def hello():
    return jsonify({"message": "HELLO!"})

@dhis2_export.route('/locationTree', methods=['POST'])
@auth.authorise()
@refresh_auth_token
def locationTree():
    location_tree = requests.get("{}/locationtree".format(api_url), headers=headers)
    country = location_tree.json()
    country_details = get("{}/location/{!r}".format(api_url, COUNTRY_LOCATION_ID)).json()

    dhis2_organisation_code = country_details["country_location_id"]
    __url = "{}/organisationUnits?filter=code:eq:{}".format(dhis2_api_url, dhis2_organisation_code)
    dhis2_country_resp = get(__url, headers=dhis2_headers)
    dhis2_country_details = dhis2_country_resp.json().get("organisationUnits", [])
    if dhis2_country_details:
        __abort_if_more_than_one(dhis2_country_details, dhis2_organisation_code)
        dhis2_parent_id = dhis2_country_details[0]["id"]
    else:
        logger.error(msg="Error: Country location not found in DHIS2")
        abort(500, message="Error: Country location not found in DHIS2")
    child_locations = country["nodes"]
    __populate_child_locations(dhis2_parent_id, child_locations)

    return jsonify({"message": "Exporting location tree finished successfully"})


def __abort_if_more_than_one(dhis2_country_details, dhis2_organisation_code):
    if len(dhis2_country_details) > 1:
        logger.error("Received more than one organisation for given code: %s", dhis2_organisation_code)
        abort(500)


def __populate_child_locations(dhis2_parent_id, locations):
    for location in locations:
        loc_id = location["id"]
        location_details = requests.get("{}/location/{!r}".format(api_url, loc_id)).json()

        id = __create_new_dhis2_organisation(location_details, dhis2_parent_id)
        location_code = location_details["country_location_id"]
        # ExportLocationTree.__codes_to_dhis2_ids[location_code] = id

        child_locations = location["nodes"]
        __populate_child_locations(id, child_locations)


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

@dhis2_export.route("/formFields", methods=['POST'])
@auth.authorise()
@refresh_auth_token
def export_form_fields():
    forms = requests.get("{}/export/forms".format(api_url), headers=headers).json()
    logger.info(f"Forms: {forms}")

    form_config = {"new_som_case": "event", "new_som_register": "data_set"}

    for form_name, field_names in forms.items():
        if form_config.get(form_name) == "event":
            logger.info("Event form %s found", form_name)
            __update_dhis2_program(field_names, form_name)
        elif form_config.get(form_name) == "data_set":
            logger.info("Data set form %s found", form_name)
            __update_dhis2_dataset(field_names, form_name)

    return jsonify({"message": "Exporting form metadata finished successfully"})


def __update_dhis2_program(field_names, form_name):
    for field_name in field_names:
        if not Dhis2CodesToIdsCache.has_data_element_with_code(field_name):
            __update_data_elements(field_name)
    rv = get("{}/programs?filter=code:eq:{}".format(dhis2_api_url, form_name), headers=dhis2_headers)
    programs = rv.json().get('programs', [])
    program_payload = {
        'name': form_name,
        'shortName': form_name,
        'code': transform_to_dhis2_code(form_name),
        'programType': 'WITHOUT_REGISTRATION'
    }
    if programs:
        # Update organisations
        program_id = programs[0]["id"]
        program_payload["id"] = program_id
        req = get("{}/programs/{}".format(dhis2_api_url, program_id), headers=dhis2_headers)
        old_organisation_ids = [x["id"] for x in req.json().get('organisationUnits', [])]

        organisations = list(
            set(old_organisation_ids) | set(get_all_operational_clinics_as_dhis2_ids()))
        program_payload["organisationUnits"] = [{"id": x} for x in organisations]
        payload_json = json.dumps(program_payload)
        # TODO: IDSchemes doesn't seem to work here
        req = put("{}/programs/{}".format(dhis2_api_url, program_id), data=payload_json, headers=dhis2_headers)
        logger.info("Updated program %s (id:%s) with status %d", form_name, program_id, req.status_code)

    else:
        program_id = dhis2_ids.pop()
        program_payload["id"] = program_id
        old_organisation_ids = []

        organisations = list(
            set(old_organisation_ids) | set(get_all_operational_clinics_as_dhis2_ids()))
        program_payload["organisationUnits"] = [{"id": x} for x in organisations]
        payload_json = json.dumps(program_payload)
        # TODO: IDSchemes doesn't seem to work here
        req = post("{}/programs".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
        logger.info("Created program %s (id:%s) with status %d", form_name, program_id, req.status_code)
    # Update data elements
    data_element_keys = [{"dataElement": {"id": Dhis2CodesToIdsCache.get_data_element_id(f"TRACKER_{code}")}} for code in
                         field_names]
    # Update data elements
    stages = get("{}/programStages?filter=code:eq:{}".format(dhis2_api_url, form_name),
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
        res = put("{}/programStages/{}".format(dhis2_api_url, stage_id), data=json_stage_payload,
                  headers=dhis2_headers)
        logger.info("Updated stage for program %s with status %d", form_name, res.status_code)
    else:
        stage_id = dhis2_ids.pop()
        stage_payload["id"] = stage_id
        json_stage_payload = json.dumps(stage_payload)
        res = post("{}/programStages".format(dhis2_api_url), data=json_stage_payload, headers=dhis2_headers)
        logger.info("Created stage for program %s with status %d", form_name, res.status_code)


def __update_dhis2_dataset(field_names, form_name):

    rv = get("{}/dataSets?filter=code:eq:{}".format(dhis2_api_url, form_name), headers=dhis2_headers)
    datasets = rv.json().get('dataSets', [])
    dataset_payload = {
        'name': form_name,
        'shortName': form_name,
        'code': transform_to_dhis2_code(form_name),
        'periodType': "Daily"
    }
    if datasets:
        # Update organisations
        dataset_id = datasets[0]["id"]
        dataset_payload["id"] = dataset_id
        req = get("{}/dataSets/{}".format(dhis2_api_url, dataset_id), headers=dhis2_headers)
        old_organisation_ids = [x["id"] for x in req.json().get('organisationUnits', [])]

        for id in old_organisation_ids:
            logger.info("Found old organisationid %s: ", id)

        organisations = list(
            set(old_organisation_ids) | set(get_all_operational_clinics_as_dhis2_ids()))
        dataset_payload["organisationUnits"] = [{"id": x} for x in organisations]
        payload_json = json.dumps(dataset_payload)
        # TODO: IDSchemes doesn't seem to work here
        req = put("{}/dataSets/{}".format(dhis2_api_url, dataset_id), data=payload_json, headers=dhis2_headers)
        logger.info("Updated data set %s (id:%s) with status %d", form_name, dataset_id, req.status_code)

    else:
        dataset_id = dhis2_ids.pop()
        dataset_payload["id"] = dataset_id
        old_organisation_ids = []

        organisations = list(
            set(old_organisation_ids) | set(get_all_operational_clinics_as_dhis2_ids()))
        dataset_payload["organisationUnits"] = [{"id": x} for x in organisations]
        payload_json = json.dumps(dataset_payload)
        # TODO: IDSchemes doesn't seem to work here
        req = post("{}/dataSets".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
        logger.info("Created data set %s (id:%s) with status %d", form_name, dataset_id, req.status_code)

    # Create data elements
    for field_name in field_names:
        if not Dhis2CodesToIdsCache.has_data_element_with_code(field_name, "AGGREGATE"):
            __update_data_elements(field_name, "AGGREGATE")

    # Connect data elements to data set
    data_element_keys = [{"dataElement": {"id": Dhis2CodesToIdsCache.get_data_element_id(f"AGGREGATE_{name}")}} for name in
                         field_names]

    logger.info("Found %d relevant data elements", len(data_element_keys))

    dataset_payload['dataSetElements'] = data_element_keys
    payload_json = json.dumps(dataset_payload)

    res = put("{}/dataSets/{}".format(dhis2_api_url, dataset_id), data=payload_json,
              headers=dhis2_headers)
    logger.info("Updated data elements for data set %s (id: %s) with status %d", form_name, dataset_id, res.status_code)


def get_all_operational_clinics_as_dhis2_ids():
    locations = requests.get("{}/locations".format(api_url), headers=headers).json()
    for location in locations.values():
        if location.get('case_report') != 0 and location.get('level') == 'clinic' and location.get('country_location_id'):
            yield Dhis2CodesToIdsCache.get_organisation_id(location.get('country_location_id'))


def __update_data_elements(key, domain_type="TRACKER"):
    id = dhis2_ids.pop()

    name_ = f"HOQM {key}"
    json_payload = {
        'id': id,
        'code': transform_to_dhis2_code(f"{domain_type}_{key}"),
        'domainType': domain_type,
        'valueType': 'TEXT'
    }

    short_name_postfix = key[-47:]
    if domain_type == "AGGREGATE":
        json_payload['aggregationType'] = "NONE"
        json_payload['categoryCombo'] = {"id": Dhis2CodesToIdsCache.get_category_combination_id('default')}
        json_payload['name'] = f"{name_} Daily Registry"
        json_payload['shortName'] = f"DR_{short_name_postfix}"
    elif domain_type == 'TRACKER':
        json_payload['aggregationType'] = "NONE"
        json_payload['name'] = f"{name_} Case Form"
        json_payload['shortName'] = f"CF_{short_name_postfix}"

    json_payload_flat = json.dumps(json_payload)

    post_res = post("{}/dataElements".format(dhis2_api_url), data=json_payload_flat, headers=dhis2_headers)
    if post_res.status_code >= 300:
        abort(500, message=f"Unable to create data element {key} - {domain_type}")
    logger.info("Created data element \"{}\" with status {!r}".format(key, post_res.status_code))
    return id


def meerkat_to_dhis2_date_format(meerkat_date):
    return datetime.strptime(meerkat_date, "%b %d, %Y %H:%M:%S %p").strftime("%Y-%m-%d")


@dhis2_export.route("/events", methods=['POST'])
@auth.authorise()
def events():
    logger.debug("Starting event export.")
    event_payload_array = []
    try:
        json_request = json.loads(reqparse.request.get_json())
    except JSONDecodeError:
        abort(400, messages="Unable to parse posted JSON")
    for message in json_request['Messages']:
        case = message['Body']
        case_data = case['data']
        program = case['formId']
        date = meerkat_to_dhis2_date_format(case_data['SubmissionDate'])
        _uuid = case['data'].get('meta/instanceID')[-11:]
        event_id = uuid_to_dhis2_uid(_uuid)
        data_values = [{'dataElement': Dhis2CodesToIdsCache.get_data_element_id(f"TRACKER_{i}"), 'value': v} for i, v in
                       case['data'].items()]
        country_location_id = MeerkatCache.get_location_from_deviceid(case_data['deviceid'])
        event_payload = {
            'event': event_id,
            'program': Dhis2CodesToIdsCache.get_program_id(program),
            'orgUnit': Dhis2CodesToIdsCache.get_organisation_id(country_location_id),
            'eventDate': date,
            'completedDate': date,
            'dataValues': data_values,
            'status': 'COMPLETED'
        }
        event_payload_array.append(event_payload)
    events_payload = {"events": event_payload_array}
    post_events(events_payload)
    return jsonify({"message": "Sending event batch finished successfully"}), 202


@dhis2_export.route("/data_set", methods=['POST'])
@auth.authorise()
def data_set():
    logger.debug("Starting data set export")
    data_set_payload_array = []
    try:
        json_request = json.loads(reqparse.request.get_json())
    except JSONDecodeError:
        abort(400, messages="Unable to parse posted JSON")
    for message in json_request['Messages']:
        data_entry = message['Body']
        data_entry_content = data_entry['data']
        data_set_code = data_entry['formId']
        date = meerkat_to_dhis2_date_format(data_entry_content['SubmissionDate'])
        data_values = [{'dataElement': Dhis2CodesToIdsCache.get_data_element_id(f"AGGREGATE_{i}"), 'value': v} for i, v in
                       data_entry['data'].items()]
        country_location_id = MeerkatCache.get_location_from_deviceid(data_entry_content['deviceid'])
        data_set_payload = {
            'dataSet': Dhis2CodesToIdsCache.get_data_set_id(data_set_code),
            'completeDate': date,
            'period': get_period_from_date(date, data_entry['formId']),
            'orgUnit': Dhis2CodesToIdsCache.get_organisation_id(country_location_id),
            'attributeOptionCombo': "aoc_id",
            'dataValues': data_values
        }
        data_set_payload_array.append(data_set_payload)
    data_sets_payload = {"data_entries": data_set_payload_array}
    post_data_set(data_sets_payload)
    return jsonify({"message": "Sending data entry batch finished successfully"}), 202


@async
def post_events(events_payload):
    event_res = post("{}/events?importStrategy=CREATE_AND_UPDATE".format(dhis2_api_url), headers=dhis2_headers,
                     data=json.dumps(events_payload))
    logger.info("Send batch of events with status: %d", event_res.status_code)
    logger.debug(event_res.json().get('message'))


@async
def post_data_set(data_sets_payload):
    data_set_res = post("{}/dataValueSets?importStrategy=CREATE_AND_UPDATE".format(dhis2_api_url),
                        headers=dhis2_headers, data=json.dumps(data_sets_payload))
    logger.info("Send batch of data entries with status: %d", data_set_res.status_code)
    logger.debug(data_set_res.json().get('message'))


def uuid_to_dhis2_uid(uuid):
    result = uuid[-11:]
    # DHIS2 uid needs to start with a character
    if result[0].isdigit():
        result = 'X' + result[1:]
    return result


def get_period_from_date(input_date, formId):
    period = dhis2_config.get('data_set_peroid', {}).get(formId, 'daily')

    if period == 'daily':
        ret = str(input_date.year) + str(input_date.month) + str(input_date.day)
        return ret
    else:
        return


class MeerkatCache():
    caches = defaultdict(dict)

    @staticmethod
    def get_location_from_deviceid(deviceid):
        return MeerkatCache.get_and_cache_value('device', deviceid)

    @staticmethod
    @backoff.on_exception(backoff.expo, JSONDecodeError, max_tries=3, max_value=1)
    def get_and_cache_value(resource_name, deviceid):
        cache = MeerkatCache.caches[resource_name]
        if not cache.get(deviceid):
            url = "{}/{}/{}".format(api_url, resource_name, deviceid)
            req = requests.get(url)
            country_location_id = req.json().get('country_location_id')
            cache[deviceid] = country_location_id
        return cache.get(deviceid)


class Dhis2CodesToIdsCache():
    caches = defaultdict(dict)

    @staticmethod
    def get_organisation_id(organisation_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('organisationUnits', organisation_code)

    @staticmethod
    def get_data_element_id(data_element_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('dataElements', transform_to_dhis2_code(data_element_code))

    @staticmethod
    def get_program_id(program_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('programs', transform_to_dhis2_code(program_code))

    @staticmethod
    def get_data_set_id(data_set_code):
        return Dhis2CodesToIdsCache.get_and_cache_value('dataSets', transform_to_dhis2_code(f"TRACKER_{data_set_code}"))

    @staticmethod
    def get_category_combination_id(category_combination):
        return Dhis2CodesToIdsCache.get_and_cache_value('categoryCombos', category_combination)

    @staticmethod
    def has_data_element_with_code(dhis2_code_suffix, domain_type="TRACKER"):
        try:
            dhis2_code = f"{domain_type}_{dhis2_code_suffix}"
            Dhis2CodesToIdsCache.get_and_cache_value('dataElements', dhis2_code)
        except ValueError:
            return False
        return True

    @staticmethod
    def get_and_cache_value(dhis2_resource, dhis2_code):
        cache = Dhis2CodesToIdsCache.caches[dhis2_resource]
        if not cache.get(dhis2_code):
            logger.info("{} with code {} not found in cache.".format(dhis2_resource, dhis2_code))
            rv = get("{url}/{resource_path}?filter=code:eq:{code}".format(
                url=dhis2_api_url,
                resource_path=dhis2_resource,
                code=dhis2_code),
                headers=dhis2_headers)
            dhis2_objects = rv.json().get(dhis2_resource)
            if not dhis2_objects or len(dhis2_objects) == 0:
                raise ValueError("{} with code {} not found in DHIS2".format(dhis2_resource, dhis2_code))
            elif len(dhis2_objects) != 1:
                logger.error("Found more then one dhis2 {} for code: {}".format(dhis2_resource, dhis2_code))
            cache[dhis2_code] = dhis2_objects[0]["id"]
        return cache.get(dhis2_code)
