import json
import logging
from collections import defaultdict
from json import JSONDecodeError

import backoff as backoff
from datetime import datetime
import requests
from flask import Blueprint, jsonify
from flask_restful import abort, reqparse

from meerkat_consul import logger, api_url, app
from meerkat_consul.auth_client import auth
from meerkat_consul.authenticate import meerkat_headers
from meerkat_consul.decorators import get, post, put, async
from meerkat_consul.dhis2 import NewIdsProvider, transform_to_dhis2_code
from meerkat_consul.errors import MissingCountryLocationIdError

__codes_to_ids = {}
dhis2_config = app.config['DHIS2_CONFIG']
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]

dhis2_ids = NewIdsProvider(dhis2_api_url, dhis2_headers)

form_export_config = app.config['FORM_EXPORT_CONFIG']

dhis2_export = Blueprint('export', __name__, url_prefix='/dhis2/export')

@dhis2_export.route('/hello')
@auth.authorise()
def hello():
    return jsonify({"message": "HELLO!"})


def __abort_if_more_than_one(dhis2_country_details, dhis2_organisation_code):
    if len(dhis2_country_details) > 1:
        logger.error("Received more than one organisation for given code: %s", dhis2_organisation_code)
        abort(500)


def export_form_fields():
    form_configs = app.config['FORM_DEFINITIONS'] or __get_forms_from_meerkat_api()
    logger.info("Starting export of form metadata.")
    for form_name, export_config in form_export_config.items():
        form_config = form_configs.get(form_name)
        if not form_config:
            raise ValueError(f"Can't find fields for form {form_name}")
        export_type = export_config["exportType"]
        if export_type == "event":
            logger.debug("Event form %s found", form_name)
            __update_dhis2_program(form_config, form_name)
        elif export_type == "data_set":
            logger.debug("Data set form %s found", form_name)
            __update_dhis2_dataset(form_config, form_name)
        else:
            msg_ = f"Unsupported exportType {export_type} for {form_name}"
            raise ValueError(msg_)
    logger.info("Finished export of form metadata.")


def __get_forms_from_meerkat_api():
    return requests.get("{}/export/forms".format(api_url), headers=meerkat_headers()).json()


def __update_dhis2_program(form_config, form_name):
    _create_data_elements(form_config, data_elements_type="TRACKER")
    dhis2_code = transform_to_dhis2_code(form_name)
    rv = get("{}/programs?filter=code:eq:{}".format(dhis2_api_url, dhis2_code), headers=dhis2_headers)
    programs = rv.json().get('programs', [])
    display_name = form_export_config[form_name].get("exportName", form_name)
    program_payload = {
        'name': display_name,
        'shortName': display_name,
        'code': dhis2_code,
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
        req = put("{}/programs/{}".format(dhis2_api_url, program_id), data=payload_json, headers=dhis2_headers)
        logger.info("Updated program %s (id:%s) with status %d", dhis2_code, program_id, req.status_code)

    else:
        program_id = dhis2_ids.pop()
        program_payload["id"] = program_id
        old_organisation_ids = []

        organisations = list(
            set(old_organisation_ids) | set(get_all_operational_clinics_as_dhis2_ids()))
        program_payload["organisationUnits"] = [{"id": x} for x in organisations]
        payload_json = json.dumps(program_payload)
        req = post("{}/programs".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
        logger.info("Created program %s (id:%s) with status %d", dhis2_code, program_id, req.status_code)
    # Update data elements
    data_element_keys = [{"dataElement": {"id": Dhis2CodesToIdsCache.get_data_element_id(f"TRACKER_{field_config['name']}")}} for field_config in
                         form_config]
    stages = get("{}/programStages?filter=code:eq:{}".format(dhis2_api_url, dhis2_code),
                 headers=dhis2_headers).json()
    stage_payload = {
        "name": display_name,
        "code": dhis2_code,
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
        logger.info("Updated stage for program %s with status %d", dhis2_code, res.status_code)
    else:
        stage_id = dhis2_ids.pop()
        stage_payload["id"] = stage_id
        json_stage_payload = json.dumps(stage_payload)
        res = post("{}/programStages".format(dhis2_api_url), data=json_stage_payload, headers=dhis2_headers)
        logger.info("Created stage for program %s with status %d", dhis2_code, res.status_code)


def __update_dhis2_dataset(form_config, form_name):
    dhis2_code = transform_to_dhis2_code(form_name)
    rv = get("{}/dataSets?filter=code:eq:{}".format(dhis2_api_url, dhis2_code), headers=dhis2_headers)
    datasets = rv.json().get('dataSets', [])
    display_name = form_export_config[form_name].get("exportName", form_name)
    dataset_payload = {
        'name': display_name,
        'shortName': display_name,
        'code': dhis2_code,
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
        req = post("{}/dataSets".format(dhis2_api_url), data=payload_json, headers=dhis2_headers)
        logger.info("Created data set %s (id:%s) with status %d", form_name, dataset_id, req.status_code)

    _create_data_elements(form_config, data_elements_type="AGGREGATE")

    # Connect data elements to data set
    data_element_keys = [{"dataElement": {"id": Dhis2CodesToIdsCache.get_data_element_id(f"AGGREGATE_{field_config['name']}")}} for field_config in
                         form_config]

    logger.info("Found %d relevant data elements", len(data_element_keys))

    dataset_payload['dataSetElements'] = data_element_keys
    payload_json = json.dumps(dataset_payload)

    res = put("{}/dataSets/{}".format(dhis2_api_url, dataset_id), data=payload_json,
              headers=dhis2_headers)
    logger.info("Updated data elements for data set %s (id: %s) with status %d", form_name, dataset_id, res.status_code)


def _create_data_elements(form_config, data_elements_type):
    for field_config in form_config:
        field_name = field_config['name']
        field_type = field_config['type']
        if not Dhis2CodesToIdsCache.has_data_element_with_code(field_name, data_elements_type):
            __update_data_elements(field_name, field_type, data_elements_type)

@backoff.on_exception(backoff.expo, json.decoder.JSONDecodeError, max_tries=5, max_value=45, base=5)
def get_all_operational_clinics_as_dhis2_ids():
    locations = requests.get("{}/locations".format(api_url), headers=meerkat_headers()).json()
    for location in locations.values():
        if location.get('case_report') != 0 and location.get('level') == 'clinic' and location.get('country_location_id'):
            yield Dhis2CodesToIdsCache.get_organisation_id(location.get('country_location_id'))


def __update_data_elements(key, field_type='TEXY', domain_type="TRACKER"):
    id = dhis2_ids.pop()

    name_ = f"HOQM {key}"
    json_payload = {
        'id': id,
        'code': transform_to_dhis2_code(f"{domain_type}_{key}"),
        'domainType': domain_type,
        'valueType': field_type
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


def meerkat_to_dhis2_period_date_format(meerkat_date, form_name):
    period = dhis2_config.get('data_set_period', {}).get(form_name, 'daily')

    if period == 'daily':
        return datetime.strptime(meerkat_date, "%b %d, %Y %H:%M:%S %p").strftime("%Y%m%d")
    else:
        return None


@dhis2_export.route("/submissions", methods=['POST'])
@auth.authorise()
def submissions():
    logger.debug("Starting event export.")
    payload_array = []
    try:
        json_request = json.loads(reqparse.request.get_json())
    except JSONDecodeError:
        logger.error("Failed to decode JSON body")
        abort(400, messages="Unable to parse posted JSON")
    form_name = json_request["formId"]
    if form_name not in form_export_config:
        msg = f"Form {form_name} is not supported."
        logger.warning(msg)
        return jsonify({"message": msg}), 404
    export_type = form_export_config[form_name].get("exportType")
    if export_type == "event":
        for message in json_request['Messages']:
            try:
                case = message['Body']
                case_data = case['data']
                program = case['formId']
                _uuid = case_data.get('meta/instanceID', 'Unknown')[-11:]
            except (TypeError, KeyError, AttributeError):
                logger.error("Failed to parse message for form %s", form_name)
                logger.debug("Message: %s", message)
                logger.exception("Exception details:")
                continue
            try:
                date = meerkat_to_dhis2_date_format(case_data['SubmissionDate'])
                event_id = uuid_to_dhis2_uid(_uuid)
                data_values = [{'dataElement': Dhis2CodesToIdsCache.get_data_element_id(f"TRACKER_{i}"), 'value': v} for i, v in
                               case['data'].items()]
                country_location_id = MeerkatCache.get_location_from_deviceid(case_data['deviceid'])
            except MissingCountryLocationIdError as e:
                logger.error(e, exc_info=logger.getEffectiveLevel() == logging.DEBUG)
                continue
            except (TypeError, ValueError):
                logger.warning("Failed to prepare data elements for uuid: %s in form %s", _uuid, form_name)
                logger.exception("Exception details:")
                continue
            event_payload = {
                'event': event_id,
                'program': Dhis2CodesToIdsCache.get_program_id(program),
                'orgUnit': Dhis2CodesToIdsCache.get_organisation_id(country_location_id),
                'eventDate': date,
                'completedDate': date,
                'dataValues': data_values,
                'status': 'COMPLETED'
            }
            payload_array.append(event_payload)
        events_payload = {"events": payload_array}
        post_events(events_payload)
    elif export_type == "data_set":
        for message in json_request['Messages']:
            try:
                data_entry = message['Body']
                data_entry_content = data_entry['data']
                form_name = data_entry['formId']
                _uuid = data_entry_content.get('meta/instanceID')[-11:]
            except (TypeError, KeyError, AttributeError):
                logger.error("Failed to parse message for form %s", form_name)
                logger.debug("Message: %s", message)
                logger.exception("Exception details:")
                continue
            try:
                country_location_id = MeerkatCache.get_location_from_deviceid(data_entry_content['deviceid'])
                data_values = [{'dataElement': Dhis2CodesToIdsCache.get_data_element_id(f"AGGREGATE_{i}"), 'value': v} for i, v in
                               data_entry_content.items()]
                date = meerkat_to_dhis2_date_format(data_entry_content['SubmissionDate'])
                period = meerkat_to_dhis2_period_date_format(data_entry_content['SubmissionDate'], form_name)
                data_set_id = Dhis2CodesToIdsCache.get_data_set_id(form_name)
                organisation_id = Dhis2CodesToIdsCache.get_organisation_id(country_location_id)
            except MissingCountryLocationIdError as e:
                logger.warning(e, exc_info=logger.getEffectiveLevel() == logging.DEBUG)
                continue
            except (ValueError, TypeError):
                logger.error("Failed to prepare data elements for uuid: %s in form %s", _uuid, form_name)
                logger.exception("Exception details:")
                continue
            data_set_payload = {
                'dataSet': data_set_id,
                'completeDate': date,
                'period': period,
                'orgUnit': organisation_id,
                'dataValues': data_values
            }
            payload_array.append(data_set_payload)
        data_sets_payload = {"data_entries": payload_array}
        post_data_set(data_sets_payload)
    else:
        msg = f"Export for form {form_name} with type {export_type} nod defined."
        logger.error(msg)
        return jsonify({"message": msg}), 404
    return jsonify({"message": "Sending submission batch finished successfully"}), 202


@async
def post_events(events_payload):
    event_res = post("{}/events?importStrategy=CREATE_AND_UPDATE".format(dhis2_api_url), headers=dhis2_headers,
                     data=json.dumps(events_payload))
    logger.info("Send batch of events with status: %d", event_res.status_code)
    logger.debug("Message: %s", event_res.json().get('message'))


@async
def post_data_set(data_sets_payload):
    for data_set in data_sets_payload['data_entries']:
        data_set_res = post("{}/dataValueSets?importStrategy=CREATE_AND_UPDATE".format(dhis2_api_url),
                            headers=dhis2_headers, data=json.dumps(data_set))
        logger.info("Send batch of data entries with status: %d", data_set_res.status_code)
        msg_ = data_set_res.json().get('message')
        if msg_:
            logger.debug(f"With message: {msg_}")


def uuid_to_dhis2_uid(uuid):
    result = uuid[-11:]
    # DHIS2 uid needs to start with a character
    if result[0].isdigit():
        result = 'X' + result[1:]
    return result


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
            if not country_location_id:
                raise MissingCountryLocationIdError(f"Failed to get country location id for device: {deviceid}")
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
        return Dhis2CodesToIdsCache.get_and_cache_value('dataSets', transform_to_dhis2_code(data_set_code))

    @staticmethod
    def get_category_combination_id(category_combination):
        return Dhis2CodesToIdsCache.get_and_cache_value('categoryCombos', category_combination)

    @staticmethod
    def has_data_element_with_code(dhis2_code_suffix, domain_type="TRACKER"):
        try:
            dhis2_code = f"{domain_type}_{dhis2_code_suffix}"
            Dhis2CodesToIdsCache.get_and_cache_value('dataElements', transform_to_dhis2_code(dhis2_code))
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
