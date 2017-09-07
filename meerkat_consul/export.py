import json

from datetime import datetime
import requests

from meerkat_consul import app, dhis2_config, logger, api_url
from meerkat_consul.config import COUNTRY_LOCATION_ID, headers
from meerkat_consul.decorators import get, post
from meerkat_consul.dhis2 import NewIdsProvider

__codes_to_ids = {}
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]

dhis2_ids = NewIdsProvider(dhis2_config["url"], dhis2_headers)


@app.route('/locations')
def locations():
    result = ""
    location_tree = requests.get(api_url + "/locationtree", headers=headers)
    country = location_tree.json()
    country_details = get("{}/location/{!r}".format(api_url, COUNTRY_LOCATION_ID)).json()
    if country_details["country_location_id"]:
        dhis2_country_resp = get("{}/organisationUnits?filter=code:eq:{}".format(dhis2_api_url, country_details["country_location_id"])).json()
        dhis2_country_details = dhis2_country_resp.json().get("organisationUnits", [])
        dhis2_parent_id = dhis2_country_details ["id"]
    else:
        # TODO: create country
        dhis2_parent_id = dhis2_ids.pop()
        pass

    # TODO: check if county is there
    locations = country["nodes"]
    for location in locations:
        loc_id = location["id"]
        location_details = requests.get("{}/location/{!r}".format(api_url, loc_id)).json()

        country_location_id = location_details["country_location_id"]
        name = location_details["name"]
        if location_details["start_date"]:
            # TODO: check this string > datetime > string formatting
            opening_date = datetime.strptime(location_details["start_date"], "%Y-%m-%d %m:%s:%n").strftime("%Y-%m-%d")
        else:
            opening_date = "1970-01-01"
        json_dict = {
            "id": dhis2_ids.pop(),
            "name": name,
            "shortName": name,
            "code": country_location_id,
            "openingDate": opening_date,
            "parent": {"id": dhis2_parent_id}
        }

        print(json.dumps(location_details.json(), indent=4))
        result += "{!r}: {!r}".format(location.get("id", "no_id"), location.get("text", "no_text"))
        result += "\n"

    return result


def create_dhis2_organisation(_location):
    """
    Creates a dhis2 organisation.
    :param _location: Meerkat location to be published as dhis2_organisation
    :return: void
    """
    organisation_code = _location["country_location_id"]
    if organisation_code is None:
        return
    if _location.start_date:
        opening_date = _location.start_date.strftime("%Y-%m-%d")
    else:
        opening_date = "1970-01-01"
    json_res = get("{}organisationUnits?filter=code:eq:{}".format(dhis2_api_url, organisation_code),
                   header=dhis2_headers).json()
    if not json_res['organisationUnits']:
        __create_new_organisation(_location, opening_date, organisation_code)
    else:
        logger.info("Organisation %12s with code %15s already exists", _location.name, organisation_code)
        uid = json_res['organisationUnits'][0]['id']
        __codes_to_ids[organisation_code] = uid

def __create_new_organisation(_location, opening_date, organisation_code):
    uid = dhis2_ids.pop()
    name = _location.name
    parent_location_id = _location.parent_location
    if parent_location_id == 1:
        parent_id = dhis2_config['countryId']
    else:
        parent_id = __codes_to_ids[locations[parent_location_id].country_location_id]
    json_dict = {
        "id": uid,
        "name": name,
        "shortName": name,
        "code": organisation_code,
        "openingDate": opening_date,
        "parent": {"id": parent_id}
    }
    payload = json.dumps(json_dict)
    response = post("{}organisationUnits".format(dhis2_api_url), headers=dhis2_headers, data=payload)
    logger.info("Created location %s with response %d", name, response.status_code)
    logger.info(response.text)
    __codes_to_ids[organisation_code] = uid

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
