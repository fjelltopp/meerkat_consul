import requests
import json
from pprint import pprint
import datetime
from meerkat_consul import logger, app

dhis2_config = app.config['DHIS2_CONFIG']
dhis2_api_url = dhis2_config["url"] + dhis2_config["apiResource"]
dhis2_headers = dhis2_config["headers"]


def get_pages(url, data_object):
    ret = requests.get(url, headers=dhis2_headers)
    ret_dict = json.loads(ret.text)
    collated = []
    if 'pager' in ret_dict.keys():
        while int(ret_dict['pager']['page']) < int(ret_dict['pager']['pageCount']):
            collated = collated + ret_dict[data_object]
            next_page = int(ret_dict['pager']['page']) + 1
            ret = requests.get(url + '&page=' + str(next_page), headers=dhis2_headers)
            ret_dict = json.loads(ret.text)

        collated = collated + ret_dict[data_object]
        return collated
    else:
        return ret_dict


def delete_data_sets(prefix):

    # get the data set Ids
    get_data_set_url = '{}/dataSets?filter=displayName:like:{}'.format(dhis2_api_url, prefix)

    r = requests.get(get_data_set_url, headers=dhis2_headers)
    r_dict = json.loads(r.text)

    dataSetIds = [r_dict['dataSets'][0]['id']]

    # get relevant organisation Ids
    get_org_unit_url = '{}/organisationUnits?filter=dataSets.id:eq:{}'.format(dhis2_api_url, dataSetIds[0])
    organisation_units = get_pages(get_org_unit_url, 'organisationUnits')

    # define date limits
    period_start = datetime.date(2018, 1, 1)
    today = datetime.date.today()
    period_start_str = period_start.strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    for dataSetId in dataSetIds:
        pprint('handling data set {}'.format(dataSetId))
        for orgUnit in organisation_units:
            pprint('handling org unit {}'.format(orgUnit['id']))

            # get data values for each org unit within the date limits
            get_dvs_url = \
                '{root_url}/dataValueSets?dataSet={dataSetId}&orgUnit={orgUnitId}&startDate={startDate}&endDate={endDate}'\
                .format(
                    root_url=dhis2_api_url,
                    dataSetId=dataSetId,
                    orgUnitId=orgUnit['id'],
                    startDate=period_start_str,
                    endDate=today_str
                )

            dvs = requests.get(get_dvs_url, headers=dhis2_headers)

            # loop through data values and remove them
            data_dict = json.loads(dvs.text)
            for data_point in data_dict.get('dataValues', []):
                get_data_value_url = \
                    '{root_url}/dataValues?de={dataElementId}&ou={orgUnitId}&pe={period}'.format(
                        root_url=dhis2_api_url,
                        orgUnitId=orgUnit['id'],
                        dataElementId=data_point['dataElement'],
                        period=data_point['period'])
                ret_delete = requests.delete(get_data_value_url, headers=dhis2_headers)
            if ret_delete.status_code < 300 and ret_delete.status_code >= 200:
                logger.info('Deleted data values for dataSet {} and orgUnit {}'.format(dataSetId, orgUnit['id']))
            else:
                logger

        # Find and delete data elements
        get_data_elements_url = '{}/dataElements?filter=dataSetElements.dataSet.id:eq:{}' \
            .format(dhis2_api_url, dataSetId)
        data_elements = get_pages(get_data_elements_url, 'dataElements')

        for data_element in data_elements:
            delete_data_element_url = '{}/dataElements/{}'.format(dhis2_api_url, data_element['id'])
            ret = requests.delete(delete_data_element_url, headers=dhis2_headers)

        # Delete data set
        delete_data_set_url = '{}/dataSets/{}'.format(dhis2_api_url, dataSetId)
        ret = requests.delete(delete_data_set_url, headers=dhis2_headers)


def delete_event_trackers(prefix):

    # get the program Ids
    get_program_url = '{}/programs?filter=displayName:like:{}'.format(dhis2_api_url, prefix)
    programs = get_pages(get_program_url, 'programs')

    for program in programs:

        # get relevant organisation Ids
        get_org_unit_url = '{}/organisationUnits?filter=programs.id:eq:{}'.format(dhis2_api_url, program['id'])
        organisation_units = get_pages(get_org_unit_url, 'organisationUnits')

        for ou in organisation_units:
            get_events_url = '{}/events?filter=program={}&orgUnit={}'\
                .format(dhis2_api_url, program['id'], ou['id'])
            r_events = requests.get(get_events_url, headers=dhis2_headers)
            r_events_dict = json.loads(r_events.text)

            # delete events from organisation unit
            for event in r_events_dict['events']:
                event_id = event['event']

                delete_event_url = '{}/events/{}'.format(dhis2_api_url, event_id)

                ret_delete = requests.delete(delete_event_url, headers=dhis2_headers)

        get_program_stages_url = '{}/programStages?filter=program.id:eq:{}'.format(dhis2_api_url, program['id'])
        program_stages = get_pages(get_program_stages_url, 'programStages')

        # get data elements from program stage
        data_elements = []
        for program_stage in program_stages:
            get_program_stage_details_url = '{}/programStages/{}'.format(dhis2_api_url, program_stage['id'])
            program_stage_details = requests.get(get_program_stage_details_url, headers=dhis2_headers)
            program_stage_dict = json.loads(program_stage_details.text)
            data_elements = data_elements + program_stage_dict['programStageDataElements']

        for data_element in data_elements:
            delete_data_element_url = '{}/dataElements/{}'.format(dhis2_api_url, data_element['dataElement']['id'])
            ret_delete = requests.delete(delete_data_element_url, headers=dhis2_headers)

        for program_stage in program_stages:
            delete_program_stage_url = '{}/programStages/{}'.format(dhis2_api_url, program_stage['id'])
            ret_delete = requests.delete(delete_program_stage_url, headers=dhis2_headers)

        delete_program_url = '{}/programs/{}'.format(dhis2_api_url, program['id'])
        ret_delete = requests.delete(delete_program_url, headers=dhis2_headers)


if __name__ == "__main__":
    dhis2_api_url = 'https://dhis2.emro.info'
    prefix = 'HOQM'

    delete_data_sets(dhis2_api_url, prefix)
#    delete_event_trackers(dhis2_api_url, prefix)
