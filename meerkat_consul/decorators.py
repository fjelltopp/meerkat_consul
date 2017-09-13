from json import JSONDecodeError

import requests

from meerkat_consul import logger


def put(url, data=None, json=None, **kwargs):
    """
    Wrapper for requests.put which validates status code of the response
    :param url:
    :param data:
    :param json:
    :param kwargs:
    :return: requests.Response
    """
    response = requests.put(url, data=data, json=json, **kwargs)
    return __check_if_response_is_ok(response)


def get(url, params=None, **kwargs):
    """
    Wrapper for requests.get which validates status code of the response
    :param url:
    :param data:
    :param json:
    :param kwargs:
    :return: requests.Response
    """
    response = requests.get(url, params=params, **kwargs)
    return __check_if_response_is_ok(response)


def post(url, data=None, json=None, **kwargs):
    """
    Wrapper for requests.post which validates status code of the response
    :param url:
    :param data:
    :param json:
    :param kwargs:
    :return: requests.Response
    """
    response = requests.post(url, data=data, json=json, **kwargs)
    return __check_if_response_is_ok(response)


def delete(url, **kwargs):
    """
    Wrapper for requests.delete which validates status code of the response
    :param url:
    :param data:
    :param json:
    :param kwargs:
    :return: requests.Response
    """
    response = requests.delete(url, **kwargs)
    return __check_if_response_is_ok(response)


def __check_if_response_is_ok(response):
    if 200 < response.status_code >= 300:
        logger.error("Request failed with code %d.", response.status_code)
        try:
            logger.error(response.json().get("message"), stack_info=True)
        except JSONDecodeError:
            logger.error(response.text, stack_info=True)
    return response