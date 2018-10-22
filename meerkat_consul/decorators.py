import os

import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from json import JSONDecodeError
from time import time

import requests
from flask import current_app, request
from werkzeug.exceptions import InternalServerError, HTTPException

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
            logger.debug(response.text)
        except JSONDecodeError:
            logger.error(response.text, stack_info=True)
    return response


tasks = {}
BACKGROUND_THREAD_COUNT = int(os.environ.get("BACKGROUND_THREAD_COUNT", "20"))
executor = ThreadPoolExecutor(max_workers=BACKGROUND_THREAD_COUNT)

def async(f):
    """
    This decorator transforms a sync route to asynchronous by running it
    in a background thread.
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        def task(app, environ):
            # Create a request context similar to that of the original request
            # so that the task can have access to flask.g, flask.request, etc.
            with app.request_context(environ):
                try:
                    # Run the route function and record the response
                    tasks[id]['rv'] = f(*args, **kwargs)
                except HTTPException as e:
                    tasks[id]['rv'] = current_app.handle_http_exception(e)
                except Exception as e:
                    # The function raised an exception, so we set a 500 error
                    tasks[id]['rv'] = InternalServerError()
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise
                finally:
                    # We record the time of the response, to help in garbage
                    # collecting old tasks
                    tasks[id]['t'] = time()

        # Assign an id to the asynchronous task
        id = uuid.uuid4().hex

        # Record the task, and then launch it
        logger.info("Starting background tasks with id: {}".format(id))
        tasks[id] = {'task': executor.submit(task, current_app._get_current_object(), request.environ)}

        return '', 202, {'Location': id}
    return wrapped