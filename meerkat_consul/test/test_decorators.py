from unittest import TestCase
from unittest.mock import patch, MagicMock

import meerkat_consul.decorators as decorators


class RequestsTestCase(TestCase):
    """
    Unit tests for the requests wrapper
    """

    def setUp(self):
        self.kwargs = {"they": "shall", "pass": "ok"}
        self.fake_url = "http://foo"
        self.bar = "bar"
        self.baz = "baz"

    @patch('requests.put')
    def test_put(self, requests_mock):
        self.__mock_ok_response(requests_mock)
        decorators.put(self.fake_url, data=self.bar, json=self.baz, **self.kwargs)
        requests_mock.assert_called_once_with(self.fake_url, data=self.bar, json=self.baz, **self.kwargs)

    @patch('requests.post')
    def test_post(self, requests_mock):
        self.__mock_ok_response(requests_mock)
        decorators.post(self.fake_url, data=self.bar, json=self.baz, **self.kwargs)
        requests_mock.assert_called_once_with(self.fake_url, data=self.bar, json=self.baz, **self.kwargs)

    @patch('requests.get')
    def test_get(self, requests_mock):
        self.__mock_ok_response(requests_mock)
        decorators.get(self.fake_url, params=self.bar, **self.kwargs)
        requests_mock.assert_called_once_with(self.fake_url, params=self.bar, **self.kwargs)

    @patch('requests.delete')
    def test_delete(self, requests_mock):
        self.__mock_ok_response(requests_mock)
        decorators.delete(self.fake_url, **self.kwargs)
        requests_mock.assert_called_once_with(self.fake_url, **self.kwargs)

    @patch('requests.Response')
    @patch('requests.get')
    def test_should_report_error_when_error_response(self, requests_mock, response_mock):
        response_mock.status_code = 999
        response_mock.json.return_value = {"message": "Error 999"}
        requests_mock.return_value = response_mock
        with self.assertLogs('meerkat_consul', level='ERROR') as cm:
            decorators.get(self.fake_url)
            self.assertEqual(cm.output[0], 'ERROR:meerkat_consul:Request failed with code 999.')
            self.assertTrue("Error 999" in cm.output[1])

    def __mock_ok_response(self, requests_mock):
        response = MagicMock('requests.Response')
        response.status_code = 200
        requests_mock.return_value = response
