from meerkat_consul import logger
from meerkat_consul.decorators import get


class NewIdsProvider:
    def __init__(self, dhis2_api_url, headers):
        self.dhis2_api_url = dhis2_api_url
        self.headers = headers
        self.ids = []

    def pop(self):
        if not self.ids:
            self.ids = self.__get_dhis2_ids()
        return self.ids.pop()

    def __get_dhis2_ids(self, n=100):
        response = get("{}system/id.json?limit={}".format(self.dhis2_api_url, n), headers=self.headers).json()
        result = response.get('codes', [])
        if not result:
            logger.error("Could not get ids from DHIS2.")
        return result