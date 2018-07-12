import meerkat_libs.auth_client
from meerkat_consul import app

country_ = app.config['COUNTRY']
consul_auth_role_ = app.config['CONSUL_AUTH_ROLE']


class Authorise(meerkat_libs.auth_client.Authorise):

    def authorise(self, **kwargs):
        return super().authorise([consul_auth_role_], [country_])


auth = Authorise()
