COUNTRY = 'demo'
CONSUL_AUTH_ROLE = 'consul'

FORM_EXPORT_CONFIG = {
    "demo_case": {
        "exportName": "Meerkat Case Form",
        "exportType": "event"
    },
    "demo_register": {
        "exportName": "Meerkat Daily Registry",
        "exportType": "data_set"
    }
}

DHIS2_CONFIG = {
    "url": "http://172.18.0.1:8085",
    "apiResource": "/api/27",
    "credentials": ('senyoni', 'admin'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic c2VueW9uaTphZG1pbg=="
    }
}
