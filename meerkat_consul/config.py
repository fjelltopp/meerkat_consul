# Need this module to be importable without the whole of meerkat_auth config
# Directly load secret settings file from which to import required variables
# File must include JWT_COOKIE_NAME, JWT_ALGORITHM and JWT_PUBLIC_KEY variables

dhis2_config = {
    "url": "http://dhis2-web:8080",
    "apiResource": "/api/26/",
    "credentials": ('admin', 'district'),
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46ZGlzdHJpY3Q="
    },
    "loggingLevel": "DEBUG",
    "forms": [
        {
            "name": "demo_case",
            "event_date": "pt./visit_date",
            "completed_date": "end",
            # "programId": "fgrH0jPDNEP", # optional
            "status": "COMPLETED"
        },
        {
            "name": "demo_alert",
            "date": "end"
        },
        {
            "name": "demo_register",
            "date": "intro./visit_date"
        }
    ]
}

COUNTRY_LOCATION_ID = 1
