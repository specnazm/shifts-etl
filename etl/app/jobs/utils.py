import requests

API_CALLS_LIMIT = 100
RESPONSE_LIMIT = 30
BASE_URL =  f'http://shifts_api:8000'
INITIAL_URL = f'/api/shifts?limit={RESPONSE_LIMIT}'


def get_next_url(response):
    if ("next" in response["links"]):
        return BASE_URL + response["links"].get("next")

    return None

def send_request(url):
    try:
        response = requests.get(url)
        json = response.json()
        if (response.status_code != 200):
            log.error(f"Api responded with code: {response.status_code}")
            return None
        return json
    except Exception as err:
        print(f'An error occured : f{err}')
        return None



def log_status(func):

    def wrapper(*args, **kwargs):
        from app.jobs.etl_job import log
        log.warn('job is up and running')

        result = func(*args, **kwargs)

        log.warn('job is finished')

        return result

    return wrapper