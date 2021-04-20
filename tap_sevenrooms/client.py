from datetime import datetime
import singer
import backoff
import requests
from requests import Response


logger = singer.get_logger()


class SevenroomClientError(Exception):
    pass


class SevenroomBadRequestError(SevenroomClientError):
    pass


class SevenroomUnauthorizedError(SevenroomClientError):
    pass


class SevenroomPaymentRequiredError(SevenroomClientError):
    pass


class SevenroomForbiddenError(SevenroomClientError):
    pass


class SevenroomNotFoundError(SevenroomClientError):
    pass


class SevenroomRequestTimeoutError(SevenroomClientError):
    pass


class SevenroomConflictError(SevenroomClientError):
    pass


class SevenroomTooManyRequestsError(SevenroomClientError):
    pass


class SevenroomInternalServiceError(SevenroomClientError):
    # Error 5XX
    pass


# https://api-docs.sevenrooms.com/getting-started/api-status-codes
ERROR_CODE_EXCEPTION_MAPPING = {
    400: SevenroomBadRequestError,
    401: SevenroomUnauthorizedError,
    403: SevenroomForbiddenError,
    404: SevenroomNotFoundError,
    408: SevenroomRequestTimeoutError,
    409: SevenroomConflictError,
    429: SevenroomTooManyRequestsError
}


def handle_request_error(res: Response):
    if type(res) is not Response:
        raise Exception(f'Response from request is of type {type(res)} -- should be Request')

    if res.status_code >= 500:
        exception = SevenroomInternalServiceError
    else:
        exception = ERROR_CODE_EXCEPTION_MAPPING.get(res.status_code, SevenroomClientError)

    try:
        response = res.json()
    except ValueError:
        raise exception(f'{res.status_code} --- Response not JSON: {res.text}')

    raise exception(f'{res.status_code} --- {response.get("msg", "no error message in response")}')


class SevenRoomsClient:

    def __init__(self, config=None):

        if not config:
            raise Exception('No config found.')

        if 'client_id' not in config or 'client_secret' not in config:
            raise Exception('No client ID or Secret provided')

        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.base_url = config.get('base_url', 'https://demo.sevenrooms.com/api-ext/2_2')

    def __enter__(self):
        res = requests.post(f'{self.base_url}/auth', data=dict(client_id=self.client_id, client_secret=self.client_secret))

        # An exception can be raised here.
        if res.status_code != 200:
            handle_request_error(res)

        logger.info('client connected')

        api_token = res.json()['data']['token']
        self.s = requests.Session()
        self.s.headers.update(dict(Authorization=api_token))

        return self

    def __exit__(self, type, value, traceback):
        self.s.close()
        logger.info("client closed")

    # Rate limiting
    # No official rate limit is defined in the Sevenrooms API however the precense of code 429 in the doc indicates a limit is present.
    @backoff.on_exception(backoff.expo,
                          (SevenroomInternalServiceError, requests.exceptions.ConnectionError, requests.exceptions.Timeout, SevenroomTooManyRequestsError),
                          max_tries=7,
                          factor=3)
    @singer.utils.ratelimit(600, 60)
    def get_data(self, route, params):
        # We will always be using GET, as we have no need to push info upstream.
        res = self.s.get(f'{self.base_url}/{route}', params=params)

        logger.info(f'Sevenroom API request /{route} -- response status: {res.status_code}')
        if res.status_code == 200:

            try:
                res_data = res.json().get('data')
            except ValueError:
                handle_request_error(res)

            if not res_data:
                handle_request_error(res)

            return res_data
        else:
            handle_request_error(res)

    def request_data(self, stream=None, endpoint=None, data_key=None, day=None, use_dates=True, additional_params=None):

        if not stream or not endpoint:
            raise SevenroomClientError('No stream or endpoint sent to client for request.')

        # Default to today.
        if not day:
            day = datetime.now()

        # This is the key in the response body
        # ex, the key would be 'objects' for this response: { 'status': 200, 'objects': [] }
        if not data_key:
            data_key = 'results'

        data = []
        date = day.strftime("%Y-%m-%d")
        date_time = day.strftime("%Y-%m-%d 00:00")
        logger.info(f"Request for date {date}")

        params = dict(limit=400)

        if additional_params:
            params.update(additional_params)

        if use_dates:
            params['to_date'] = date
            params['from_date'] = date

        page = 1
        # Loop until cursor returns nothing
        while True:
            logger.info(f"...page {page}...")

            res = self.get_data(endpoint, params)

            if not res['results']:
                break

            params['cursor'] = res['cursor']
            data += parse_results(res[data_key], date_time)
            page += 1

        return data


def parse_results(result, date):
    return [
        {
            "date": date,
            **{
                key: val
                for key, val in r.items()
                if val is not None
            },
        }
        for r in result
    ]
