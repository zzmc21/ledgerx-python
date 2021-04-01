from ledgerx.http_client import HttpClient
from typing import List, Dict
from ledgerx.util import gen_url
from ledgerx import DEFAULT_LIMIT


class Transactions:
    default_list_params = dict(limit=DEFAULT_LIMIT)

    @classmethod
    def list(cls, params: Dict = {}) -> List[Dict]:
        """Returns a list of all debits and credits to your accounts.

        https://docs.ledgerx.com/reference#gettransactions

        Args:
            params (Dict, optional): [description]. Defaults to {}.

        Returns:
            List[Dict]: [description]
        """
        include_api_key = True
        url = gen_url("/funds/transactions")
        qps = {**cls.default_list_params, **params}
        res = HttpClient.get(url, qps, include_api_key)
        return res.json()

    ### helper methods specific to this API client
