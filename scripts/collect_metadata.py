import requests
import pandas as pd
import sys
import yaml


from typing import Union, List, Optional
from functools import reduce
from requests.adapters import HTTPAdapter, Retry
from urllib import parse


# Customized Exceptions
class AssetNotFound(Exception):
    def __init__(self, contract: str, token_id: Union[str, int]):
        super().__init__(f"Token {contract}:{token_id} not found!")


class NoAttribute(Exception):
    def __init__(self, contract: str, token_id: Union[str, int]):
        super().__init__(f"Token {contract}:{token_id} has no attribute!")

class NoImage(Exception):
    def __init__(self, contract: str, token_id: Union[str, int]):
        super().__init__(f"Token {contract}:{token_id} has no image!")
  

class AlchemySession(requests.Session):
    def __init__(self, prefix_url=None, *args, **kwargs):
        super(AlchemySession, self).__init__(*args, **kwargs)
        self.prefix_url = prefix_url
        retries = Retry(total=6,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ])

        self.mount('https://', HTTPAdapter(max_retries=retries))

    def request(self, method, url, *args, **kwargs):
        url = parse.urljoin(self.prefix_url, url)
        return super(AlchemySession, self).request(method, url, *args, **kwargs)


# Client for Alchemy NFT API
class AlchemyClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    @property
    def alchemy_client(self):
        return AlchemySession(f'https://eth-mainnet.g.alchemy.com/v2/{self.api_key}/')

    def format_nft_item(self, item):
        token_id = str(int(item['id']['tokenId'], 16))
      
        attributes = reduce(
          lambda acc, x: acc + [(x['trait_type'], x['value'])], item['metadata']['attributes'],
          [('token_id', token_id), ('image', item['metadata']['image'])]
        )
        
        return dict(attributes)

    def safe_get_nft_item(self, item):
        token_id = item['id']['tokenId']
        contract_address = item['contract']['address']

        if not bool(item["metadata"]):
            raise AssetNotFound(contract_address, token_id)

        if not item["metadata"]["attributes"]:
            raise NoAttribute(contract_address, token_id)
        
        if not item["metadata"]["image"]:
            raise NoImage(contract_address, token_id)
      
        return self.format_nft_item(item)


    def get_collection_metadata_paginate(self, contract_address: str, start_token: Optional[str] = None):
        resp = self.alchemy_client.get('getNFTsForCollection', params={
            'contractAddress': contract_address,
            'withMetadata': True,
            'startToken': start_token,
        })
        return resp.json()


def collect_project_seed(api_key, project_name, contract_address):

    filename = "../seeds/{0}.csv".format(project_name)
    client = AlchemyClient(api_key)

    start_token = None
    has_next_token = True
    result = []

    while has_next_token:
        tmp = client.get_collection_metadata_paginate(contract_address, start_token)
        result.extend([client.safe_get_nft_item(i) for i in tmp['nfts']])
        print("{0} nfts collected".format(len(result)))
        start_token = tmp.get('nextToken')
        if not start_token:
            has_next_token = False

    df = pd.DataFrame(result)
    print(df)
    df.to_csv(filename, index=False)


def run():
    conf = {}
    with open("config.yaml", "r") as f:
        conf = yaml.safe_load(f)

    project_name = str(sys.argv[1])

    for proj in conf['projects']:
        if proj['name'] == project_name:
            contract_address = proj['address']
            api_key = conf['alchemy']['api_key']

            collect_project_seed(api_key, project_name, contract_address)
            break
    else:
        print('project not found in config.yaml: {0}',format(project_name))

run()