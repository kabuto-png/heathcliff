import requests
from typing import Optional, Dict, List
from .context import AppleSearchAdsCertificate

class SearchAdsAPI:
  api_version: str
  certificates: Dict[str, str]
  org_id: Optional[int]

  def __init__(self, certificates: Dict[str, str], org_id: Optional[int]=None, api_version: str='v3'):
    self.api_version = api_version
    self.certificates = certificates
    self.org_id = org_id

  def _api_call(self, method: any, endpoint: str, json_data: Dict[str, any]={}, verbose: bool=False, query_parameters: Dict[str, any]={}):
    call_kwargs = {
      'cert': (
        self.certificates['pem'],
        self.certificates['key'],
      ),
      'headers': {},
      **({'params': query_parameters} if query_parameters else {}),
    }

    if json_data:
      call_kwargs['json'] = json_data

    if self.org_id:
      call_kwargs['headers']['Authorization'] = "orgId={org_id}".format(org_id=self.org_id)

    req = method(
      "https://api.searchads.apple.com/api/{api_version}/{endpoint}".format(
        api_version=self.api_version, 
        endpoint=endpoint
      ),
      **call_kwargs,
    )

    if verbose:
      print(req.text)

    return req.json()
  
  def get_org_name(self):
    if self.org_id is None:
      raise ValueError('get_org_name() called with no org_id is not yet supported.')

    response = self.get('acls')
    if response['data'] is None:
      raise ValueError('acls response data is None', response)

    names = [elem for elem in response['data'] if elem['orgId'] == self.org_id]
    if len(names) != 1:
      raise ValueError('unable to get valid orgName value from response', response)
    
    return names[0]['orgName']

  def get_org_ids(self) -> List[int]:
    response = self.get('acls')
    if response['data'] is None:
      raise ValueError('acls response data is None', response)
    org_ids = [acl['orgId'] for acl in response['data']]
    return org_ids

  def get(self, endpoint, verbose=False, query_parameters: Dict[str, any]=[]):
    return self._api_call(
      method=requests.get,
      endpoint=endpoint,
      verbose=verbose,
      query_parameters=query_parameters
    )

  def put(self, endpoint, data={}, verbose=False):
    return self._api_call(
      method=requests.put,
      endpoint=endpoint,
      json_data=data,
      verbose=verbose
    )

  def post(self, endpoint, data={}, verbose=False):
    return self._api_call(
      method=requests.post,
      endpoint=endpoint,
      json_data=data,
      verbose=verbose
    )
  
  def get_campaigns(self) -> List[Dict[str, any]]:
    response = self.post(endpoint='campaigns/find')
    return response

class AppleSearchAdsAPI(SearchAdsAPI):
  certificate: AppleSearchAdsCertificate

  def __init__(self, **kwargs):
    self.certificate = AppleSearchAdsCertificate(certificate=kwargs)
    self.certificate.connect()
    super().__init__(
      certificates=self.certificate.connection,
      org_id=self.certificate.org_id
    )

  def __del__(self):
    if self.certificate.connection is not None:
      self.certificate.disconnect()
