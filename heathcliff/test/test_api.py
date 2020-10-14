import unittest
import sys, os

from ..api import SearchAdsAPI

class Test_SearchAdsAPI(unittest.TestCase):
    def setUp(self):
      certificates = {
        'pem': 'PEM_FILE_PATH',
        'key': 'KEY_FILE_PATH',
      }
      self.api = SearchAdsAPI(certificates=certificates)

    def test_api_connection(self):
      """
      Test that the SearchAdsAPI class connects to the Apple Search Ads API
      """
      response = self.api.get(endpoint='acls', verbose=True)
      self.assertIsNotNone(response['data'])
    
    def test_api_org_name(self):
      """
      Test that the SearchAdsAPI class connects to the Apple Search Ads API
      """
      response = self.api.get(endpoint='acls', verbose=True)
      org_data = response['data'][0]

      self.api.org_id = org_data['orgId']
      org_name = self.api.get_org_name()

      print('org name:', org_name)
      self.assertIsNotNone(org_name)
