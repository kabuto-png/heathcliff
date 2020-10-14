import unittest
import sys, os

from ..api import SearchAdsAPI
from ..reporting import SearchAdsReporter
from datetime import datetime, timedelta

class Test_SearchAdsReporter(unittest.TestCase):
    def setUp(self):
      certificates = {
        'pem': 'PEM_FILE_PATH',
        'key': 'KEY_FILE_PATY',
      }
      api = SearchAdsAPI(certificates=certificates, org_id=117310)
      self.reporter = SearchAdsReporter(api=api)
      self.reporter.verbose = True

    # def test_campaigns_reporting(self):
    #   start = datetime.utcnow() - timedelta(days=2)
    #   end = datetime.utcnow() - timedelta(days=1)
    #   df = self.reporter.get_campaigns_report(start_date=start, end_date=end)

    #   print(df)
    #   self.assertIsNotNone(df)

    def test_adgroups_reporting(self):
      start = datetime.utcnow() - timedelta(days=2)
      end = datetime.utcnow() - timedelta(days=1)
      df = self.reporter.get_adgroups_report(start_date=start, end_date=end)

      print(df)
      import pdb; pdb.set_trace()
      self.assertIsNotNone(df)

    # def test_campaigns_reporting_columns(self):
    #   start = datetime.utcnow() - timedelta(days=2)
    #   end = datetime.utcnow() - timedelta(days=1)
    #   columns = ['date', 'campaignName', 'impressions']
    #   df = self.reporter.get_campaigns_report(start_date=start, end_date=end, columns=columns)

    #   print(df)
    #   self.assertListEqual(list(df.columns.values), columns)
