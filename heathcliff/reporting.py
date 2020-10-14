import pandas as pd
import copy

from . import api
from datetime import datetime
from typing import Optional, List, Dict

class SearchAdsReporter:
  api: api.SearchAdsAPI
  verbose: bool = False
  _date_format = '%Y-%m-%d'

  def __init__(self, api):
    self.api = api

  def get_searchterms_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None, request_overrides: Dict[str, any]={}) -> pd.DataFrame:
    campaigns_report = self.get_campaigns_report(start_date=start_date, end_date=end_date, columns=columns)
    df = pd.DataFrame()
    if campaigns_report.empty:
      return df
    for campaign_id in campaigns_report.campaignId.unique():
      body = self.report_request_body(
        start_date=start_date, 
        end_date=end_date, 
        body_overrides={
          'returnRecordsWithNoMetrics': False,
          'selector': {
            'orderBy': [{
              'field': 'keywordId',
              'sortOrder': 'DESCENDING'
            }],
            'pagination': {
              'limit': 5000,
            }
          },
          **request_overrides,
        }
      )
      response = self.api.post(endpoint=f'reports/campaigns/{campaign_id}/searchterms', data=body, verbose=self.verbose)
      
      report = self._convert_response_to_data_frame(response=response, columns=columns)
      report['campaignId'] = campaign_id
      report['campaignName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignName.unique()[0]
      report['campaignStatus'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignStatus.unique()[0]
      report['adamId'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].adamId.unique()[0]
      report['appName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].appName.unique()[0]
      df = df.append(report, sort=False)

    df.reset_index(drop=True, inplace=True)
    return df

  def get_keywords_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None, request_overrides: Dict[str, any]={}) -> pd.DataFrame:
    campaigns_report = self.get_campaigns_report(start_date=start_date, end_date=end_date, columns=columns)
    df = pd.DataFrame()
    if campaigns_report.empty:
      return df
    for campaign_id in campaigns_report.campaignId.unique():
      body = self.report_request_body(start_date, end_date, request_overrides)
      response = self.api.post(endpoint=f'reports/campaigns/{campaign_id}/keywords', data=body, verbose=self.verbose)

      report = self._convert_response_to_data_frame(response=response, columns=columns)
      report['campaignId'] = campaign_id
      report['campaignName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignName.unique()[0]
      report['campaignStatus'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignStatus.unique()[0]
      report['adamId'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].adamId.unique()[0]
      report['appName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].appName.unique()[0]
      df = df.append(report, sort=False)

    df.reset_index(drop=True, inplace=True)
    return df

  def get_creative_sets_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None, request_overrides: Dict[str, any]={}) -> pd.DataFrame:
    campaigns_report = self.get_campaigns_report(start_date=start_date, end_date=end_date, columns=columns)
    df = pd.DataFrame()
    if campaigns_report.empty:
      return df
    for campaign_id in campaigns_report.campaignId.unique():
      body = self.report_request_body(start_date, end_date, request_overrides)
      response = self.api.post(endpoint=f'reports/campaigns/{campaign_id}/creativesets', data=body, verbose=self.verbose)

      report = self._convert_response_to_data_frame(response=response, columns=columns)
      report['campaignId'] = campaign_id
      report['campaignName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignName.unique()[0]
      report['campaignStatus'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignStatus.unique()[0]
      report['adamId'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].adamId.unique()[0]
      report['appName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].appName.unique()[0]
      df = df.append(report, sort=False)

    df.reset_index(drop=True, inplace=True)
    return df

  def get_adgroups_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None, request_overrides: Dict[str, any]={}) -> pd.DataFrame:
    campaigns_report = self.get_campaigns_report(start_date=start_date, end_date=end_date, columns=columns)
    df = pd.DataFrame()
    if campaigns_report.empty:
      return df
    for campaign_id in campaigns_report.campaignId.unique():
      body = self.report_request_body(start_date, end_date, request_overrides)
      response = self.api.post(endpoint=f'reports/campaigns/{campaign_id}/adgroups', data=body, verbose=self.verbose)

      adgroup_report = self._convert_response_to_data_frame(response=response, columns=columns)
      adgroup_report['campaignId'] = campaign_id
      adgroup_report['campaignName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignName.unique()[0]
      adgroup_report['campaignStatus'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].campaignStatus.unique()[0]
      adgroup_report['adamId'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].adamId.unique()[0]
      adgroup_report['appName'] = campaigns_report.loc[campaigns_report.campaignId == campaign_id].appName.unique()[0]
      df = df.append(adgroup_report, sort=False)

    df.reset_index(drop=True, inplace=True)
    return df

  def get_campaigns_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None, request_overrides: Dict[str, any]={}) -> pd.DataFrame:
    body = self.report_request_body(start_date, end_date, request_overrides)
    response = self.api.post(endpoint='reports/campaigns', data=body, verbose=self.verbose)
    return self._convert_response_to_data_frame(response=response, columns=columns)
  
  def report_request_body(self, start_date: datetime, end_date: datetime, body_overrides: Dict[str, any]={}) -> Dict[str, any]:
    body = {
      'startTime': start_date.strftime(self._date_format),
      'endTime': end_date.strftime(self._date_format),
      'timeZone': 'UTC',
      'granularity': 'DAILY',
      'returnRowTotals': False,
      'returnRecordsWithNoMetrics': True,
      'selector': {
        'orderBy': [{
          'field': 'modificationTime',
          'sortOrder': 'DESCENDING'
        }],
        'pagination': {
          'limit': 5000,
        }
      }
    }
    body.update(body_overrides)
    return body

  def get_entity_report(self, granularity: str, ids: Optional[List[str]]=None, parent_ids: Optional[List[str]]=None, columns: Optional[List[str]]=None) -> pd.DataFrame:
    if granularity == 'org':
      endpoints = ['acls']
    elif granularity == 'campaign':
      endpoints = [f'campaigns']
    elif granularity == 'adGroup':
      if parent_ids is None:
        parent_report = self.get_entity_report(
          granularity='campaign',
          columns=['id']
        )
        parent_ids = list(parent_report.id.unique()) if not parent_report.empty else []
      endpoints = [
        f'campaigns/{i}/adgroups'
        for i in parent_ids
      ]

    report = pd.DataFrame()
    for endpoint in endpoints:
      response = self.api.get(
        endpoint=endpoint,
        query_parameters={
          'limit': '1000',
        }
      )
      df = self._convert_response_to_data_frame(
        response=response,
        columns=columns,
        is_entity_response=True
      )
      report = report.append(df)
    
    if ids is not None and not report.empty:
      report = report.loc[report.id.isin(ids)]

    report.reset_index(drop=True, inplace=True)
    return report

  def _convert_response_to_data_frame(self, response: Dict[str, any], columns: List[str], is_entity_response: bool=False) -> pd.DataFrame:
    pagination = response['pagination']
    
    if pagination is not None:
      total_results = pagination['totalResults']
      items_per_page = pagination['itemsPerPage']

      if total_results > items_per_page:
        error_string = f'totalResults ({total_results}) is greater than itemsPerPage ({items_per_page})'
        raise ValueError('Apple Search Ads reporting paginiation is not supported', error_string)

    try:
      if is_entity_response:
        reporting_data = [{'granularity': [r]} for r in response['data']]
      else:
        reporting_data = response['data']['reportingDataResponse']['row']
    except:
        raise ValueError('Unexpected response', response) # ['data']['error']['errors'])

    output = []
    for row in reporting_data:
      base = {}
      if 'metadata' in row:
        base.update(row['metadata'])
      if 'app' in base:
        base['adamId'] = base['app']['adamId']
        base['appName'] = base['app']['appName']
        del base['app']

      if 'servingStateReasons' in base and type(base['servingStateReasons']) is list:
        base['servingStateReasons'] = ','.join(base['servingStateReasons'])

      if columns is not None:
        base = {key: base[key] for key in base if key in columns}

      for granularity in row['granularity']:
        final_row = copy.copy(base)
        final_row.update(granularity)
        if columns is not None:
          final_row = {key: final_row[key] for key in final_row if key in columns}
        final_row = _convert_to_float_all_amounts_in_row(final_row)
        if columns is not None and 'original_currency' not in columns:
          del final_row['original_currency']
        output.append(final_row)

    df = pd.DataFrame(output)
    if columns is not None:
      df = df[columns]

    return df

def _amount_to_float(amount):
  return float(amount['amount'])

def _convert_to_float_all_amounts_in_row(row):
  _row = copy.copy(row)
  currencies = {}
  for field_name, value in _row.items():
    if isinstance(value, dict) and 'currency' in value:
      if value['currency'] not in currencies.values():
        currencies[field_name] = value['currency']
      _row[field_name] = _amount_to_float(value)

  if len(currencies) > 1:
    raise ValueError('Report row includes different currencies', currencies)
  else:
    _row['original_currency'] = list(currencies.values())[0] if currencies else None

  return _row