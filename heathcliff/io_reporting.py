import pandas as pd

from typing import Optional, Dict, List
from io_channel import IOChannelSourceReporter, IOEntityAttribute, IOEntityGranularity, IOChannelGranularity, IOChannelProperty
from .api import AppleSearchAdsAPI
from .reporting import SearchAdsReporter

class IOAppleSearchAdsReporter(IOChannelSourceReporter):
  entity_ids: Optional[Dict[str, List[str]]]

  @classmethod
  def _get_map_identifier(cls) -> str:
    return f'heathcliff/{cls.__name__}'

  def io_entity_granularity_to_api(self, granularity: IOEntityGranularity) -> Optional[str]:
    if granularity is IOEntityGranularity.account:
      return 'org'
    elif granularity is IOEntityGranularity.adgroup:
      return 'adGroup'
    else:
      return super().io_entity_granularity_to_api(granularity)

  def io_entity_attribute_to_api(self, attribute: IOEntityAttribute, granularity: IOEntityGranularity) -> Optional[str]:
    if attribute is IOEntityAttribute.id and granularity:
      if granularity is IOEntityGranularity.account:
        return 'orgId'
      elif granularity is IOEntityGranularity.campaign:
        return 'campaignId'
      elif granularity is IOEntityGranularity.adgroup:
        return 'adGroupId'
    if attribute is IOEntityAttribute.name and granularity is IOEntityGranularity.account:
      return 'orgName'
    if attribute is IOEntityAttribute.daily_budget and granularity is IOEntityGranularity.campaign:
      return 'dailyBudgetAmount'
    if attribute is IOEntityAttribute.goal and granularity is IOEntityGranularity.adgroup:
      return 'cpaGoal'
    return super().io_entity_attribute_to_api(
      attribute=attribute,
      granularity=granularity
    )

  def api_column_to_io(self, api_report: pd.DataFrame, api_column: str, granularity: IOChannelGranularity, property: IOChannelProperty) -> Optional[any]:
    if api_column not in api_report:
      return None
    if property is IOEntityAttribute.id:
      return api_report[api_column].apply(lambda i: str(int(i)) if pd.notna(i) else None)
    return super().api_column_to_io(
      api_report=api_report,
      api_column=api_column,
      granularity=granularity,
      property=property
    )

  def fetch_entity_report(self, granularity: IOEntityGranularity, reporter: SearchAdsReporter) -> pd.DataFrame:
    api_entity_granularity = self.io_entity_granularity_to_api(granularity)
    api_parent_entity_granularity = self.io_entity_granularity_to_api(granularity.higher[0]) if granularity.higher else None
    api_entity_id_column = self.io_entity_attribute_to_api(
      attribute=IOEntityAttribute.id,
      granularity=granularity
    )
    report = reporter.get_entity_report(
      granularity=api_entity_granularity,
      parent_ids=self.entity_ids[api_parent_entity_granularity] if api_parent_entity_granularity in self.entity_ids else None
    )
    if 'id' in report:
      report.rename(columns={'id': api_entity_id_column}, inplace=True)
    if api_entity_id_column in report:
      self.entity_ids[api_entity_granularity] = [str(int(i)) for i in report[api_entity_id_column].unique() if pd.notna(i)]
    return report

  def run(self, credentials: Dict[str, any]) -> Dict[str, any]:
    # TODO: standardize the credentials structure, supporting certificate URLs or content strings, and not assuming they are coming from DataDragon API
    self.prepare_run(
      entity_ids={}
    )
    api = AppleSearchAdsAPI(**credentials)
    reporter = SearchAdsReporter(api=api)

    report = pd.DataFrame()
    org_ids = api.get_org_ids() if api.org_id is None else [api.org_id]
    for org_id in org_ids:
      api.org_id = org_id
      for granularity in reversed(sorted(self.filtered_io_entity_granularities)):
        # TODO: support metrics and time granularity as well
        api_report = self.fetch_entity_report(
          granularity=granularity,
          reporter=reporter
        )
        io_report = self.api_report_to_io(
          api_report=api_report,
          granularities=[granularity]
        )
        self.fill_api_ancestor_identifiers_in_io(
          api_report=api_report,
          io_report=io_report,
          granularities=[granularity]
        )
        report = report.append(io_report)

    report = self.finalized_io_report(report)
    self.clear_run()
    return report