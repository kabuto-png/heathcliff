from io_channel import IOEntityKey, IOEntity, IOEntityCommitmentKey, IOEntityCommitter, IOEntityGranularity, IOEntityAttribute
from typing import Dict, Optional
from .api import AppleSearchAdsAPI

class IOAppleSearchAdsEntity(IOEntity):
  def io_property_to_api(self, io_property: str, io_context: Optional[str]=None) -> Optional[str]:
    if io_context == IOEntityKey.update.value:
      if io_property == IOEntityAttribute.daily_budget.value:
        return 'dailyBudgetAmount'
      if io_property == IOEntityAttribute.goal.value:
        return 'cpaGoal'
    return super().io_property_to_api(
      io_property=io_property,
      io_context=io_context
    )

  def io_value_to_api(self, io_property: str, io_value: Optional[any], api_property: Optional[str]=None, io_context: Optional[str]=None) -> Optional[any]:
    if io_context == IOEntityKey.update.value:
      if io_property in [IOEntityAttribute.daily_budget.value, IOEntityAttribute.goal.value]:
        return self.api_money_value(amount=io_value)
    return super().io_value_to_api(
      io_property=io_property,
      io_value=io_value,
      api_property=api_property,
      io_context=io_context
    )

  def api_money_value(self, amount: float, currency: str='USD'):
    return {
      'currency': currency,
      'amount': str(round(amount, 2)),
    }

class IOAppleSearchAdsEntityCommitter(IOEntityCommitter):
  entity: Optional[Dict[str, any]]

  @property
  def api_entity_path(self) -> str:
    ids = self.entity[IOEntityKey.parent_ids.value] if IOEntityKey.parent_ids.value in self.entity else {}
    ids[self.entity[IOEntityKey.granularity.value]] = self.entity[IOEntityKey.id.value]
    path_components = [
      c
      for g in IOEntityGranularity.account.lower
      if g.value in ids
      for c in [f'{g.value}s', ids[g.value]]
    ]
    return '/'.join(path_components)

  def run(self, entity: Dict[str, any], dry_run: bool=False, credentials: Dict[str, any]={}, api: Optional[AppleSearchAdsAPI]=None) -> Dict[str, any]:
    io_entity = IOAppleSearchAdsEntity(entity=entity)
    self.prepare_run(
      entity=entity,
      io_entity=io_entity,
      dry_run=dry_run
    )
    if api is None:
      api = AppleSearchAdsAPI(**credentials)

    update = io_entity.entity[IOEntityKey.update.value]    
    api_update = io_entity.io_to_api(
      io_structure=update,
      io_context=IOEntityKey.update.value
    )
    
    if api_update:
      if io_entity.entity[IOEntityKey.granularity.value] == IOEntityGranularity.campaign.value:
        api_update = {
          io_entity.io_property_to_api(entity[IOEntityKey.granularity.value]): api_update,
        }
      if dry_run:
        api_response = None
      else:
        api_response = api.put(
          endpoint=self.api_entity_path,
          data=api_update
        )
    output = {
      IOEntityCommitmentKey.dry_run.value: dry_run,
      IOEntityCommitmentKey.api_url.value: self.api_entity_path,
      IOEntityCommitmentKey.api_request.value: api_update,
      IOEntityCommitmentKey.api_response.value: api_response,
    }
    self.clear_run()
    return output
