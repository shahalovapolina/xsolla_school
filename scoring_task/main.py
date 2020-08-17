from google.oauth2 import service_account
import lib_main

account_info = {} # данные для подключения

CREDENTIALS = service_account.Credentials.from_service_account_info(account_info)

DB_CONFIG = {
  'ProjectId': 'findcsystem',
  'DatasetId': 'xsolla_summer_school',
  'Developer': 'polina.kondrashova'
}

DataFrame = lib_main.getFreshData(CREDENTIALS,DB_CONFIG['ProjectId'])

lib_main.insertScoreResultData(
  lib_main.ResultStatus(DataFrame),
  DB_CONFIG['ProjectId'],
  DB_CONFIG['DatasetId'],
  'score_result_status',
  DB_CONFIG['Developer']
)

lib_main.insertScoreResultData(
  lib_main.ResultTotal(DataFrame),
  DB_CONFIG['ProjectId'],
  DB_CONFIG['DatasetId'],
  'score_result_total',
  DB_CONFIG['Developer']
)

lib_main.insertScoreResultData(
  lib_main.ResultStatusChannel(DataFrame),
  DB_CONFIG['ProjectId'],
  DB_CONFIG['DatasetId'],
  'score_result_status_channel',
  DB_CONFIG['Developer']
)

print("Данные загружены!")