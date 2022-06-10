import requests
import json
url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
nickname = "lakamb"
cohort = "1"
headers = {
    "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
    "X-Nickname": nickname,
    "X-Cohort": cohort
}
#делаем запрос на формирование данных
method_url = '/generate_report'
r = requests.post(url + method_url, headers=headers)
response_dict = json.loads(r.content)
print(response_dict)
task_id = response_dict['task_id']
cohort = int(cohort)