import requests
import json
import pandas as pd

auth_url = 'https://api.sensitconnect.net/users/signin'
data = {'email': 'emil.varghese@cstep.in',
'password': 'Cstep123!@#'
}

response = requests.post(auth_url, data=data)
response_data = response.json()
# print(response_data)
            
# c. API call returns: 'accessToken', 'user', 'message'
# 2. Get the data using the below url and device id and access token received from step 1.
data_url = 'https://api.sensitconnect.net/sensors-data/getRAMPDeviceLogBetweenTimePeriod'
device_id = [2000]
headers = { "Authorization": "Bearer " + response_data['accessToken']}
pay = {"DeviceId": "",
"StartDate": "2024-06-11 06:20:00",
"EndDate": "2024-06-11 07:20:00"
}
res=requests.post(data_url, data=pay, headers=headers)
print(res.json()['data'])
df = pd.DataFrame(res.json()['data'])
print("**********************************")
print(df.columns)
print("**********************************")
print(df)
