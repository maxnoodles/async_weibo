from jsonpath import jsonpath
import json


with open('test.txt', 'r', encoding='utf8') as f:
    test = f.read()
    test = json.loads(test)

user = jsonpath(test, expr='$..user')[0]
desc1 = jsonpath(test, expr='$..desc1')[0]
print(user)
print(desc1)