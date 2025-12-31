import json
from urllib.request import urlopen

with urlopen("https://latest.currency-api.pages.dev/v1/currencies/usd.json") as response:
	source = response.read()

data = json.loads(source)

# print(json.dumps(data, indent=2))

for key, value in data.items():
	print(key, value)

