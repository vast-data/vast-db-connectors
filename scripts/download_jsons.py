#!/usr/bin/env python3

import requests

import sys
import os

coordinator, = sys.argv[1:]
headers = {
    "X-Trino-User": "user"
}

jsons = requests.get(f"{coordinator}/v1/query", headers=headers).json()
print(f"Downloading {len(jsons)} JSONs to {os.getcwd()}")
for i, query in enumerate(jsons):
    url = query["self"]
    filename = f'{query["queryId"]}.json'
    blob = requests.get(url, headers=headers).content
    print(f"{i:4d} {url} -> {len(blob) / 1e3:10.3f} kB")
    with open(filename, "wb") as f:
        f.write(blob)
