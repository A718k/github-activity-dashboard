"""@bruin
name: test_gcs
type: python
image: python:3.13
connection: gcs-default
@bruin"""

from google.cloud import storage


client = storage.Client()
buckets = list(client.list_buckets())
print("Buckets:", [b.name for b in buckets])
