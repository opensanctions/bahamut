from http import client
from zahirclient.client import ZahirClient

client = ZahirClient("grpc://localhost:6674")
print(client.session_service.CreateSession())
