from http import client
from zahirclient.client import ZahirClient

from zahirclient.proto import view_pb2


client = ZahirClient("grpc://localhost:6674")
req = view_pb2.CreateViewRequest()
resp = client.view_service.CreateView(req)
print("New session: " + resp.session_id)
