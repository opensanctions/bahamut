from http import client
from bahamut.client import BahamutClient

from bahamut.proto import view_pb2


client = BahamutClient("grpc://localhost:6674")
req = view_pb2.CreateViewRequest()
resp = client.view_service.CreateView(req)
print("New session: " + resp.session_id)
