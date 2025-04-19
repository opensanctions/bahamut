
# betterproto:
# 	python -m grpc_tools.protoc -I../proto --python_betterproto_out=bahamut/proto ../proto/*.proto

proto:
	python -m grpc_tools.protoc -I../proto --python_out=bahamut/proto --pyi_out=bahamut/proto --grpc_python_out=bahamut/proto ../proto/*.proto
	# perl -pi -e 's/^import (?!\.)([a-zA-Z0-9_]+_pb2 as [a-zA-Z0-9_]+__pb2)/import .$1/' bahamut/proto/*.py
	protol --create-package --in-place --python-out bahamut/proto protoc --proto-path=../proto ../proto/*.proto
