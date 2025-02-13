
# betterproto:
# 	python -m grpc_tools.protoc -I../proto --python_betterproto_out=zahirclient/proto ../proto/*.proto

proto:
	python -m grpc_tools.protoc -I../proto --python_out=zahirclient/proto --pyi_out=zahirclient/proto --grpc_python_out=zahirclient/proto ../proto/*.proto
	# perl -pi -e 's/^import (?!\.)([a-zA-Z0-9_]+_pb2 as [a-zA-Z0-9_]+__pb2)/import .$1/' zahirclient/proto/*.py
	protol --create-package --in-place --python-out zahirclient/proto protoc --proto-path=../proto ../proto/*.proto








