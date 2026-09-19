from locust.exception import RescheduleTask, StopUser, CatchResponseError
import subprocess
import locust.env
from random import randint
import grpc

import api_pb2
import api_pb2_grpc

import time
import os
import inspect

from locust import events, User, task 
from locust.exception import LocustError
import gevent

#generate api_pb* using
# python3 -m grpc_tools.protoc -I../../dkv/pkg/serverpb --python_out=scripts --grpc_python_out=scripts ../../dkv/pkg/serverpb/api.proto


class GrpcClient:
    def __init__(self, stub):
        self._stub_class = stub.__class__
        self._stub = stub

    def __getattr__(self, name):
        func = self._stub_class.__getattribute__(self._stub, name)

        def wrapper(*args, **kwargs):
            # get task's function name
            previous_frame = inspect.currentframe().f_back
            _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)

            start_time = time.perf_counter()
            request_meta = {
                "request_type": "grpc",
                "name": task_name,
                "response_length": 0,
                "exception": None,
                "context": None,
                "response": None,
            }
            try:
                request_meta["response"] = func(*args, **kwargs)
                if hasattr(request_meta["response"], 'value'):
                    request_meta["response_length"] = len(request_meta["response"].value)
                if hasattr(request_meta["response"], 'keyValues'):
                    request_meta["response_length"] = sum([len(x.value) for x in request_meta["response"].keyValues])
            except grpc.RpcError as e:
                request_meta["exception"] = e
            request_meta["response_time"] = (time.perf_counter() - start_time) * 1000
            #events.request.fire(**request_meta) # not availabel on locust 1.2.2 on flash.fkinternal.com
            # on 1.2.2 have to use the older method.
            if request_meta["exception"] is not None:
                events.request_failure.fire(**request_meta)
            else:
                events.request_success.fire(**request_meta)
            return request_meta["response"]

        return wrapper


class GrpcUser(User):
    abstract = True
    stub_class = None

    def __init__(self, environment):
        super().__init__(environment)
        for attr_value, attr_name in ((self.host, "host"), (self.stub_class, "stub_class")):
            if attr_value is None:
                raise LocustError(f"You must specify the {attr_name}.")
        grpc_host = self.host.replace("http://","")
        self._channel = grpc.insecure_channel(grpc_host)
        self._channel_closed = False
        stub = self.stub_class(self._channel)
        self.client = GrpcClient(stub)

    def stop(self, force=False):
        self._channel_closed = True
        time.sleep(1)
        self._channel.close()
        super().stop(force=True)

def random_NDigits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)