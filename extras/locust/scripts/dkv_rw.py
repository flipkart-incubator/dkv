#!interpreter [optional-arg]
# -*- coding: utf-8 -*-

"""
This contains to load test of DKV example of gRPC call with locust.
"""

import grpc
import api_pb2
import api_pb2_grpc

from locust import events, User, task, between,constant
import gevent
import time
import uuid
import os
from common import GrpcUser

# patch grpc so that it uses gevent instead of asyncio
import grpc.experimental.gevent as grpc_gevent
grpc_gevent.init_gevent()


class DKVGrpcUser(GrpcUser):
    host = "localhost:8080"
    stub_class = api_pb2_grpc.DKVStub
    userId = None
    rand_bytes = os.urandom(1024)
    # wait_time = constant(0.0)
    wait_time = between(0, 0.1)

    def on_start(self):
        """ on_start is called when a Locust start before 
            any task is scheduled
        """
        self.userId = str(uuid.uuid4())

    @task(5)
    def doPut(self):
        if not self._channel_closed:
            rq = api_pb2.PutRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'), value=self.rand_bytes)
            self.client.Put(rq)

    @task(5)
    def doGet(self):
        if not self._channel_closed:
            rq = api_pb2.GetRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'))
            out = self.client.Get(rq)
            # print(out)

 