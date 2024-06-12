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
    rand_bytes = os.urandom(4096)
     # wait_time = constant(0.0)
    wait_time = between(0, 0.1)

    @task
    def doPut(self):
        if not self._channel_closed:
            userId = str(uuid.uuid4())
            rq = api_pb2.PutRequest(key=bytes(f"nfr-{userId}", 'utf-8'), value=self.rand_bytes)
            self.client.Put(rq)

    @task
    def doPutTTL(self):
        if not self._channel_closed:
            userId = str(uuid.uuid4())
            ttl = int(time.time()) + 600 
            rq = api_pb2.PutRequest(key=bytes(f"nfr-{userId}", 'utf-8'), value=self.rand_bytes, expireTS=ttl)
            self.client.Put(rq)
    
    @task
    def doMultiPut(self):
        if not self._channel_closed:
            m_keys=[]
            for x in range(10):
                userId = str(uuid.uuid4())
                rq = api_pb2.PutRequest(key=bytes(f"nfr-multiput-{userId}", 'utf-8'), value=self.rand_bytes)
                m_keys.append(rq)
                    
            rq = api_pb2.MultiPutRequest(putRequest=m_keys)
            out = self.client.MultiPut(rq)
