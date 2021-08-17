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
    rand_bytes = os.urandom(4096)
    # wait_time = constant(0.0)
    wait_time = between(0, 0.1)

    def on_start(self):
        """ on_start is called when a Locust start before 
            any task is scheduled
        """
        self.userId = str(uuid.uuid4())
        self.doPrepareMGet()

        
    def doPrepareMGet(self):
        if not self._channel_closed:
            for x in range(10):
                rq = api_pb2.PutRequest(key=bytes(f"nfr-{self.userId}-{x}", 'utf-8'), value=self.rand_bytes)
                self.client.Put(rq)

    @task(1)
    def doPut(self):
        if not self._channel_closed:
            rq = api_pb2.PutRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'), value=self.rand_bytes)
            self.client.Put(rq)

    @task(1)
    def doPutTTL(self):
        if not self._channel_closed:
            epoch_time = int(time.time())
            ttl = epoch_time + 3600 
            rq = api_pb2.PutRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'), value=self.rand_bytes, expireTS=ttl)
            self.client.Put(rq)
    
    @task(1)
    def doMultiPut(self):
        if not self._channel_closed:
            m_keys=[]
            for x in range(10):
                userId = str(uuid.uuid4())
                rq = api_pb2.PutRequest(key=bytes(f"nfr-multiput-{userId}", 'utf-8'), value=self.rand_bytes)
                m_keys.append(rq)
                    
            rq = api_pb2.MultiPutRequest(putRequest=m_keys)
            out = self.client.MultiPut(rq)

    @task(2)
    def doGet(self):
        if not self._channel_closed:
            rq = api_pb2.GetRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'))
            out = self.client.Get(rq)
            # print(out)

    @task(2)
    def doMultiGet(self):
        m_keys=[bytes(f"nfr-{self.userId}", 'utf-8'), bytes("non-existent",'utf-8')]
        for x in range(10):
            m_keys.append(bytes(f"nfr-{self.userId}-{x}", 'utf-8'))

        if not self._channel_closed:
            rq = api_pb2.MultiGetRequest(keys=m_keys)
            out = self.client.MultiGet(rq)
            # print(out)

    @task(1)
    def doDelete(self):
        if not self._channel_closed:
            rq = api_pb2.DeleteRequest(key=bytes(f"nfr-{self.userId}", 'utf-8'))
            out = self.client.Delete(rq)

    @task(1)
    def doCAS(self):
        if not self._channel_closed:
            keyId = str(uuid.uuid4())

            rq = api_pb2.PutRequest(key=bytes(f"nfr-{keyId}", 'utf-8'), value=self.rand_bytes)
            self.client.Put(rq)

            rq = api_pb2.CompareAndSetRequest(key=bytes(f"nfr-{keyId}", 'utf-8'), oldValue=self.rand_bytes, newValue=os.urandom(100))
            out = self.client.CompareAndSet(rq)

            # rq = api_pb2.GetRequest(key=bytes(f"nfr-{keyId}", 'utf-8'))
            # out = self.client.Get(rq)
