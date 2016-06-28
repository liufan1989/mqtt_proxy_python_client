#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
from tornado.concurrent import Future

class PushAPI(object):
    def __init__(self,mqtt,dbengine=None):
        print 'init PushAPI......'
        self.mqtt = mqtt
        self.dbengine = dbengine

    def dispatch_broker(self,key):
        if not re.match('\d{11}',key):
            raise Exception("dispatch key is not 11 digit number")
        future = Future()
        ipport = self.mqtt.dispatch_broker_server(key)
        future.set_result(ipport)
        return future

    def sendmessage(self,topic,msg,key=None):
        if key is None:
            raise Exception("dispatch key is none")
        future = Future()
        res = self.mqtt.sendmessage(topic,msg,key)
        future.set_result('ok')
        return future

    def broadcast(self,topic,msg,key=None):
        future = Future()
        self.mqtt.broadcast(topic,msg,key)
        future.set_result('ok')
        return future

    ###添加业务api####



