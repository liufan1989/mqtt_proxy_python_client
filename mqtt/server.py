#!/usr/bin/python

import logging
import traceback

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.gen

from tornado.options import define, options
from tornado.concurrent import Future

from mqttproxy import MQTTProxy
from pushapi import PushAPI

define("port", default=7890, help="Push Server run on port : 7890", type=int)                                                                                                      

cfg = {
        "mqtt_broker_servers":['192.168.1.241:1883','192.168.1.241:1884','192.168.1.24l:1885'],
        "mqtt_redis_host":"127.0.0.1",
        "mqtt_redis_db":0,
        "mqtt_redis_port":6379,
        "mqtt_redis_expire":60
      }

class Application(tornado.web.Application):
    def __init__(self,config):
        handlers=[
            (r"/getbroker", ProxyHandler),
            (r"/sendmessage", ProxyHandler),
            (r"/broadcast", ProxyHandler)
        ]
        logging.info('Application init...........')
        self.mqtt = MQTTProxy(cfg)
        self.pushapi = PushAPI(self.mqtt)
        tornado.web.Application.__init__(self, handlers, debug=True)

class ProxyHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        pushapi = self.application.pushapi
        try:
            info = self.request.query_arguments
            key = info['key'][0]
            ipport = yield pushapi.dispatch_broker(key)
            res = "Dispatch one Mqtt Broker Server to Client:\n ip:%s,port:%s" % tuple(ipport)
            self.write(res)
        except Exception as e:
            logging.info(traceback.format_exc())
            self.write("error:%s"%e.message)
        self.finish()
        
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        pushapi = self.application.pushapi
        info = self.request.query_arguments
        logging.info(info)
        res = "ok"
        try:
            if self.request.path == "/sendmessage":
                key     = info['key'][0]
                topic   = info['topic'][0]
                message = info['message'][0]
                res = yield pushapi.sendmessage(topic,message,key)

            if self.request.path == "/broadcast":
                keys = info['key']
                topic   = info['topic'][0]
                message = info['message'][0]
                res = yield pushapi.broadcast(topic,message,keys)

        except Exception as e:
            logging.info(traceback.format_exc())
            self.write("error:%s"%e.message)
        self.write(res)
        self.finish()
        

if __name__ == '__main__':
    tornado.options.parse_command_line()
    logging.info("Server Running in Port:%s" % options.port)
    app = Application(options)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

