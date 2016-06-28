#!/usr/bin/python
# -*- coding: utf-8 -*-

from hash_ring import HashRing
import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqtt_publish
import redis
import threading
import time
import logging

class IMQTT(object):
    def on_message(self,msg):
        pass
    def on_connect(self,flag,rc):
        pass
    def on_disconnect(self,rc):
        pass
    def on_publish(self,mid):
        pass
    def on_subscribe(self,mid,qos):
        pass
    def on_unsubscribe(self,mid):
        pass

#mqtt 功能类，可以实现subscribe和publish
#MQTTProxy主要使用了publish接口，没有使用subscribe接口，如果有需要可自行修改MQTTProxy类
class MQTT(IMQTT,threading.Thread):
    def __init__(self,ip=None,port=None,id=None,kp=60,cs=True):
        logging.info('MQTT init...........')
        threading.Thread.__init__(self)
        if id != None:
            self.clientid = id
        else:
            self.clientid = ("%.6f" % time.time()).replace(".","")
        if ip == None or port == None:
            raise ValueError("init MQTT error!")
        self.ip = ip
        self.port = port
        self.kp = kp
        self.cs = cs
        self.topic = []
        self.client = mqtt.Client(client_id=self.clientid,clean_session=self.cs, userdata=self)
        self.client.on_connect = MQTT.connect_callback
        self.client.on_message = MQTT.message_callback
        self.client.on_publish = MQTT.publish_callback
        self.client.on_subscribe = MQTT.subscribe_callback
        self.client.on_unsubscribe = MQTT.unsubscribe_callback
        self.client.on_disconnect = MQTT.disconnect_callback
        #连接broker
        self.client.connect(self.ip,self.port,keepalive=self.kp)
        self.start()
        self.__initialized = True


    def __reset(self):
        self.__initialized = True
        self.client.connect(self.ip,self.port,keepalive=self.kp)
        for x in self.topic:
            self.client.subscribe(x)

    def run(self):
        while True:
            try:
                self.client.loop_forever()
                break
            except:
                logging.info('mqtt loop_forever exception')
                self.__reset()
        logginfo('disconnect with broker normally')

    def __def__(self):
        self.disconnect()

    def subscribe(self,topic=[]):
        if not self.__initialized:
            raise RuntimeError("MQTT.__init__() not called")
        if not isinstance(topic,list) or topic == []:
            raise TypeError("MQTT subscribe type is error")
        self.topic += topic
        for x in self.topic:
            self.client.subscribe(x)

    def unsubscribe(self,topic=[]):
        if not self.__initialized:
            raise RuntimeError("MQTT.__init__() not called")
        if not isinstance(topic,list) or topic == []:
            raise TypeError("MQTT unsubscribe type is error")
        for x in topic:
            if x not in self.topic:
                raise RuntimeError("unsubscribe topic is not exist")
            self.client.unsubscribe(x)

    def disconnect(self):
        if not self.__initialized:
           raise RuntimeError("MQTT.__init__() not called")
        time.sleep(1)
        self.__initialized = False
        self.client.disconnect()

    def on_message(self,msg):
        logging.info('MQTT call on_message')
        logging.info("topic:"+msg.topic+","+"msg:"+str(msg.payload))

    @staticmethod
    def connect_callback(client,userdata,flag,rc):
        logging.info('connect callback')
        userdata.on_connect(flag,rc)

    @staticmethod
    def message_callback(client,userdata,msg):
        logging.info('message callback')
        userdata.on_message(msg)

    @staticmethod
    def publish_callback(client,userdata,mid):
        logging.info('publish callback')
        userdata.on_publish(mid)

    @staticmethod
    def subscribe_callback(client,userdata,mid,qos):
        logging.info('subscirbe callback')
        userdata.on_subscribe(mid,qos)

    @staticmethod
    def unsubscribe_callback(client,userdata,mid):
        logging.info('unsubscribe callback')
        userdata.on_unsubscribe(mid)

    @staticmethod
    def disconnect_callback(client,userdata,rc):
        logging.info('disconnect callback')
        userdata.on_disconnect(rc)

    @staticmethod 
    def compose_msg(topic,msgs,rt,qs):
        return [(topic,x,qs,rt) for x in msgs]

    #发送一个主题的一条消息
    @staticmethod 
    def publish_one(ip,port,topic,msg,rt=False,qs=0):
        mqtt_publish.single(topic,msg,hostname=ip,port=port,retain=rt,qos=qs)

    #发送一个主题的多条消息
    @staticmethod 
    def publish_many(ip,port,topic,msgs,rt=False,qs=0):
        if not isinstance(msgs,list):
            raise TypeError('publish_many msgs type is error!')
        payloads = MQTT.compose_msg(topic,msgs,rt,qs)
        mqtt_publish.multiple(payloads,hostname=ip,port=port)

class MQTTProxy(object):
    broker_servers = []
    broker_number = 0
    redis_pool = None
    redis_key_expire = 60*60*24#redis中信息老化时间暂时没有使用
    '''
    broker_servers = ['192.168.1.241:1883',
                        '192.168.1.241:1884',
                        '192.168.1.249:1885']
    '''
    def __init__(self,cfg):
        logging.info('init MQTTProxy.....')
        logging.info("broker servers : " + str(cfg['mqtt_broker_servers']))

        MQTTProxy.redispool = redis.ConnectionPool(host=cfg["mqtt_redis_host"], port=cfg["mqtt_redis_port"],
                                                db=cfg["mqtt_redis_db"])
        MQTTProxy.redis_key_expire = cfg['mqtt_redis_expire']
        MQTTProxy.broker_servers = cfg['mqtt_broker_servers']

        if not isinstance(MQTTProxy.broker_servers,list) or MQTTProxy.broker_servers == []:
            raise RuntimeError ('init MQTTProxy fail....')

        MQTTProxy.broker_number = len(MQTTProxy.broker_servers)
        #如果启动时要检查mqtt broker服务有没开启，可以将这行注释打开
        #self.check_broker_server(self.broker_servers)
        self.ring = HashRing(self.broker_servers)

    def __get_redis_conn(self):
        return redis.StrictRedis(connection_pool=MQTTProxy.redispool)       
  
    def __get_ip_port(self,broker):
        return broker.split(":")

    #根据key获取所对应的broker的ip和port,key合法性在函数外校验
    def get_broker_server_info(self,key):
        redisconn = self.__get_redis_conn()
        broker = redisconn.get(key)
        if broker == None:
            broker = self.dispatch_broker_server(key)
        return broker

    #根据key删除redis中保存的broker的ip和port
    def del_broker_server_info(self,key):
        redisconn = self.__get_redis_conn()
        broker = redisconn.get(key)
        pipe = redisconn.pipeline()
        pipe.multi()
        pipe.delete(key)
        pipe.hdel(broker,key)
        pipe.execute()

    #根据key给用户分配一个broker服务的ip和port
    def dispatch_broker_server(self,key):
        redisconn = self.__get_redis_conn()
        brokerN = self.ring.get_node(key) 
        brokerO = redisconn.get(key)
        if brokerN != brokerO:
           redisconn.hdel(brokerO,key) 
        pipe = redisconn.pipeline()
        pipe.multi()
        pipe.set(key,brokerN)
        pipe.hset(brokerN,key,1)
        pipe.execute()
        return self.__get_ip_port(brokerN)

    #检查所有的broker服务是否已经启动
    def check_broker_server(self,brokerlist):
        try:
            if not isinstance(brokerlist,list):
                brokerlist = list(brokerlist)
            for broker in brokerlist:
                ip,port = self.__get_ip_port(broker)
                client = mqtt.Client()
                client.connect(ip,port)
        except:
            raise RuntimeError("Checking out MQTT broker:broker server is not Running!")

    #获取这个代理管理的所有broker服务器ip和port
    def get_broker_server_list(self):
        return MQTTProxy.broker_servers

    #获取这些key所对应的broker的ip和port
    def get_broker_server_by_keys(self,keys):
        if not isinstance(keys,list):
            raise TypeError("get_broker_server_by keys type is error!")
        redisconn = self.__get_redis_conn()
        brokerlist = redisconn.mget(keys)
        brokerlist=list(set(brokerlist))
        if None in brokerlist:
            brokerlist.remove(None)
        return brokerlist

    #给一个话题topic发送一条消息或者多条消息 
    def sendmessage(self,topic,msg,key):
        if key == None:
            raise TypeError("sendmessage: key is None!")
        broker = self.get_broker_server_info(key)
        ip,port = self.__get_ip_port(broker)
        if isinstance(msg,str):
            MQTT.publish_one(ip,port,topic,msg)
            return
        if isinstance(msg,list):
            MQTT.publish_many(ip,port,topic,msg)
            return
        raise TypeError("sendmessage: msg type is error!")

    #给订阅了这个topic的所有用户发送一条或者多条消息
    #给对应key，订阅了这个话题的所有用户发送一条或者多条消息
    def broadcast(self,topic,msgs,keys=None):
        if not isinstance(topic,str):
            raise TypeError("broadcast: topic type is error!")
        if keys == None:
            brokerlist = self.get_broker_server_list()
            for broker in brokerlist:
                ip,port = self.__get_ip_port(broker)
                if isinstance(msgs,list):
                    MQTT.publish_many(ip,port,topic,msgs)
                if isinstance(msgs,str):
                    MQTT.publish_one(ip,port,topic,msgs)
            return 
        if isinstance(keys,list):
            brokerlist = self.get_broker_server_by_keys(keys)
            for broker in brokerlist:
                ip,port = self.__get_ip_port(broker)
                if isinstance(msgs,str):
                    MQTT.publish_one(ip,port,topic,msgs)
                if isinstance(msgs,list):
                    MQTT.publish_many(ip,port,topic,msgs)
            return
        raise TypeError("broadcast: keys type is error")

