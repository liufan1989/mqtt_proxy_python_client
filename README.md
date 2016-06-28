# mqtt_proxy_python_client
A simple push message system android app or ios app can establish tcp connection with one of mqtt broker processes then using python api can push message to app through mqtt broker process mqtt python client is managed multiple mqtt（mosquitto）processes, useing redis for save map relationship!


* apt-get install redis-server 
* pip install redis
* pip install hash-ring
* pip install tornado
* git clone https://github.com/eclipse/paho.mqtt.python   [http://www.eclipse.org/paho/downloads.php]
* pip install org.eclipse.paho.mqtt.python-1.1.tar.gz

##version 1.0: 借助key来实现消息的分发
###这是个简易推送系统，使用redis保存两个映射关系:
* 用户key和用户被分配到的那个mqtt broker进程的ip和地址
* 这个mqtt进程中保存了与该进程建立tcp连接的所有用户key列表

* usage:
1. GET方法通过这个URL: [http://192.168.1.163:7890/getbroker?key=13609183715] 通过key获取对应的一个broker服务的ip和port
2. mqtt client 连接这个broker服务,并订阅相关主题, mosquitto_sub -h ip -p port -t game (安装mosquitto)
3. POST方法 http://192.168.1.163:7890/sendmessage?key=13609183715&topic=game&message=nihao! 发送消息nihao!到订阅了game这个主题且连接对应key所在的broker服务的mqtt client端
4. POST方法 http://192.168.1.163:7890/broadcast?topic=game&message=dajiahao! 发送消息:dajiaohao!到所有订阅了这个主题game的所有mqtt client端




