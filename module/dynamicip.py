# -*- coding: utf-8 -*-
import sys,os,threading,random,json
from datetime import datetime
from urllib.parse import urlencode, quote

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.web import RequestHandler,Application
from tornado.ioloop import IOLoop

from apscheduler.schedulers.tornado import TornadoScheduler

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
from fileBasedConfiguration import ApplicationProperties
from constants import RequestMapping

http_client = AsyncHTTPClient()
http_header = {
        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36"
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) VAR/1.0.0.1"
    }

# (ip, port, expire, used_times)
global_ip_pool = [([""] * 4) for i in range(1)]
global_ip_dic = {}
ip_dic_lock = threading.Lock()

async def refreship():
    maxtimes=int(ApplicationProperties.configure("application.shared.ippool.maxUseTimes"))
    expireOffset=int(ApplicationProperties.configure("application.shared.ippool.expireOffset"))
    now = datetime.now()
    for idx,rec in enumerate(global_ip_pool):
        expire = rec[2]
        times = rec[3]
        if ((expire - now) / 60 < expireOffset) or (times > maxtimes):
            del(global_ip_pool[idx])
            
async def fetchips():
    flag = int(ApplicationProperties.configure("application.shared.ippool.refreshFlagNum"))
    if len(global_ip_dic) < flag:
        print("job fired, current ip num %d, need min num is %d"%(len(global_ip_dic), flag))
    #for key,values in  global_ip_dic.items():
    #    print ("%s - %s"%(key,values))
        response = await fetch('http://api.wandoudl.com/api/ip?app_key=8fbe2e6d52ee4657e89580276d698abc&pack=210570&num=10&xy=1&type=2&mr=1&')
        print(response)
        if response:
            results = json.loads(response)
            if "data" in results:
                iplist = results["data"]
                for ip in iplist:
                    ts=datetime.strptime(ip["expire_time"], '%Y-%m-%d %H:%M:%S')
                    #global_ip_pool.append([ip["ip"],  ip["port"], ts, 0, ])
                    global_ip_dic[ip["ip"]] = [ip["port"], ts, 0]
                print(global_ip_dic)
                
async def fetch(url):
    print ("async_fetch_url: %s"%(url))
    if not url:
        return
    
    try:
        #response = await http_client.fetch(url,method='GET',headers=http_header,connect_timeout=connectTimeOut,request_timeout=requestTimeOut, validate_cert=False)
        response = await http_client.fetch(url,method='GET',headers=http_header,connect_timeout=5,request_timeout=5,validate_cert=False)
    except Exception as e:
        print("Error: %s" % e)
        return None
    else:
        print("fetched %s" % url)
        return response.body

class IPAssign(RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])

    async def post(self):
        if "action" not in self.args or self.args["action"] != "fetchip":
            for key,values in  global_ip_dic.items():
                print ("%s - %s"%(key,values))
            self.write({"msg":"unknown action"})
            return
        
        while True:
            ips = list(global_ip_dic.keys())
            if len(ips) < 1:
                self.write({"msg":"no ip"})
                break;
            #print("index %d arrived"%(int(self.args["index"])))
            getint = random.randint(1,len(ips))        
            ip = ips[getint-1]  
            maxtimes=int(ApplicationProperties.configure("application.shared.ippool.maxUseTimes"))
            expireOffset=int(ApplicationProperties.configure("application.shared.ippool.expireOffset"))
            now = datetime.now()
            expire = global_ip_dic[ip][1]
            times = global_ip_dic[ip][2]
            if ((expire - now).seconds / 60 < expireOffset) or (times > maxtimes):
                print("ip[%s] is out of work - %s"%(ip,datetime.now()))
                print(global_ip_dic[ip])
                global_ip_dic.pop(ip)
            else:
                global_ip_dic[ip][2] = global_ip_dic[ip][2] + 1
                self.write({"ip":ip, "port":global_ip_dic[ip][0]}) 
                break
        
class IpPool(object):
    def __init__(self):
        None

    def start(self):
        self._scheduler = TornadoScheduler()    
        self._scheduler.add_job(fetchips,'interval', seconds=int(ApplicationProperties.configure("application.shared.ippool.fetchInterval")))
        self._scheduler.start()
        
        print('Application listeming...[Port:%s]'%(ApplicationProperties.configure(p_key='application.shared.ippool.server.port')))
        application = Application([ (RequestMapping.dynamic_ip_assign, IPAssign),])
        application.listen(ApplicationProperties.configure(p_key='application.shared.ippool.server.port'))
       
        #IOLoop.current().start()
        IOLoop.current().run_sync(fetchips)
        IOLoop.current().start()
        
if __name__ == '__main__':
    ApplicationProperties.populate({"config":"/Users/apple/Documents/var/workspace/var-daemon"})

    IpPool().start()
