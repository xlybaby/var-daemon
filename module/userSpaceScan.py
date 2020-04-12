# -*- coding: utf-8 -*-
import sys,os,json,codecs,re,requests,random,threading,time,datetime,math
from multiprocessing import Process

import tornado.ioloop
from tornado import gen
from tornado.tcpclient import TCPClient
from tornado import queues

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.tornado import TornadoScheduler
#parent_path = os.path.dirname(sys.path[0])
#if parent_path not in sys.path:
#  sys.path.append(parent_path)
from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

inq = queues.Queue()

async def sjob(pid,scannerId,startSeq,endSeq):
    conn,cur = PgConnectionPool.getConn()
    try:
        print("job fired...%s,%s,%d,%d"%(pid,scannerId,startSeq,endSeq))
        
        limit = int(ApplicationProperties.configure("application.storage.access.page.limit"))
        offset = 0
        flag = limit
        while flag == limit:
            flag = 1
            cur.execute('SELECT * FROM t_uc_material__%s where status = 2 and materialId >= %d and materialId < %d limit %d offset %d'%(pid,startSeq,endSeq,limit,offset))
            results=cur.fetchall()
            print(results)
            for idx, r in enumerate(results):
                #print(r[2])
                #definition = json.loads(r[2])
                #print("task definition:%s"%(definition))
                type = r[2]["type"]
                addr = r[2]["addr"]
                secondDomain = re.search(r"[A-Za-z0-9\-]+\.com|[A-Za-z0-9\-]+\.edu.cn|[A-Za-z0-9\-]+\.cn|[A-Za-z0-9\-]+\.com.cn|[A-Za-z0-9\-]+\.org|[A-Za-z0-9\-]+\.net|[A-Za-z0-9\-]+\.tv|[A-Za-z0-9\-]+\.vip|[A-Za-z0-9\-]+\.cc|[A-Za-z0-9\-]+\.gov.cn|[A-Za-z0-9\-]+\.gov|[A-Za-z0-9\-]+\.edu|[A-Za-z0-9\-]+\.biz|[A-Za-z0-9\-]+\.net.cn|[A-Za-z0-9\-]+\.org.cn",addr.lower())
                #if type == 1 or type == 9:
                    #todo: send to massCollect
                    #{materialId, seqno, definition}
                    #None
                #else:
                await inq.put({"pid":pid,"materialId":r[0],"uuid":r[1],"type":type,"url":addr,"domain":secondDomain.group(),"definition":r[2]});
                flag=flag+idx
                
            offset=offset+limit
    except Exception as e:
        print(e)
    finally:
        PgConnectionPool.release(conn, cur)        
    
class InfiniteScan(object):     
    
    def __init__(self,p_command):
        #threading.Thread.__init__(self)
        #Process.__init__(self)
        #self._exchangeServerHost = None
        #self._exchangeServerPort = None
        #self._scanner = scanner
        #self._partition = partition
        self._command = p_command
        
    def dashboard(self):
        None
        
    def stop(self):
        None
        
    def resume(self):
        None
        
    async def send(self):
        try:
            stream = await TCPClient().connect(self._exchangeServerHost, self._exchangeServerPort)
            #stream = await TCPClient().connect('127.0.0.1',8888)
            async for record in inq:
                            try:
                                print("Got a task: %s"%(json.dumps(record)))
                                await stream.write((json.dumps(record) + "\n").encode())
                                print("Sent to server:")
                                reply = await stream.read_until(b"\n")
                                print("Response from server:", reply.decode().strip())
                            except Exception as e:
                                print(e)
                            finally:
                                inq.task_done()       
        except Exception as e:
            print(e)
            
            
    def start(self):
        global __g_scheduler
        __g_scheduler = TornadoScheduler()
        #ApplicationProperties.populate(p_command=self._command)
        
        scannerId = ApplicationProperties.configure("scannerId")
        print("Ready initialize scanner(id=%s)"%(scannerId))
        conn,cur = PgConnectionPool.getConn()
        try:
            
            sql = 'SELECT p.pid,p.maxSeq,p.minSeq,p.splitNum,s.intervalSecs FROM t_uc_material_partition_scan s inner join t_uc_material_partition p on s.pid=p.pid where s.scannerId = %s'
            data = (scannerId)
            cur.execute(sql, data)
            results=cur.fetchall()
            print(results)
            record = results[0]
            
            splitNum = record[3]
            maxSeq = record[1]
            minSeq = record[2]
            intervalSecs = int(record[4])
            pid = record[0]
            fragment = math.ceil((maxSeq-minSeq+1)/splitNum)
            sp = minSeq
            for i in range(splitNum):
                fromI = sp
                sp = sp + fragment
                print("add scanner, from %d to %d, interval %d seconds, partition id %s."%(fromI,sp,intervalSecs,pid))
                #memS.insert(startSeq=fromI,endSeq=sp,intervalSecs=__g_properties.getProperty("application.scanner.schedule.intervalSeconds"),scannerId=scannerId,pid=unassiP["pid"])
                __g_scheduler.add_job(sjob,'interval', seconds=intervalSecs, args=(pid,scannerId,fromI,sp))
            __g_scheduler.start()
                    
        #except Exception as e:
        #    print(e)
        finally:
            PgConnectionPool.release(conn, cur)

        self._exchangeServerHost = ApplicationProperties.configure("application.exchange.server.host")
        self._exchangeServerPort = ApplicationProperties.configure("application.exchange.server.port")
        '''
        jobstores = {
            'mongo': {'type': 'mongodb'},
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        }
        executors = {
            'default': {'type': 'threadpool', 'max_workers': 5}
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }
        '''
        '''
        for idx, sche in enumerate(p_schedule):
            sid = sche["scannerId"]+"#"+str(idx)
            _scheduler.add_job(sjob,'interval', seconds=_properties.getProperty("application.scanner.schedule.intervalSeconds"), args=(sche["pid"],sche["tableName"],sid,sche["startSeq"],sche["endSeq"]))
        '''
        
        
        #if q:
        #    q.put_nowait(self)
        #newLoop = tornado.ioloop.IOLoop.current()
        #print(newLoop)
        tornado.ioloop.IOLoop.current().run_sync(self.send)
        #await self.send()
        
if __name__ == '__main__':
    scanner = InfiniteScan({"pid":"p10000","tableName":"aaa","scannerId":"scannerP10000","intervalSecs":10,"startSeq":10000,"endSeq":20000},{'pid': 'p10000', 'maxSeq': 19999, 'minSeq': 10000, 'tableName': 't_uc_material_p10000', 'assigned': True, 'splitNum': 1, 'phase': 1, 'creator': None, 'createTime': None, 'lastUpdateTime': None, '__id__': 1, '__version__': 0})
    scanner.start()
    