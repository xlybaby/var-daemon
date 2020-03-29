# -*- coding: utf-8 -*-
import sys,os,json,codecs,re,math
import subprocess
import time
import datetime
from multiprocessing import Process

import urllib3
import urllib3.contrib.pyopenssl
import certifi

import tornado.ioloop
import tornado.web
from tornado import gen, httpclient, ioloop, queues

from pydblite import Base

from fileBasedConfiguration import ApplicationProperties, PgConnectionPool
from module.userSpaceScan import InfiniteScan
from constants import RequestMapping

nodeQueue = queues.Queue()
__g_userSpace = None
__g_memDB = None

class MemDB(object):
    def __init__(self):
        self._pdb = Base('centric.partition')
        self._sdb = Base('centric.scanner')
        self._pdb.create('pid','maxSeq','minSeq','tableName','assigned','splitNum','phase','creator','createTime','lastUpdateTime',mode="override")
        self._sdb.create('startSeq','endSeq','intervalSecs','scannerId','pid','creator','createTime','lastUpdateTime','url','port',mode="override")
        
    def db(self):  
        return self._pdb, self._sdb
    

async def worker(idx):
    async for task in nodeQueue:
                    try:
                        None
                    except Exception:
                        traceback.print_exc()
                    finally:
                        nodeQueue.task_done()

class DashBoardHandler(tornado.web.RequestHandler):
    async def get(self):
        None
        
class RegisterHandler(tornado.web.RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])

    async def post(self):
        event = self.args["event"]
        if event == "register":
            memP, memS = __g_memDB.db()
            unassigned = memP(assigned=False)
            if len(unassigned)>0 :
                partition = unassigned[0]
                scanner = {"intervalSecs":__g_properties.getProperty("application.scanner.schedule.intervalSeconds"),"scannerId":None,"pid":partition["pid"]}
                #p = Process(target=InfiniteScan,args=(scanner,partition)) 
                #p.start() 
        

class UserSpace(object):
    def __init__(self, p_memDB,p_persistent = 'database', p_mode='new'):
        #self._properties = p_properties
        self._mode = p_mode
        self._memDB = p_memDB
        self._persistent = p_persistent
        #self._pgpool = p_pgpool
        
    def init(self):
        if self._mode == 'new':
            if self._persistent == 'database':
                self.iniFromDB()
                self.gen()
            #memP, memS = self._memDB.db()
            #for s in memS:
            #    print(s)
            #print(memP)
            #print(memS)
            
            
    def modifyPartition(self, p_new_scanners, p_target_partition):
            scannerId = p_node["nodeId"]
            insertSeq = p_node["insertSeq"]
            tarpid = p_node["targetPartition"]
            
            if len(memP(pid=tarpid)) > 0:
                pa = memP(pid=tarpid)[0]
                splitNum = pa["splitNum"]
                recs = [ r for r in memS if insertSeq > r['startSeq'] and r['endSeq'] >= insertSeq and r['pid'] == tarpid]
                if len(recs) >0:
                    orig = recs[0]
                    origFrom = orig['startSeq']
                    origEnd = orig['endSeq']
                    memS.insert(startSeq=insertSeq,endSeq=origEnd,intervalSecs=_properties.getProperty("application.scanner.schedule.intervalSeconds"),scannerId=scannerId,pid=tarpid)
                    memS.update(memS[orig['__id__']], endSeq=insertSeq)
            else:
                return {"msg":"No such partition", "data":None}
    
    def  notifyScanner(self, p_scanners):
        None
        
    #one scanner process per partition
    def gen(self):
            memP, memS = self._memDB.db()
            
            for s in memS:
                pid = s["pid"]
                partition = memP(pid=pid)[0]
                print(partition)
                #p = Process(target=InfiniteScan,args=(s,partition,)) 
                #p.start() 
                #scanner = InfiniteScan(s,partition)
                #scanner.start()
                #print(sys.path[0])
                subprocess.run(sys.path[0]+"/daemon.sh userSpaceScan -scannerId="+str(s["scannerId"]),shell=True)
                print("generate a new scanner.")
        
    def iniFromDB(self):
        memP, memS = self._memDB.db()
        conn,cur = PgConnectionPool.getConn()
        try:
            cur.execute('SELECT * FROM t_uc_material_partition')
            results=cur.fetchall()
            for r in results:
                print(r)
                memP.insert(pid=r[0],maxSeq=r[1],minSeq=r[2],tableName=r[3],assigned=r[4],splitNum=r[5],phase=r[6])
            
            cur.execute('SELECT * FROM t_uc_material_partition_scan')
            results=cur.fetchall()
            for r in results:
                print(r)
                memS.insert(startSeq=r[0],endSeq=r[1],intervalSecs=int(r[2]),scannerId=r[3],pid=r[4])
        except Exception as e:
            print(e)
        finally:
            PgConnectionPool.release(conn, cur)
    
class CentricHost(object):        
    def __init__(self, p_command = None):
        self._command = p_command
        #self._sysproperties = ApplicationProperties(p_command)
        #self._userSpace = UserSpace(self._sysproperties,)
        

    def start(self):
        global __g_userSpace
        global __g_memDB
        
        __g_memDB = MemDB()
        __g_userSpace = UserSpace(__g_memDB)
        
        application = tornado.web.Application([ (RequestMapping.register_node, RegisterHandler),
                                                                                   (RequestMapping.fetch_dashboard, DashBoardHandler), ])
        application.listen(ApplicationProperties.configure(p_key='application.controller.register.server.port'))
        print('Application listeming...[Port:%s]'%(ApplicationProperties.configure(p_key='application.controller.register.server.port')))

        workers = gen.multi([worker(idx) for idx in range(1)])
        
        __g_userSpace.init()
        
        ioLoop = tornado.ioloop.IOLoop.current()
        #print(ioLoop)
        ioLoop.start()
        