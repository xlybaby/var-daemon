# -*- coding: utf-8 -*-
import sys,os

import psycopg2
from DBUtils.PooledDB import PooledDB

class ApplicationProperties(object):
    configures = {}
    
    @staticmethod
    def populate(p_command=None):
        if p_command and "config" in p_command:
            project_path = p_command["config"]
        else:
            project_path = sys.path[0]
        
        if( os.path.exists( project_path + '/application.properties' ) ):
            file = open(project_path + '/application.properties', 'r')
            for line in file.readlines():
                pair = line.split("=")
                if len(pair) != 2:
                    continue
                ApplicationProperties.configures[pair[0].strip('\n\r\t ')] = pair[1].strip('\n\r\t ')
        
        if p_command:
            for item in p_command.items():
                ApplicationProperties.configures[item[0]] = item[1]
     
    @staticmethod
    def configure(p_key):      
        return ApplicationProperties.configures[p_key] if p_key in ApplicationProperties.configures else None
    
    def __init__(self, p_command=None):
        self.__properties = {}
        if p_command and "config" in p_command:
            self.__project_path = p_command["config"]
        else:
            self.__project_path = sys.path[0]
        
        if( os.path.exists( self.__project_path + '/application.properties' ) ):
            file = open(self.__project_path + '/application.properties', 'r')
            for line in file.readlines():
                pair = line.split("=")
                if len(pair) != 2:
                    continue
                self.__properties[pair[0].strip('\n\r\t ')] = pair[1].strip('\n\r\t ')
        
        if p_command:
            for item in p_command.items():
                self.__properties[item[0]] = item[1]
            
    def getProperty(self, p_key):
        return self.__properties[p_key] if p_key in self.__properties else None
    
    def list(self):
        for item in self.__properties.items():
            print( "%s=%s" % (item[0], item[1]) )
            
        
class PgConnectionPool(object):
    pool = None
    
    @staticmethod
    def ini(host,port,user,password,database):
        PgConnectionPool.pool = PooledDB(
                                                    creator=psycopg2,  
                                                    maxconnections=3,  
                                                    mincached=1,  
                                                    maxcached=2,  
                                                    maxshared=2,  
                                                    blocking=True,  
                                                    maxusage=None,  
                                                    setsession=[],  
                                                    ping=0,
                                                    host=host,
                                                    port=port,
                                                    user=user,
                                                    password=password,
                                                    database=database
                                                    )
    @staticmethod
    def getConn():
        conn = PgConnectionPool.pool.connection()
        cursor = conn.cursor()
        return conn,cursor
    
    @staticmethod
    def release(conn, cur):
        if conn:
            conn.close()
        if cur:
            cur.close()
            
    def __init__(self, p_properties=None):
        self._pool = PooledDB(
                                                    creator=psycopg2,  
                                                    maxconnections=3,  
                                                    mincached=1,  
                                                    maxcached=2,  
                                                    maxshared=2,  
                                                    blocking=True,  
                                                    maxusage=None,  
                                                    setsession=[],  
                                                    ping=0,
                                                    host=p_properties.getProperty("application.storage.postgres.connection.host"),
                                                    port=p_properties.getProperty("application.storage.postgres.connection.port"),
                                                    user=p_properties.getProperty("application.storage.postgres.connection.user"),
                                                    password=p_properties.getProperty("application.storage.postgres.connection.password"),
                                                    database=p_properties.getProperty("application.storage.postgres.connection.database")
                                                    )

    def getConnection(self):
        conn = self._pool.connection()
        cursor = conn.cursor()
        return conn,cursor
    
    def close(self, conn, cur):
        if conn:
            conn.close()
        if cur:
            cur.close()
            