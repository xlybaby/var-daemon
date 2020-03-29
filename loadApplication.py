# -*- coding: utf-8 -*-
import sys,os,time,shutil

from module.centricControl import CentricHost
from module.userSpaceScan import InfiniteScan
from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

if __name__ == '__main__':
    props={}
    if len(sys.argv) > 1:
        for idx in range(1, len(sys.argv)):
            arg = sys.argv[idx]
            if arg.startswith("-") :
                prop = arg[1:]
                pair = prop.split("=")
                props[pair[0]]=pair[1]
    
    print(props)            
    #Main( p_command=props )    
    module=None
    if "module" in props:
        ApplicationProperties.populate(props)
        PgConnectionPool.ini(host=ApplicationProperties.configure("application.storage.postgres.connection.host"),
                                                    port=ApplicationProperties.configure("application.storage.postgres.connection.port"),
                                                    user=ApplicationProperties.configure("application.storage.postgres.connection.user"),
                                                    password=ApplicationProperties.configure("application.storage.postgres.connection.password"),
                                                    database=ApplicationProperties.configure("application.storage.postgres.connection.database"))

        module = props["module"]
        print("Run application's module [%s]"%(module))       
        if module == "centricControl":
            CentricHost(p_command=props).start()
        elif module == "userSpaceScan":
            InfiniteScan(p_command=props).start()
        else:
            print("Unknow module [%s]"%(module))
    else:
        print("No module identified")