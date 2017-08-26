#!/usr/bin/env python
# coding=utf-8
# FileName: monitor_mongo.py
# auth    : By Eter
# date    : 2017-01-10
#
#==============================================================
#Info:
#23333:PRIMARY> info.wiredTiger.connection
#{
#        "auto adjusting condition resets" : 7922,
#        "auto adjusting condition wait calls" : 504942,
#        "files currently open" : 40,
#        "memory allocations" : 8007002,
#        "memory frees" : 7319138,
#        "memory re-allocations" : 564283,
#        "pthread mutex condition wait calls" : 1326192,
#        "pthread mutex shared lock read-lock calls" : 2138484,
#        "pthread mutex shared lock write-lock calls" : 238456,
#        "total fsync I/Os" : 39253,
#        "total read I/Os" : 5600,
#        "total write I/Os" : 55043
#}
#==============================================================
#Usage:
#
# def wiredTigerItems():
#     items = ['connection->memory allocations',
#              'connection->memory frees',
#              'connection->re-allocations',
#             ]
#     return(items)
#
#==============================================================
import os
import sys
import optparse
import ConfigParser
from pymongo import MongoClient
from json import dumps,loads

def Zabbix_Sender():
    zabbix_sender='/usr/bin/zabbix_sender'
    if os.path.exists(zabbix_sender):
        return(zabbix_sender)

    else:
        print("[error]: {}".format("ERROR:Can't find zabbix_sender,EXIT!"))
        sys.exit(1)

def zabbixSenderData(zabbix_sender, zabbix_server, hostname, key_name, key_value):
    try:
        keyName='FromDual.MySQL.mongo.'+key_name
        cmd='%s --zabbix-server=%s --host=%s --key=%s --value=%s' % (zabbix_sender,
                zabbix_server,hostname, keyName, key_value)
        ret=os.system(cmd)

    except Exception as err:
        print("[error]: {}".format(err.message))
        return(-1)

def sendDataToMongo(conn, hostname, keyName, keyValue):
    try:
        client = conn

    except Exception as err:
        print("[error]: {}".format(err.message))

def tojson(data):
    try:
        jdata = dumps(data, sort_keys=True, indent=2)
        return jdata
    except Exception as err:
        print("[error]: {}".format(err.message))

def todata(data):
    try:
        data = loads(data)
        return(data)
    except Exception as err:
        print("[error]: {}".format(err.message))

def mongoConn(host, port, replset):
    try:
        URI = "mongodb://test:test@%s:%s/admin?replicaSet=%s&serverselectiontimeoutms=1000" % (host, port, replset)
        client = MongoClient(URI)
        return client

    except Exception as err:
        print("[error]: {}".format(err.message))

def mongoNoAuthConn(host, port, replset):
    try:
        URI = "mongodb://%s:%s/admin?replicaSet=%s&serverselectiontimeoutms=1000" % (host, port, replset)
        client = MongoClient(URI)
        return client

    except Exception as err:
        print("[error]: {}".format(err.message))

def getServerStatus(client):
    try:
        info = client.admin.command("serverStatus")
        return(info)

    except Exception as err:
        print("[error]: {}".format(err.message))

def getReplSetStatus(client):
    try:
        info = client.admin.command("replSetGetStatus")
        return(info)
    except Exception as err:
        print("[error]: {}".format(err.message))

def getServerStatusItem(info, item):
    try:
        itemInfo= tojson(info.get(item))
        return(itemInfo)

    except Exception as err:
        print("[error]: {}".format(err.message))

def parameterItems():
    items = ['opcounters',
             'network',
             'connections',
             'mem',
             'opcountersRepl',
             'metrics',
             'wiredTiger',
             'globalLock',
             'extra_info',
             ]
    return(items)


def opcountersItems():
    items = ['insert',
             'query',
             'update',
             'delete',
             'getmore',
             'command',
             ]
    return(items)

def opcountersReplItems():
    items = ['insert',
             'query',
             'update',
             'delete',
             'getmore',
             'command',
             ]
    return(items)

def networkItems():
    items = ['bytesIn',
             'bytesOut',
             'numRequests',
             ]
    return(items)

def connectionsItems():
    items = ['current',
             'available',
             'totalCreated',
             ]
    return(items)

def memItems():
    items = ['bits',
             'resident',
             'virtual',
             'mapped',
             'mappedWithJournal',
             ]
    return(items)


def globalLockItems():
    items = ['currentQueue->total',
             'currentQueue->readers',
             'currentQueue->writers',
             'activeClients->total',
             'activeClients->readers',
             'activeClients->writers',
             ]
    return(items)

def wiredTigerItems():
    items = ['cache->bytes belonging to page images in the cache',
             'cache->bytes currently in the cache',
             'cache->bytes not belonging to page images in the cache',
             'cache->bytes read into cache',
             'cache->bytes written from cache',
             'cache->maximum bytes configured',
             'cache->overflow pages read into cache',
             'cache->overflow values cached in memory',
             'cache->pages read into cache',
             'cache->pages written from cache',
            ]
    return(items)

def metricsItems():
    items = ['cursor->timedOut',
             'cursor->open->total',
             'cursor->open->pinned',
             'queryExecutor->scanned',
             'queryExecutor->scannedObjects',
            ]
    return(items)


def extra_infoItems():
    items = ['page_faults',
             'heap_usage_bytes',
            ]
    return(items)

def peculiarPortList():
    list = [7111,
            7112,
           ]
    return(list)

def aliveCheck(info):
    values={}
    if getServerStatusItem(info,'ok'):
        values['aliveCheck'] = getServerStatusItem(info, 'ok')
        return(values)
    else:
        values['aliveCheck'] = 0
        return(values)

def getReplRole(info, host):
    try:
        for i in info.get('members'):
            if i.get('name').split(':')[0] == host:
                return i.get('state')
    except Exception as err:
        print("[error]: {}".format(err.message))
        return(-1)

def getMonitorVariables(info, item, mItems):
    try:
        values   = {}
        itemsList= []
        status  = todata(getServerStatusItem(info, item))
        for mItem in mItems:
            itemsList = mItem.split('->')
            rpmItem   = mItem.replace(' ', '_').replace('->', '_')
            if len(itemsList)   == 1:
                values[item+'_'+rpmItem] = status.get(itemsList[0])

            elif len(itemsList) == 2:
                values[item+'_'+rpmItem] = status.get(itemsList[0]).get(itemsList[1])

            elif len(itemsList) == 3:
                values[item+'_'+rpmItem] = status.get(itemsList[0]).get(itemsList[1]).get(itemsList[2])

            elif len(itemsList) == 4:
                values[item+'_'+rpmItem] = status.get(itemsList[0]).get(itemsList[1]).get(itemsList[2]).get(itemsList[3])

            else:
                print("{}".format("Item has input error."))
    except:
         pass

    return(values)

def readFromConfigFile():
    infoList={}
    return(infoList)

def getOptions():
    try:
        usage = """usage: %prog [-c|--config] FromDualagent_mongo.conf"""
        parser = optparse.OptionParser(usage=usage)
        parser.add_option(
                "-c",
                "--config",
                dest="configFile",
                action="store",
                default="NULL",
                help="-c | --config FromDualagent_mongo.conf"
                )
        (options, args) = parser.parse_args()
        if len(sys.argv) >= 2 and os.path.isfile(options.configFile):
            return(options)
        else:
            print("[error]: {}".format("Input error Or ConfigFile not exists"))
            sys.exit(1)

    except Exception as err:
        print("[error]: {}".format(err.message))

def getAllSections(zbxCfgFile):
    configPar=ConfigParser.ConfigParser()
    configPar.read(zbxCfgFile)
    return(configPar)

def getZabbixServer():
    has_default=cf.has_section('default')
    if has_default:
        ZabbixServer=cf.get('default', 'ZabbixServer')
        return(ZabbixServer)
        if ZabbixServer.strip()=="":
            print 'ERROR:Not Found ZabbixServer,EXIT!'
            sys.exit(1)
    else:
        print "ERROR:Can't find default section from zbxCfgFile,EXIT!"
        sys.exit(1)

if __name__ == '__main__':
    """
    DESC:
        Main()
    """
    try:
        ops=getOptions()
        zabbixSender=Zabbix_Sender()
        cf=ConfigParser.ConfigParser()
        cf.read(ops.configFile)
        has_default=cf.has_section('default')
      

        if has_default:
            ZabbixServer=cf.get('default','ZabbixServer')
            if ZabbixServer.strip()=="":
                print 'ERROR:Not Found ZabbixServer,EXIT!'
                sys.exit(1)
        else:
            print "ERROR:Can't find default section from zbxCfgFile,EXIT!"
            sys.exit(1)

        allSections=cf.sections()
        for section in allSections:
            configVariables={}
            if section == 'default':
                continue
            Modules=cf.get(section,'Modules')
            mongoServer=cf.get(section,'MongoHost')
            mongoPort=cf.getint(section,'MongoPort')
            mongoReplSet=cf.get(section,'ReplSet')
            hostName=section[:]
            hostRole=hostName.split('_')[0]

            # Make Connection
            if mongoPort in peculiarPortList():
                try:
                    conn=mongoNoAuthConn(mongoServer, mongoPort, mongoReplSet)
                    info = getServerStatus(conn)
                    rsinfo = getReplSetStatus(conn)
                except Exception as err:
                    break
            else:
                try:
                    conn=mongoConn(mongoServer, mongoPort, mongoReplSet)
                    info = getServerStatus(conn)
                    rsinfo = getReplSetStatus(conn)
                except Exception as err:
                    break

            ###　aliveCheck
            aliveItem=aliveCheck(info)
            for k,v in aliveItem.items():
                if v in ['1.0', '1', 1, 1.0]:
                    ret=zabbixSenderData(zabbixSender, ZabbixServer, hostName, k, v)
                else:
                    ret=zabbixSenderData(zabbixSender, ZabbixServer, hostName, k, v)
            try:
                mongoRole = getReplRole(rsinfo, mongoServer)
                ret = zabbixSenderData(zabbixSender, ZabbixServer, hostName, 'mongoRole', mongoRole)
            except Exception as err:
                print("[error]: {}".format(err.message))

            monitorVariables = {}
            items = parameterItems()
            if items:
                for item in items:
                    try:
                        monitorVariables.update(getMonitorVariables(info, item, globals()['%sItems'%item]()))
                    except:
                        pass

                for k,v in monitorVariables.items():
                    ret=zabbixSenderData(zabbixSender, ZabbixServer, hostName, k, v)

            else:
                print("[error:] {}".format("Cannot get the items."))


    except Exception as err:
        print("[error]: {}".format(err.message))
        exit(1)
