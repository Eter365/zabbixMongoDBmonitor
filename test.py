
import os
from pymongo import MongoClient

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

def getReplSetStatus(client):
    try:
        info = client.admin.command("replSetGetStatus")
        return(info)
    except Exception as err:
        print("[error]: {}".format(err.message))

def getReplRole(info, host):
    for i in info.get('members'):
		if i.get('name').split(':')[0] == host:
			return i.get('stateStr')

def zabbixSenderData(zabbix_sender, zabbix_server, hostname, key_name, key_value):
    try:
        keyName='FromDual.MySQL.mongo.'+key_name
        cmd='%s --zabbix-server=%s --host=%s --key=%s --value=%s' % (zabbix_sender,
                zabbix_server,hostname, keyName, key_value)
        ret=os.system(cmd)

    except Exception as err:
        print("[error]: {}".format(err.message))
        return(-1)			
	
if __name__ == '__main__':
	IPLIST = ['10.0.0.16', '10.0.0.209', '10.0.0.205']
	for ip in IPLIST:
		client = mongoConn(ip, 7132, 7132)
		info = getReplSetStatus(client)
		role = getReplRole(info, ip)
		print("IPaddr: {} Role: {}".format(ip, role))
	
