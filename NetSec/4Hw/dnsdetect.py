#!/usr/bin/env python
import sys, getopt
from scapy.all import *
from collections import deque
import netifaces

dnsResponseBuffer = deque(maxlen=20)
def getDefaultIP():
	return "172.24.224.78"

def getDefaultInterface():
	defaultInterface = netifaces.gateways()['default'][netifaces.AF_INET][1]
	return defaultInterface

def decode(byt):
	stg=byt.decode("utf-8")
	return stg

def isTypeA(pkt):
	'''
		repr(pkt[DNS]===========> 
		<DNS  id=47602 qr=1 opcode=QUERY aa=0  tc=0 rd=1 ra=1 z=0 ad=0 cd=0 rcode=ok qdcount=1 ancount=4 nscount=0 
		arcount=0 qd=<DNSQR  qname='bbc.com.' qtype=A qclass=IN |> an=<DNSRR  rrname='bbc.com.' type=A rclass=IN ttl=299 
		rdata='212.58.246.79' |<DNSRR  rrname='bbc.com.' type=A rclass=IN ttl=299 rdata='212.58.244.23' |<DNSRR  
		rrname='bbc.com.' type=A rclass=IN ttl=299 rdata='212.58.246.78' |<DNSRR  rrname='bbc.com.' type=A rclass=IN 
		ttl=299 rdata='212.58.244.22' |>>>> ns=None ar=None |>
	'''
	tempStg=repr(pkt[DNS])
	if("type=A" in tempStg):
		return True

	return False


def spoofDNS(pkt):
	if(pkt.haslayer(DNS) and pkt.haslayer(DNSRR)):
		# print("pkt[DNS].id =>",pkt[DNS].id,", pkt[DNS].qr=>",pkt[DNS].qr,", pkt[IP].src => ",pkt[IP].src,", element[IP].dst=>",pkt[IP].dst," pkt[DNS].id=>",pkt[DNS].id,"pkt[DNSRR].rdata=>",pkt[DNSRR].rdata)
		# print("\n\n\****************")
		# print("pkt[DNS].qd.qtype => ",pkt[DNS].qd.qtype)
		#print("element[DNS].id =>",element[DNS].id,", element[DNS].qr=>",element[DNS].qr,", element[IP].src => ",element[IP].src,", element[IP].dst=>",element[IP].dst," element[DNS].id=>",element[DNS].id,"element[DNSRR].rdata=>",element[DNSRR].rdata)

		if(pkt[DNS].qr == 1):
			if len(dnsResponseBuffer)>0:
				for element in dnsResponseBuffer:
					if isTypeA(pkt) == True and isTypeA(element) == True and element[IP].src == pkt[IP].src and\
					element[IP].dst == pkt[IP].dst and\
					element[IP].sport == pkt[IP].sport and\
					element[IP].dport == pkt[IP].dport and\
					element[DNS].qd.qname == pkt[DNS].qd.qname and\
					element[DNS].id == pkt[DNS].id and\
					element[DNSRR].rdata != pkt[DNSRR].rdata and\
					element[IP].payload != pkt[IP].payload:
						pktHostname=element[DNS].qd.qname.decode("utf-8").rstrip(".")
						# print("##################################################")
						# print("element[IP].src => ",element[IP].src,", element[IP].dst=>",element[IP].dst," element[DNS].id=>",element[DNS].id,"qd=>",decode(element[DNS].qd.qname),", element[DNSRR].rrname=>",element[DNSRR].rrname,"element[DNSRR].rdata=>",element[DNSRR].rdata)
						# print("*********************")
						# print("pkt[IP].src => ",pkt[IP].src,", element[IP].dst=>",pkt[IP].dst," pkt[DNS].id=>",pkt[DNS].id,"qd=>",decode(pkt[DNS].qd.qname),", pkt[DNSRR].rrname=>",pkt[DNSRR].rrname,", pkt[DNSRR].rdata=>",pkt[DNSRR].rdata)
						print("DNS poisoning attempt detected")
						print("TXID ",element[DNS].id," Request URL ",pktHostname)
						print("Answer1 [%s]"%element[DNSRR].rdata)
						print("Answer2 [%s]"%pkt[DNSRR].rdata)
			dnsResponseBuffer.append(pkt)
		# print("dnsResponseBuffer =>",dnsResponseBuffer)

def getArgs(argv):
	interface=None
	traceFilePath=None
	bfpExpr=None
	try:
  		opts, args = getopt.getopt(argv,"i:r:",["interface=","tracefile="])
	except getopt.GetoptError:
  		print('inject.py -i <interface> -r <path_to_hostname_file>') 
  		sys.exit(2)
	for opt, arg in opts:
		if opt in ("-i", "--interface"):
			interface = arg
		elif opt in ("-r", "--file"):
			traceFilePath = arg
	if len(args) == 1:
		bfpExpr = args[0]
	return interface,traceFilePath,bfpExpr


def main(argv):
	interface,traceFilePath,bfpExpr=getArgs(argv)
	print("interface =>",interface,", traceFilePath=>",traceFilePath,", bfpExpr=>",bfpExpr)
	if(bfpExpr == None):
		bfpExpr='udp port 53'
	if(interface == None):	
		interface=getDefaultInterface()
	print("bfpExpr =>",bfpExpr,", interface=>",interface)
	if(traceFilePath!=None):
		sniff(filter=bfpExpr, offline = traceFilePath, store=0, prn=spoofDNS)
	else:
		sniff(filter=bfpExpr, iface=interface, store=0, prn=spoofDNS)


if __name__ == '__main__':
	main(sys.argv[1:])








