#include <stdio.h>
#include <pcap.h>
#include <netinet/if_ether.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <time.h>
#include <netinet/ether.h>
#include <stdint.h>

char *searchPattern=NULL;
void createPayloadString(char *dest,const u_char *src, int len){
	for (int i = 0; i < len; ++i){
		if (isprint(*src)){
			*dest = *src;
		}else{
			*dest = '.';
		}
		src++;
		dest++;
	}
}

void printSourceDestMacAddresses(u_char *srcMac,u_char *destMac){
	int ind = ETHER_ADDR_LEN;
	while(ind>0){
		printf("%s%02x",(ind == ETHER_ADDR_LEN) ? " " : ":",*srcMac++);
		ind--;
	}
	printf(" -> ");
    ind = ETHER_ADDR_LEN;
    while(ind>0){
		printf("%s%02x",(ind == ETHER_ADDR_LEN) ? " " : ":",*destMac++);
		ind--;
	}
}

void printHexAndLine(const u_char *payload, int len, int offset)
{
	int i;
	int gap;
	const u_char *ch;

	/* offset */
	printf("%05d   ", offset);
	
	/* hex */
	ch = payload;
	for(i = 0; i < len; i++) {
		printf("%02x ", *ch);
		ch++;
		/* print extra space after 8th byte for visual aid */
		if (i == 7)
			printf(" ");
	}
	/* print space to handle line less than 8 bytes */
	if (len < 8)
		printf(" ");
	
	/* fill hex gap with spaces if not full line */
	if (len < 16) {
		gap = 16 - len;
		for (i = 0; i < gap; i++) {
			printf("   ");
		}
	}
	printf("   ");
	/* ascii (if printable) */
	ch = payload;
	for(i = 0; i < len; i++) {
		if (isprint(*ch))
			printf("%c", *ch);
		else
			printf(".");
		ch++;
	}
	printf("\n");
return;
}

int searchInPayloadString(const char *string,const char *pat){
	char *f = strstr(string,pat);
	// printf("inside searchInPayloadString string : %s =>\npat : %s\n result of FP : \n\n", string,pat,f);
	if(f!=NULL){
		// printf("Found in searchInPayloadString %s  at location  %s\n", string,f);
		return 1;
	}
	return 0;
}


//IP_HEADER_LENGTH,sourceAddr,destAddr ,protocol,sourcePort,destPort,tcpHeaderLength);
void print_payload(char *timestamp,u_char *sourceMac,u_char *destMac,int type,
	int headerLength, char *sourceAddr, char *destAddr, char *protocol, int sourcePort,
	int destPort,const u_char *payload, int len)
{
	int len_rem = len;
	char dest[10000]; //payload string pointer
	const u_char *ch = payload;
	createPayloadString(dest,ch,len);
	// printf("dest inside payload is %s\n\n\n",dest);
	if(searchPattern!=NULL && (!searchInPayloadString(dest,searchPattern))){
		return;
	}

	/*****************Printing packet details*******************/
	printf("%s ",timestamp);
	printSourceDestMacAddresses(sourceMac,destMac);
	printf(" type 0x%04x len %d %s:%d -> %s:%d %s\n",type,headerLength,sourceAddr,sourcePort,destAddr,destPort ,protocol);
	/***********************************************************/
	// return;
	int line_width = 16;			/* number of bytes per line */
	int line_len;
	int offset = 0;					/* zero-based offset counter */
	ch = payload;

	/* data fits on one line */
	if (len <= line_width) {
		printHexAndLine(ch, len, offset);
		return;
	}
	
	/* data spans multiple lines */
	for ( ;; ) {
		/* compute current line length */
		line_len = line_width % len_rem;
		/* print line */
		printHexAndLine(ch, line_len, offset);
		/* compute total remaining */
		len_rem = len_rem - line_len;
		/* shift pointer to remaining bytes to print */
		ch = ch + line_len;
		/* add offset */
		offset = offset + line_width;
		/* check if we have line width chars or less */
		if (len_rem <= line_width) {
			/* print last line and get out */
			printHexAndLine(ch, len_rem, offset);
			break;
		}
	}
return;
}

void handle_packets_callback(u_char *args, const struct pcap_pkthdr *packetHeader,const u_char *packet){
	
	struct timeval* timestampHdr = (struct timeval*)packetHeader;
	struct ether_header *ethernet = (struct ether_header *)packet;
	char sourceAddr[30];
	char destAddr[30];
	char *protocol;
	int protocolHeaderLength=0;
	int ipSourcePort=0;
	int ipDestPort=0;
	int size_payload;
	int ETHERNET_PACKET_LENGTH = sizeof(struct ether_header);	
	char tim[80];
	int IP_HEADER_LENGTH;
	struct ip *ip;
	struct tm ts;
	u_char *payload;
	int packetCapLength;
	packetCapLength=(*packetHeader).caplen;

	//finding out the timestamp from ether_header in the correct form
	ts = *localtime(&(timestampHdr->tv_sec));
	strftime(tim, sizeof(tim), "%Y-%m-%d %H:%M:%S", &ts);
	int millis = (int)timestampHdr->tv_usec;
	char timMillis[100];
	sprintf(timMillis, "%06d", millis);
	strcat(tim,".");
	strcat(tim,timMillis);
	u_char *sourceMac=(u_char *)&ethernet->ether_shost;
	u_char *destMac=(u_char *)&ethernet->ether_dhost;
	int type = ntohs(ethernet->ether_type);
    
	if(ntohs(ethernet->ether_type) == ETHERTYPE_IP) {
		//adding ethernet packet length to move pointer to ip header field.
		ip	= (struct ip*)(packet + ETHERNET_PACKET_LENGTH);
		IP_HEADER_LENGTH = ip->ip_hl* 4;

		memcpy(sourceAddr,inet_ntoa(ip->ip_src),sizeof(char)*30);
		memcpy(destAddr,inet_ntoa(ip->ip_dst),sizeof(char)*30);
		
		if(ip->ip_p == 6){
			protocol = "TCP";
			struct tcphdr *tcp = (struct tcphdr*)(packet+ETHERNET_PACKET_LENGTH+IP_HEADER_LENGTH);
			ipSourcePort = ntohs(tcp->th_sport);
			ipDestPort = ntohs(tcp->th_dport);
			protocolHeaderLength = ((*((uint16_t *)(tcp + 12)) & 0xf0)>>4)*4;
		}else if(ip->ip_p == 17){
			protocol = "UDP";
			struct udphdr *udp = (struct udphdr*)(packet+ETHERNET_PACKET_LENGTH+IP_HEADER_LENGTH);
			ipSourcePort = ntohs(udp->uh_sport);
			ipDestPort = ntohs(udp->uh_dport);
			protocolHeaderLength = 8;
		}else if(ip->ip_p == 1){
			protocol = "ICMP";
			struct icmphdr *icmp = (struct icmphdr*)(packet+ETHERNET_PACKET_LENGTH+IP_HEADER_LENGTH);
			protocolHeaderLength = 8;
		}else{
			protocol = "OTHER";
		}
		payload = (u_char *)(packet+ETHERNET_PACKET_LENGTH+IP_HEADER_LENGTH+protocolHeaderLength);
		size_payload = packetCapLength - (ETHERNET_PACKET_LENGTH+IP_HEADER_LENGTH + protocolHeaderLength);
		
		print_payload(tim,sourceMac,destMac,type,packetCapLength,sourceAddr,destAddr,protocol,ipSourcePort,ipDestPort, payload, size_payload);
	}else{
		if(searchPattern==NULL){
			printf(" %s ",tim);
			protocol = "OTHER";
			printSourceDestMacAddresses(sourceMac,destMac);
			printf(" type 0x%04x %d %s\n",type,packetCapLength,protocol);
		}
	}	
} 



// #define PCAP_ERRBUF_SIZE 100
int main(int argc, char *argv[])
{
	char errbuf[PCAP_ERRBUF_SIZE];
	char *device;
	struct bpf_program fp;	//Filter expression
	char filter_exp[100];
	bpf_u_int32 mask;		/* The netmask of our sniffing device */
 	bpf_u_int32 net;		/* The IP of our sniffing device */
 	int num_packets=-1;
 	int val;
 	char *fileLoc=NULL;
 	pcap_t *sniffSession;

 	while ((val = getopt (argc, argv, "i:r:s:")) != -1){
 		switch(val){
 			case 'r':
				fileLoc=optarg;
				break;
 			case 'i':
 				device=optarg;
 				break;
			case 's': 
				searchPattern=optarg;
				break;				
 		}
 	}
 	for (int index = optind; index < argc; index++){
		strcpy(filter_exp,argv[index]);
		break;
	}
 	
	if(device==NULL && fileLoc==NULL){
		device = pcap_lookupdev(errbuf);
		if (device == NULL) {
		fprintf(stderr, "Couldn't find default device: %s\n", errbuf);
		return(2);
		}
		printf("device interface : %s\n",device );
	}

	if(fileLoc!=NULL){
		/* Open a capture file */
	    if ( (sniffSession = pcap_open_offline(fileLoc, errbuf) ) == NULL)
	    {
	        fprintf(stderr,"\nError while opening dump file : %s\n",fileLoc);
	        return -1;
	    }
	}else{
		if (pcap_lookupnet(device, &net, &mask, errbuf) == -1) {
			fprintf(stderr, "Can't get netmask for device %s\n", device);
			net = 0;
			mask = 0;
	 	}
	 	printf("Device: Ip: Mask: %s -> %u -> %u -> \n", device,net,mask);
		sniffSession = pcap_open_live(device, BUFSIZ, 1, 1000, errbuf);
	}
	
	if (sniffSession == NULL) {
		fprintf(stderr, "Couldn't open device %s: %s\n", device, errbuf);
		return(2);
 	}

 	if (pcap_datalink(sniffSession) != DLT_EN10MB) {
		fprintf(stderr, "Device %s doesn't provide Ethernet headers - not supported\n", device);
		return(2);
	}
	
	if (pcap_compile(sniffSession, &fp, filter_exp, 0, net) == -1) {
		fprintf(stderr, "Couldn't parse filter %s: %s\n", filter_exp, pcap_geterr(sniffSession));
		return(2);
	}

	if (pcap_setfilter(sniffSession, &fp) == -1) {
		fprintf(stderr, "Couldn't install filter %s: %s\n", filter_exp, pcap_geterr(sniffSession));
		return(2);
	}

	/* now we can set our callback function */
	pcap_loop(sniffSession, num_packets, handle_packets_callback, NULL);

	/* cleanup */
	pcap_freecode(&fp);
	pcap_close(sniffSession);
	//pcap_pkthdr , structure time_val
	return(0);
}
