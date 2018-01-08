#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h> 
#include <signal.h>
#include <sys/types.h>
#include <netdb.h>
#include <fcntl.h>
#include <openssl/aes.h>
#include <openssl/rand.h>

#define IV_SIZE 8
#define BUFFER_SIZE 4096
#define INT2VOIDP(i) (void*)(uintptr_t)(i)

int connectToSocket(struct sockaddr_in addr){
    int socketFd;
    // struct sockaddr_in addr;
    if ((socketFd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    // printf("%d socketFd %d\n",getpid(),socketFd);
    if (connect(socketFd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }
    return socketFd;
}

int getSocketFdAtPort(struct sockaddr_in addr){
    // printf("%d getSocketFdAtPort IP: %s, Port: %d\n", getpid(),ip,port);
    int socketFd;
    int opt=1;
    // struct sockaddr_in addr;
    int newFileDescriptor;
    int addrlen = sizeof(struct sockaddr_in);
    if ((socketFd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    char messageToSend[BUFFER_SIZE];// = "messageToSend from server";
    // Forcefully attaching socket to the port 8080
    if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    if (bind(socketFd, (struct sockaddr *)&addr, 
                                 sizeof(addr))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(socketFd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    return socketFd;
}

int acceptSocket(int socketFd,struct sockaddr_in addr){
    int addrlen=sizeof(addr);
    int newFileDescriptor;
    // printf("%d acceptSocket at port: %d\n", getpid(),port);
    if ((newFileDescriptor = accept(socketFd, (struct sockaddr *)&addr, 
                       (socklen_t*)&addrlen))<0)
    {
        perror("accept");
        exit(EXIT_FAILURE);
    }    
    // printf("%d successfully acceptSocket port: %d, newFileDescriptor : %d\n",getpid(),port,newFileDescriptor);
    return newFileDescriptor;
}

// Reference: https://stackoverflow.com/questions/3141860/aes-ctr-256-encryption-mode-of-operation-on-openssl
/******************************************/
typedef struct{
    unsigned char ivec[16];  /* ivec[0..7] is the IV, ivec[8..15] is the big-endian counter */
    unsigned int num;
    unsigned char ecount[16];
}ctr_state;

void init_ctr(ctr_state *state, const unsigned char iv[16])
{
    /* aes_ctr128_encrypt requires 'num' and 'ecount' set to zero on the
    * first call. */
    state->num = 0;
    memset(state->ecount, 0, AES_BLOCK_SIZE);

    /* Initialise counter in 'ivec' to 0 */
    memset(state->ivec + 8, 0, 8);

    /* Copy IV into 'ivec' */
    memcpy(state->ivec, iv, 8);
}
/******************************************/

int main(int argc, char *argv[])
{
    int val;
    int opt=1;
    int isServer=0;
    int openPort;
    int socketFd;
    int serverValRead;
    int clientValRead;
    int closedPort;
    int bufferSize = BUFFER_SIZE;
    int addrlen = sizeof(struct sockaddr_in);
    char *ip;
    char clientBuffer[BUFFER_SIZE] = {0};
    char serverBuffer[BUFFER_SIZE] = {0};
    unsigned char *key=(unsigned char *)malloc(sizeof(unsigned char)*32);
    struct sockaddr_in sshAddr; 
    struct sockaddr_in proxyAddr;
    AES_KEY aesKey;
    struct hostent *host;


    while ((val = getopt (argc, argv, "l:k:")) != -1){
        switch(val){
            case 'l':
                isServer=1;
                openPort=(int)strtol(optarg, NULL, 10);
                break;
            case 'k':
                key=optarg;
                if (AES_set_encrypt_key(key, 128, &aesKey) < 0) {
                    printf("encryption key error!\n");
                    exit(1);
                }
                break;                          
        }
    }
    int index=optind;
    ip=(char *)malloc(sizeof(char)*50);
    if(index < argc){
        ip=(char *)argv[index++];
        if(isServer==1){
            closedPort=(int)strtol(argv[index], NULL, 10);
        }else{
            openPort=(int)strtol(argv[index], NULL, 10);;
        } 
    }
    if ((host = gethostbyname(ip)) == 0) {
        fprintf(stderr, "Error while getting the hostname!\n");
        exit(EXIT_FAILURE);
    }
    printf("%d isServer: %d, IP: %s, openPort: %d, closedPort: %d, key: %s \n", getpid(),isServer,ip,openPort,closedPort,key);

    if(isServer==1){
        proxyAddr.sin_family = AF_INET;
        proxyAddr.sin_addr.s_addr = htons(INADDR_ANY);
        proxyAddr.sin_port = htons(openPort);

        sshAddr.sin_family = AF_INET;
        sshAddr.sin_port = htons(closedPort);
        sshAddr.sin_addr.s_addr = ((struct in_addr*)(host->h_addr))->s_addr;

        
        int listenSockFd = getSocketFdAtPort(proxyAddr);
        int ssh_done=0;
		while(1){
            int proxySock;
            unsigned char buffer[BUFFER_SIZE];
            bzero(buffer, BUFFER_SIZE);
            ctr_state state;
            unsigned char iv[IV_SIZE];

            /*************PROXY SOCKET*****************/

            proxySock = acceptSocket(listenSockFd,proxyAddr);
            pid_t p = fork();
            if(p==0){
                
                int ssh_fd;
                /*************SSH SOCKET*******************/
                ssh_fd = connectToSocket(sshAddr);
                int flags = fcntl(ssh_fd, F_GETFL);
                if (flags == -1) {
                    printf("read ssh_fd flag error!\n");
                    // close(ssh_fd);
                }
                fcntl(ssh_fd, F_SETFL, flags | O_NONBLOCK);


                flags = fcntl(proxySock, F_GETFL);
                if (flags == -1) {
                    printf("read proxySock 1 flag error!\n");
                    printf("Closing connections and exit thread!\n");
                    close(proxySock);
                }
                fcntl(proxySock, F_SETFL, flags | O_NONBLOCK);
                /******************************************/
                int dataLength;
                while(1){
                    while ((dataLength = read(proxySock, buffer, BUFFER_SIZE)) > 0) {
                        if (dataLength < IV_SIZE) {
                            printf("Packet length smaller than 8 server!\n");
                            close(proxySock);
                            close(ssh_fd);
                        }
                        memcpy(iv, buffer, IV_SIZE);
                        unsigned char decryptedText[dataLength-IV_SIZE];
                        init_ctr(&state, iv);
                        AES_ctr128_encrypt(buffer+IV_SIZE, decryptedText, dataLength-IV_SIZE, &aesKey, state.ivec, state.ecount, &state.num);
                        //printf("%.*s\n", n, buffer);
                        write(ssh_fd, decryptedText, dataLength-IV_SIZE);
                        if (dataLength < BUFFER_SIZE)
                            break;
                    };
                    
                    while ((dataLength = read(ssh_fd, buffer, BUFFER_SIZE)) >= 0) {
                        if (dataLength > 0) {
                            if (!RAND_bytes(iv, IV_SIZE)) {
                                printf("Error while generating IV random bytes.\n");
                                exit(1);
                            }
                            char *tmp = (char*)malloc(dataLength + IV_SIZE);
                            memcpy(tmp, iv, IV_SIZE);
                            unsigned char encryption[dataLength];
                            init_ctr(&state, iv);
                            AES_ctr128_encrypt(buffer, encryption, dataLength, &aesKey, state.ivec, state.ecount, &state.num);
                            memcpy(tmp+IV_SIZE, encryption, dataLength);
                            usleep(800);
                            write(proxySock, tmp, dataLength + IV_SIZE);
                            free(tmp);
                        }
                        if (ssh_done == 0 && dataLength == 0)
                            ssh_done = 1;
                        if (dataLength < BUFFER_SIZE)
                            break;
                    }
                    if (ssh_done == 1)
                        break;
                }
                printf("Closing connections. Exiting thread!\n");
                close(proxySock); 
            }
		}

    }else{
        proxyAddr.sin_family = AF_INET;
        proxyAddr.sin_addr.s_addr = htons(INADDR_ANY);
        proxyAddr.sin_port = htons(openPort);

        unsigned char buffer[BUFFER_SIZE];
        ctr_state state;
        unsigned char iv[IV_SIZE];
        int clientProxySockfd = connectToSocket(proxyAddr);
        fcntl(STDIN_FILENO, F_SETFL, O_NONBLOCK);

        int flags = fcntl(clientProxySockfd, F_GETFL);
        if (flags == -1) {
            printf("read clientProxySockfd flag error!\n");
            close(clientProxySockfd);
        }
        fcntl(clientProxySockfd, F_SETFL, flags | O_NONBLOCK);
        bzero(buffer, BUFFER_SIZE);
        int dataLength;
        while(1){
            while ((dataLength = read(STDIN_FILENO, buffer, BUFFER_SIZE)) > 0) {
                printf("reading from client, stdin.\n");
                if (!RAND_bytes(iv, IV_SIZE)) {
                    printf("Error while generating IV random bytes.\n");
                    exit(1);
                }
                char *tmp = (char*)malloc(dataLength + IV_SIZE);
                memcpy(tmp, iv, IV_SIZE);
                unsigned char encryptedText[dataLength];
                init_ctr(&state, iv);
                AES_ctr128_encrypt(buffer, encryptedText, dataLength, &aesKey, state.ivec, state.ecount, &state.num);
                printf("Encrypted Text : %s\n",encryptedText);
                memcpy(tmp + IV_SIZE, encryptedText, dataLength);
                write(clientProxySockfd, tmp, dataLength+IV_SIZE);
                free(tmp);
                if (dataLength < BUFFER_SIZE)
                    break;
            };
            
            while ((dataLength = read(clientProxySockfd, buffer, BUFFER_SIZE)) > 0) {
                printf("reading from openport\n");
                if (dataLength < IV_SIZE) {
                    fprintf(stderr, "Packet length smaller than 8 client!\n");
                    close(clientProxySockfd);
                    return 0;
                }
                memcpy(iv, buffer, IV_SIZE);
                unsigned char decryptedText[dataLength - IV_SIZE];
                init_ctr(&state, iv);
                AES_ctr128_encrypt(buffer + IV_SIZE, decryptedText, dataLength - IV_SIZE, &aesKey, state.ivec, state.ecount, &state.num);
                write(STDOUT_FILENO, decryptedText, dataLength-IV_SIZE);
                if (dataLength < BUFFER_SIZE)
                    break;
            }        
        }
    }

return 0;

}