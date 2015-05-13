#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <pthread.h>
#include "queue.h"

const char* TYPE1 = "heart_beat";
const char* TYPE2 = "blood_sugar";
const char* TYPE3 = "body_temp";

//the wearable server socket, which all wearables connect to
int wearable_server_fd;

//a lock for your queue sctructure... (use it)
pthread_mutex_t queue_lock_ ;//= PTHREAD_MUTEX_INITIALIZER;
//a queue for all received data... 
queue_t received_data_;
// condition variavle
pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
//server count
int count = 0, end=0;
pthread_mutex_t threadCount;
long* timeArray;
int arraySize = 0;
pthread_mutex_t array;

typedef struct SampleData {
	char type_[50];
	int data_;
} SampleData;

int compare (const void * a, const void * b) {
  return ( *(int*)a - *(int*)b );
}

/**
Used to write out the statistics of a given results set (of timestamp_entry's).
To generate the result set see queue_gather(). fd is the file descriptor to
which the information is sent out. The type is the type of data that is written out
(TYPE1, TYPE2, TYPE3). results is the array of timestamp_entrys, and size is 
the size of that array. NOTE: that you should call method for every type 
(TYPE1, TYPE2, TYPE3), and then write out the infomration "\r\n" to signify that
you have finished sending out the results.
*/
void write_results(int fd, const char* type, timestamp_entry* results, int size) {
    long avg = 0;
    int i;

    char buffer[1024];
    int temp_array[size];
    sprintf(buffer, "Results for %s:\n", type);
    sprintf(buffer + strlen(buffer), "Size:%i\n", size);
    for (i = 0;i < size;i ++) {
        temp_array[i] = ((SampleData*)(results[i].data_))->data_;
        avg += ((SampleData*)(results[i].data_))->data_;
    }

    qsort(temp_array, size, sizeof(int), compare);

    if (size != 0) {
    	sprintf(buffer + strlen(buffer), "Median:%i\n", (size % 2 == 0) ?
            (temp_array[size / 2] + temp_array[size / 2 - 1]) / 2 : temp_array[size / 2]);
    } else {
        sprintf(buffer + strlen(buffer), "Median:0\n");
    }

    sprintf(buffer + strlen(buffer), "Average:%li\n\n", (size == 0 ? 0 : avg / size));
    write(fd, buffer, strlen(buffer));
}

/**
Given an input line in the form <timestamp>:<value>:<type>, this method 
parses the infomration from the string, into the given timestamp, and
mallocs space for SampleData, and stores the type and value within
*/
void extract_key(char* line, long* timestamp, SampleData** ret) {
	*ret = malloc(sizeof(SampleData));
	sscanf(line, "%zu:%i:%[^:]%:\\.*", timestamp, &((*ret)->data_), (*ret)->type_);
}

//int input = 0;
void* wearable_processor_thread(void* args) {
	int socketfd = *((int*)args);
	//Use a buffer of length 64!
	//TODO read data from the socket until -1 is returned by read
	long* time =calloc(1,sizeof(long));
	char buffer[64];
	while( recv(socketfd, buffer, sizeof(buffer),0) > 0 ){
	//	printf("insize wearable thread: %s", buffer);
		SampleData* data;
		extract_key(buffer,time,&data);
		pthread_mutex_lock(&queue_lock_);
		queue_insert(&received_data_ , *time, data);
		pthread_mutex_unlock(&queue_lock_);

		pthread_mutex_lock(&array);
		timeArray[arraySize++] = (*time);
		if((*time) >= (long)end)
			pthread_cond_broadcast(&cv);
		pthread_mutex_unlock(&array);
		
		}
	free(time);
	pthread_mutex_lock(&array);
	count--;
	if(count == 0){
		pthread_cond_broadcast(&cv);	
	}
//	printf("about to close fd and count:%d\n",count);
	pthread_mutex_unlock(&array);
	close(socketfd);
	return NULL;
}

int Type1Selector(void* arg){ SampleData* temp = (SampleData*) arg; return strcmp(temp->type_,TYPE1)==0;}
int Type2Selector(void* arg){ SampleData* temp = (SampleData*) arg; return strcmp(temp->type_,TYPE2)==0;}
int Type3Selector(void* arg){ SampleData* temp = (SampleData*) arg; return strcmp(temp->type_,TYPE3)==0;}
void* user_request_thread(void* args) {
	int socketfd = *((int*)args);
	char buffer[64];
	while(recv(socketfd, buffer, sizeof(buffer),0)>0){
	//	printf("user request thread:%s\n", buffer);
		int start;
		sscanf(buffer, "%d:%d",&start,&end);
		pthread_mutex_lock(&array); 
		long t;
		while( (t= timeArray[arraySize-1]) < (long)end && count > 0){
		//	printf(" t: %ld end:%d count: %d\n", t, end,count);
		//	puts("enter cond_wait");
			pthread_cond_wait(&cv , &array);
		}
	//	puts("iam out");
		pthread_mutex_unlock(&array); 
		pthread_mutex_lock(&queue_lock_);
		int num = (start+end)/2;
		timestamp_entry* ret= NULL;
		ret = queue_gather(&received_data_, start, end ,Type1Selector,&num);
		write_results(socketfd, TYPE1, ret, num);
		free(ret); ret = NULL;
		ret = queue_gather(&received_data_, start, end, Type2Selector, &num);
		write_results(socketfd, TYPE2, ret, num);
		free(ret); ret = NULL;
		ret = queue_gather(&received_data_, start, end, Type3Selector, &num);
		write_results(socketfd, TYPE3, ret, num);
		free(ret); ret = NULL;
		pthread_mutex_unlock(&queue_lock_);
		write(socketfd, "\r\n",2);
	//	puts("write sucess");
				
	}
	//TODO rread data from the socket until -1 is returned by read
	//Requests will be in the form
	//<timestamp1>:<timestamp2>, then write out statiticcs for data between
	//those timestamp ranges

	close(socketfd);
	return NULL;
}

//IMPLEMENT!
//given a string with the port value, set up a 
//serversocket file descriptor and return it
int open_server_socket(const char* port) {
	//TOD0
	int sock_fd =  socket(AF_INET6, SOCK_STREAM,0);
	struct addrinfo hints, * result;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_INET6;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	int s = getaddrinfo(NULL, port, &hints, &result);
	if( s != 0){
		fprintf(stderr, " getaddrinfo: %s\n", gai_strerror(s));
		close(sock_fd);
		exit(1);
	}
	int opt = 1;
	setsockopt(sock_fd, SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt));


	int b = bind(sock_fd, result->ai_addr, result->ai_addrlen);
	if( b !=0 ){ perror("bind failed"); close(sock_fd); exit(1);}
	if(listen(sock_fd,40) != 0){
		perror("listen failed");
		close(sock_fd);
		exit(1);
	}
	return sock_fd;
}

void signal_received(int sig) {
	//TODO close server socket, free anything you dont free in main
	close(wearable_server_fd);
	return;
//	free(timeArray); 
//	queue_destroy(&received_data_,1);
}

int main(int argc, const char* argv[]) {
	if (argc != 3) {
		printf("Invalid input size - usage: wearable_server <wearable_port> <request_port>\n");
		exit(EXIT_FAILURE);
	}
	//TODO setup sig handler for SIGINT
	struct sigaction sa;
	sa.sa_handler = signal_received;
	sa.sa_flags=0;//SA_RESTART;
	sigfillset(&sa.sa_mask);
	sigaction(SIGINT, &sa, 0);
	//initialize the queue receieved_data_
  	timeArray = calloc(2048, sizeof(long));
	int request_server_fd = open_server_socket(argv[2]);
	wearable_server_fd = open_server_socket(argv[1]);

	pthread_t request_thread;

	//HERE WE SET UP THE REQUEST CLIENT
	int request_socket = accept(request_server_fd, NULL, NULL);
	pthread_create(&request_thread, NULL, user_request_thread, &request_socket);
   	//HERE WE CLOSE THE SERVER AFTER WE HAVE ACCEPTED OUR ONE CONNECITON
	close(request_server_fd);

	queue_init(&received_data_);
	pthread_mutex_init(&queue_lock_, NULL);
	pthread_mutex_init(&array,NULL);
	pthread_mutex_init(&threadCount, NULL);
	//TODO accept continous requestsu

	int *temp = calloc(40, sizeof(int)); int t=0;
	pthread_t id[40];int accept_fd; int idc = 0;
	while( (accept_fd = accept(wearable_server_fd, NULL, NULL)) != -1){
	//	printf("accept_fd:%d\n", accept_fd);
		temp[t] = accept_fd;
		pthread_create(&id[idc++], NULL, wearable_processor_thread, (void*)(&temp[t]));
		t++;
		pthread_mutex_lock(&array);
		count++;
		pthread_mutex_unlock(&array);
	}

	//TODO join all threads we spawned from the wearables
	int i = 0;
	for(;i<idc;i++) pthread_join(id[i], NULL);
	pthread_join(request_thread, NULL);
	queue_destroy(&received_data_, 1);
	free(timeArray); free(temp);
	return 0;
}
