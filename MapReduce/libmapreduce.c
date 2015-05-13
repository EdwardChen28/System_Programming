/** @file libmapreduce.c */
/* author: Edward Chen
 * CS 241
 * The University of Illinois
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>
#include <poll.h>
#include <sys/epoll.h>
#include "libmapreduce.h"
#include "libds/libds.h"


static const int BUFFER_SIZE = 2048;  /**< Size of the buffer used by read_from_fd(). */


/**
 * Adds the key-value pair to the mapreduce data structure.  This may
 * require a reduce() operation.
 *
 * @param key
 *    The key of the key-value pair.  The key has been malloc()'d by
 *    read_from_fd() and must be free()'d by you at some point.
 * @param value
 *    The value of the key-value pair.  The value has been malloc()'d
 *    by read_from_fd() and must be free()'d by you at some point.
 * @param mr
 *    The pass-through mapreduce data structure (from read_from_fd()).
 */
static void process_key_value(const char *key, const char *value, mapreduce_t *mr)
{
	//puts("get into process_key_value");
	//puts(key);
	unsigned long retv = datastore_put(mr->ds, key, value);
    if (retv == 0){
        unsigned long ver;
        const char* val = datastore_get(mr->ds, key, &ver);
        //myreduce will reture a new value on heap, need to free later
        const char* result = mr->reduce(val, value);
        //puts(result);
        //update the value, make an internal copy and need to free later
        unsigned long update = datastore_update(mr->ds, key, result, ver);
        if (update == 0){
            perror("update failed");
        }
        free((char*)val);
        free((char*)result); 
    }
    // key and value are allocated in read_from_fd
    free((char*)key);
    free((char*)value);
}


/**
 * Helper function.  Reads up to BUFFER_SIZE from a file descriptor into a
 * buffer and calls process_key_value() when for each and every key-value
 * pair that is read from the file descriptor.
 *
 * Each key-value must be in a "Key: Value" format, identical to MP1, and
 * each pair must be terminated by a newline ('\n').
 *
 * Each unique file descriptor must have a unique buffer and the buffer
 * must be of size (BUFFER_SIZE + 1).  Therefore, if you have two
 * unique file descriptors, you must have two buffers that each have
 * been malloc()'d to size (BUFFER_SIZE + 1).
 *
 * Note that read_from_fd() makes a read() call and will block if the
 * fd does not have data ready to be read.  This function is complete
 * and does not need to be modified as part of this MP.
 *
 * @param fd
 *    File descriptor to read from.
 * @param buffer
 *    A unique buffer associated with the fd.  This buffer may have
 *    a partial key-value pair between calls to read_from_fd() and
 *    must not be modified outside the context of read_from_fd().
 * @param mr
 *    Pass-through mapreduce_t structure (to process_key_value()).
 *
 * @retval 1
 *    Data was available and was read successfully.
 * @retval 0
 *    The file descriptor fd has been closed, no more data to read.
 * @retval -1
 *    The call to read() produced an error.
 */
static int read_from_fd(int fd, char *buffer, mapreduce_t *mr)
{
	//puts("get into  read_from_fd");
	/* Find the end of the string. */
	int offset = strlen(buffer);

	/* Read bytes from the underlying stream. */
	int bytes_read = read(fd, buffer + offset, BUFFER_SIZE - offset);
	if (bytes_read == 0)
		return 0;
	else if(bytes_read < 0)
	{
		fprintf(stderr, "error in read.\n");
		return -1;
	}

	buffer[offset + bytes_read] = '\0';

	/* Loop through each "key: value\n" line from the fd. */
	char *line;
	while ((line = strstr(buffer, "\n")) != NULL)
	{
		*line = '\0';

		/* Find the key/value split. */
		char *split = strstr(buffer, ": ");
		if (split == NULL)
			continue;

		/* Allocate and assign memory */
		char *key = malloc((split - buffer + 1) * sizeof(char));
		char *value = malloc((strlen(split) - 2 + 1) * sizeof(char));

		strncpy(key, buffer, split - buffer);
		key[split - buffer] = '\0';

		strcpy(value, split + 2);

		/* Process the key/value. */
		process_key_value(key, value, mr);

		/* Shift the contents of the buffer to remove the space used by the processed line. */
		memmove(buffer, line + 1, BUFFER_SIZE - ((line + 1) - buffer));
		buffer[BUFFER_SIZE - ((line + 1) - buffer)] = '\0';
	}

	return 1;
}
/**
 * Initialize the mapreduce data structure, given a map and a reduce
 * function pointer.
 */
void mapreduce_init(mapreduce_t *mr, 
                    void (*mymap)(int, const char *), 
                    const char *(*myreduce)(const char *, const char *))
{	//mr is global variable that declared by caller. no need to allocate memory
	//mr = (mapreduce_t*)calloc(1,sizeof(mapreduce_t*));
    mr->ds = (datastore_t*)calloc(1,sizeof(datastore_t));
    datastore_init(mr->ds); // ds initialization does not allocate memory in heap
    mr->map = mymap;
    mr->reduce = myreduce;

}




void* reducer(void* arg){

	mapreduce_t* m = (mapreduce_t*)arg;
	int counter = m->count;
	while(counter > 0 ){
		struct epoll_event temp;
		int wait = epoll_wait(m->epoll_fd, &temp ,1 , -1);
		if(wait == -1){
			perror("eopll_wait failed");
			exit(EXIT_FAILURE);
		}
		int i;
		for(i = 0 ; i < m->count; i ++){
			if(m->event[i].data.fd == temp.data.fd){
				int r = read_from_fd(temp.data.fd, m->buffer[i], m);
				if(r == -1) perror("read_from_fd failed");
				if(r == 0){
					counter--;
					epoll_ctl(m->epoll_fd, EPOLL_CTL_DEL, temp.data.fd, &temp);
				}
				break;
			}
		}
	}
	return NULL;
}


/**
 * Starts the map() processes for each value in the values array.
 * (See the MP description for full details.)
 */
void mapreduce_map_all(mapreduce_t *mr, const char **values)
{
	int count = 0; // cout the number of fork();
    while (values[count] != NULL)
        count++;
    mr->count = count;
    mr->fd = malloc(sizeof(int*)*count);
    mr->buffer = malloc(sizeof(char*)*count);
    int ep = epoll_create(count);
    if (ep == -1) perror("epoll failed to create");
    mr->epoll_fd = ep; // save the epoll fd in struct mr
    mr->event = calloc(count, sizeof(struct epoll_event));
    int i;
    for (i = 0; i < count; i++){
        mr->fd[i] = malloc(sizeof(int*) * 2);
        mr->buffer[i] = malloc(sizeof(char)*BUFFER_SIZE + 1);
        mr->buffer[i][0] = '\0';
        pipe(mr->fd[i]);
 
        pid_t p = fork();
        if (p == 0){// in child
	 	mr->map(mr->fd[i][1], values[i]);
            exit(0);
        }
        else if (p > 0){

            close(mr->fd[i][1]);
        }
 	mr->event[i].events = EPOLLIN;
	mr->event[i].data.fd = mr->fd[i][0];
    int ctl = epoll_ctl(mr->epoll_fd, EPOLL_CTL_ADD, mr->fd[i][0], &mr->event[i]);
 	if(ctl==-1) perror("epoll_ctl: write failed");
    }
	pthread_create(&(mr->workerThread), NULL, reducer, (void*)mr);
}


/**
 * Blocks until all the reduce() operations have been completed.
 * (See the MP description for full details.)
 */
void mapreduce_reduce_all(mapreduce_t *mr)
{
	 pthread_join(mr->workerThread, NULL);
}


/**
 * Gets the current value for a key.
 * (See the MP description for full details.)
 */
const char *mapreduce_get_value(mapreduce_t *mr, const char *result_key)
{
	 if (mr == NULL || result_key == NULL) return NULL;
    return datastore_get(mr->ds, result_key, NULL);
}


/**
 * Destroys the mapreduce data structure.
 */
void mapreduce_destroy(mapreduce_t *mr)
{
	datastore_destroy(mr->ds);
    free(mr->ds);
    int i;
    for (i = 0; i < mr->count; i++){
        free(mr->buffer[i]);
        free(mr->fd[i]);
    }
    free(mr->buffer);
    free(mr->fd); 
    free(mr->event);

}
