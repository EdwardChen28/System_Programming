#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct timestamp_entry {
	unsigned long timestamp_;
	void* data_;

} timestamp_entry;

typedef struct queue_entry_t {
	timestamp_entry stamped_entry_;

	struct queue_entry_t* next_;

} queue_entry_t;

typedef struct queue_t {
	queue_entry_t* head_;

	int size_;
} queue_t;

/**
Initilizes the queue
*/
void queue_init(queue_t* queue);

/**
Destroys the internal contents used by the queue object. 
Any calls the queue after this function are undefined. This method
will free the passed in data if free_data != 0, otherwise, the user
is responsible for freeing the data.
*/
void queue_destroy(queue_t* queue, int free_data);

/**
Inserts a void pointer into the queue, with the given timestamp. The
queue is not responsible for freeing the given data, that is left to the user
*/
void queue_insert(queue_t* queue, unsigned long timestamp, void* data);

/**
Returns an array of timestamp_entry's between the given timestamp range [start, end) (in order by timestamp).
Upon completion the value of return_size is set to the size of the returned array.
The selector is a function pointer to a function that takes in a void pointer, and returns
a int, (0 or 1). This function is passed the data passed in, and is returns 1 if this 
value should be included in the return value. This essientially lets you choose the values
from the queue you would like to gather within the range.
 
 Here's an example of selector that selects data with the word "Hello"
 int get_hello_selector(void *arg) {
    SampleData* data = (SampleData*) arg;
    return strcmp(data->type_, "hello") == 0;
 }
 
 int numresults; // all events with 100 <= timestamp < 200
 timestamp_entry* result = queue_gather( allevents, 100, 200, get_hello_selector, &numresults );
 free(result);
 */
timestamp_entry* queue_gather(queue_t* queue, unsigned long start, unsigned long end, 
							 int (*selector)(void*),
				 			 int* return_size);

/**
Returnt the element at the 
*/
int queue_at(queue_t* queue, int entry_index, timestamp_entry* result) ;
int queue_size(queue_t* queue);