#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <signal.h>
#include <sys/time.h>

#include <pthread.h>
#include "wearable.h"

const suseconds_t MICRO_TOLERANCE = 100 * 1000;

const char* sPort_ = NULL;
const char* wearable_gPort_;

FILE* user_data_;
FILE* actual_data_;
FILE* report_;

struct timeval start_time_;

typedef struct StatsInfo {
    int size_;
    int* content_;
} StatsInfo;

//sets an alarm to trigger when we should have received
//the desired timestamp. This alarm triggers at a tolerance
//of MICRO_TOLERANCE. Note, this imlies that if the timestamp
//has already passed the alarm will trigger after MICRO_TOLERANCE
void set_alarm(int timestamp) {
    //printf("Setting new alarm for %i\n", timestamp);
    struct itimerval tout_val;
    struct timeval now, interval, temp;
    gettimeofday(&now, NULL);

    tout_val.it_interval.tv_sec = 0;
    tout_val.it_interval.tv_usec = 0;
    timersub( &now, &start_time_, &interval);

    if (interval.tv_sec * 1000 + interval.tv_usec / 1000 > timestamp) {
        tout_val.it_value.tv_sec = 0;
        tout_val.it_value.tv_usec = MICRO_TOLERANCE;
        setitimer(ITIMER_REAL, &tout_val, 0);
    } else {
        temp.tv_sec = timestamp / 1000; //timestamp is in millis
        temp.tv_usec = timestamp % 1000;
        timersub( &temp, &interval, &now);

        tout_val.it_value.tv_sec = now.tv_sec + (now.tv_usec + MICRO_TOLERANCE) / 1000000; 
        tout_val.it_value.tv_usec = (now.tv_usec + MICRO_TOLERANCE) % 1000000;
        //printf("Timer set for %zu %i\n", tout_val.it_value.tv_sec, tout_val.it_value.tv_usec);
    }

    setitimer(ITIMER_REAL, &tout_val, 0);
}

//The thread sends the requests for statistics from the user
//It triggers alarms to fire to test if the statistics take
//too long to come in.
void* stats_thread(void* arg) {
    int socketfd = socket(AF_INET, SOCK_STREAM, 0);
    gettimeofday(&start_time_, NULL);

    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int ret;
    if ((ret = getaddrinfo(NULL, sPort_, &hints, &result)) != 0) {
        printf("Failed to get address info\n");
        fprintf(report_, "Failed to get address info\n");
        exit(EXIT_FAILURE);
    }

    //open connection
    connect(socketfd, result->ai_addr, result->ai_addrlen);
    freeaddrinfo(result);

    int* sample_times = ((StatsInfo*)arg)->content_;
    int sample_size = ((StatsInfo*)arg)->size_;
    int i;
    for (i = 0;i < sample_size * 3;i += 3) { //Loop through the statitic requests...
        int wait_time = sample_times[i]; //how long to wait before sending the request... (in millis)
        int t1 = sample_times[i + 1]; //start timestamp
        int t2 = sample_times[i + 2]; //end timestamp

        usleep(wait_time * 1000); //sleep the desired time
        set_alarm(t2); //set an alarm to fire after we should have received our data
        
        char buffer[1024];
        sprintf(buffer, "%i:%i", t1, t2);
        write(socketfd, buffer, strlen(buffer));

        //read here
        int current_size = 0;
        char* complete_message = NULL;
        while (1) {
            int b_read = read(socketfd, buffer, 1024);
            complete_message = realloc(complete_message, current_size + b_read + 1);
            memcpy(complete_message + current_size, buffer, b_read);
            current_size += b_read;
            if (strncmp(&complete_message[current_size - 2], "\r\n", 2) == 0)
                break;
        }
        complete_message[current_size - 2] = '\0';
        printf("Receieved:%s\n", complete_message);
        fprintf(user_data_, "%s", complete_message);
        free(complete_message);

        set_alarm(1000 * 1000 * 30); //clear out our alarm...
    }

    set_alarm(1000 * 1000 * 30);
    close(socketfd);
    return NULL;
}

int get_type1_selector(void *arg) {
    SampleData* data = (SampleData*) arg;
    return strcmp(data->type_, TYPE1) == 0;
}

int get_type2_selector(void *arg) {
    SampleData* data = (SampleData*) arg;
    return strcmp(data->type_, TYPE2) == 0;
}

int get_type3_selector(void *arg) {
    SampleData* data = (SampleData*) arg;
    return strcmp(data->type_, TYPE3) == 0;
}

int compare_num(const void * a, const void * b) {
  return ( *(int*)a - *(int*)b );
}

//Prints the results... see the template solution (its the same)
void print_results(const char* type, timestamp_entry* results, int size) {
    long avg = 0;
    int i;

    int temp_array[size];
    printf("Results for %s:\n", type);
    fprintf(actual_data_, "Results for %s:\n", type);

    printf("Size:%i\n", size);
    fprintf(actual_data_, "Size:%i\n", size);
    for (i = 0;i < size;i ++) {
        temp_array[i] = ((SampleData*)(results[i].data_))->data_;
        printf("%i %i\n", i, temp_array[i]);
        avg += ((SampleData*)(results[i].data_))->data_;
    }

    qsort(temp_array, size, sizeof(int), compare_num);

    if (size > 0) {
        printf("Median:%i\n", ((size % 2 == 0) ?
            (temp_array[size / 2] + temp_array[size / 2 - 1]) / 2 : temp_array[size / 2]));
        fprintf(actual_data_, "Median:%i\n", ((size % 2 == 0) ?
            (temp_array[size / 2] + temp_array[size / 2 - 1]) / 2 : temp_array[size / 2]));
    } else { 
        printf("Median:0\n");
        fprintf(actual_data_, "Median:0\n");
    }
    
    printf("Average:%li\n\n", (size == 0 ? 0 : avg / size));
    fprintf(actual_data_, "Average:%li\n\n", (size == 0 ? 0 : avg / size));
}

void alarm_wakeup(int i) {
    printf("Failed to deliver messages in time!\n");
    fprintf(report_, "Failed to deliver messages in time!\n");
}

//usage is <wearable_port> <user_port> <datafile>
int main(int argc, const char* argv[]) {
	report_ = fopen("_error_report.rp", "w+");
	if (argc != 4) {
		printf("Invalid input size\n");
       		fprintf(report_, "Invalid input size\n");
		fclose(report_);
		exit(EXIT_FAILURE);
	}

    	actual_data_ = fopen("_expected.rp", "w+");
    	user_data_ = fopen("_received.rp", "w+");

    signal(SIGALRM, alarm_wakeup);

	int i, k, count;
	wearable_gPort_ = argv[1];
    sPort_ = argv[2];

    int latest_launch;
    int sample_count;
    int* sample_times;
    pthread_t stats_threadp;

    //create wearables
	pthread_t* threads = get_wearables(argv[3], &count, &sample_times, &sample_count, &latest_launch);
	Wearable* wearables[count];
    //wait for all wearables to finish
	void* ret_val = NULL;
    queue_t complete_data_set;
    queue_init(&complete_data_set);

    StatsInfo info;
    info.size_ = sample_count;
    info.content_ = sample_times;
    pthread_create(&stats_threadp, NULL, stats_thread, &info);

    if (info.content_[0] <= latest_launch)
        fprintf(stderr, "POSSIBLY BAD TEST CASE! THE LATEST WEARABLE CONNECTION IS AFTER (OR EQUAL TO)\
                         THE LAUNCH OF THE FIRST STATS REQUEST. THIS MAY CAUSE INDETERMINANT BEHAVIOR\n");

	for (i = 0;i < count;i ++) {
		pthread_join(threads[i], &ret_val);
		wearables[i] = (Wearable*) ret_val;
        for (k = 0;k < queue_size(&wearables[i]->queued_results_);k ++) {
            timestamp_entry result;
            queue_at(&wearables[i]->queued_results_, k, &result);
            queue_insert(&complete_data_set, result.timestamp_, result.data_);
        }
    }

    pthread_join(stats_threadp, NULL);
    free(threads);

    //print results of all infomation
    for (i = 0;i < sample_count * 3;i += 3) {
        int t1 = sample_times[i + 1];
        int t2 = sample_times[i + 2];

        int size;
        timestamp_entry* results = queue_gather(&complete_data_set, t1, t2, 
                                        get_type1_selector, &size);
        print_results(TYPE1, results, size);
        free(results);

        results = queue_gather(&complete_data_set, t1, t2, 
                                        get_type2_selector, &size);
        print_results(TYPE2, results, size);
        free(results);

        results = queue_gather(&complete_data_set, t1, t2, 
                                        get_type3_selector, &size);
        print_results(TYPE3, results, size);
        free(results);
    }
    free(sample_times);

    for (i = 0;i < count;i ++) 
        free_wearable(wearables[i]);

    queue_destroy(&complete_data_set, 0);

    fclose(report_);
    fclose(user_data_);
    fclose(actual_data_);

	return 0;
}
