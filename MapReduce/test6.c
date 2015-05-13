/* 
 * CS 241
 * The University of Illinois
 */

#define _GNU_SOURCE
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>

#include "libmapreduce.h"

#define CHARS_PER_INPUT 30000
#define INPUTS_NEEDED 10


/* Takes input string and output the following key-value pairs if find any:
 * wonder: matched_line  key1
 * Wonder: matched_line  key2
 * WONDER: matched_line  key3
 */
void map(int fd, const char *data) {
	if(data == NULL) return;
	char*key;
    char* value1 = calloc(strlen((char*)data)*2, sizeof(char));
    char* value2 = calloc(strlen((char*)data), sizeof(char));
    char* value3 = calloc(strlen((char*)data), sizeof(char));
    char* newline = (char*) "\n" ;
    char* line; 
//	char* result = calloc(strlen(data),sizeof(char)); 
    char* str = (char*) data;
    line = strtok(str, newline);
    while (line != NULL){
        if ((key = strstr(line, "wonder")) != NULL){
		
        	strcat(value1, "wonder: ");
            strcat(value1, line);
            strcat(value1,"\n");
        }
        else if ((key = strstr(line, "Wonder")) != NULL){
       	 strcat(value2, "Wonder: ");
            strcat(value2, line);
          strcat(value2,"\n");
        }
       else if ((key = strstr(line, "WONDER")) != NULL){
      		strcat(value3, "WONDER: ");
            strcat(value3, line);
            strcat(value3,"\n");
        }
        line = strtok(NULL, newline);
 
    }
    // puts(value1);  
    // puts(value2);
 //    puts(value3);  

   // int len = strlen(value1) + strlen(value2) + strlen(value3);
  //  char* result = calloc( len+20, 1 );
//	memcpy(result, value1, strlen(value1));// puts(result);
//	memcpy(result, value2, strlen(value2));// puts(result);
//	memcpy(result, value3, strlen(value3));
   // strcat(result, value1); puts(result);
   // strcat(result, value2);
  //  strcat(result, value3);
 // puts(result);
   // write(fd, result, strlen(result));
write(fd, value1, strlen(value1));
write(fd, value2, strlen(value2));
write(fd, value3, strlen(value3));
 	free(value1); free(value2); free(value3);
 //	free(result);
    close(fd);
}

/* Takes two lines and combine them into one string as the following:
 * value1\nvalue2
*/
const char *reduce(const char *value1, const char *value2) {
	//puts(value1);
	//puts(value2);
	char*result;
    asprintf(&result, "%s\n%s", value1, value2);
    return result;  
}


int main()
{
    FILE *file = fopen("alice.txt", "r");
    char s[1024];
    int i;

    char **values = malloc(INPUTS_NEEDED * sizeof(char *));
    int values_cur = 0;

    values[0] = malloc(CHARS_PER_INPUT + 1);
    values[0][0] = '\0';

    while (fgets(s, 1024, file) != NULL)
    {
        if (strlen(values[values_cur]) + strlen(s) < CHARS_PER_INPUT)
            strcat(values[values_cur], s);
        else
        {
            values_cur++;
            values[values_cur] = malloc(CHARS_PER_INPUT + 1);
            values[values_cur][0] = '\0';
            strcat(values[values_cur], s);
        }
    }

    values_cur++;
    values[values_cur] = NULL;

    fclose(file);
	
    mapreduce_t mr;
    mapreduce_init(&mr, map, reduce);
	//printf("value 1:%s \n", values[0]);
    mapreduce_map_all(&mr, (const char **)values);
    mapreduce_reduce_all(&mr);

   
    char * keywords[3];
    
    keywords[0] = "wonder";
    keywords[1] = "Wonder";
    keywords[2] = "WONDER";


    for(i = 0; i < 3; i ++)
    {
        const char * result = mapreduce_get_value(&mr, keywords[i]);
        printf("=============%s==============\n", keywords[i]);
        if (result == NULL)
	    printf("NOT FOUND\n");
	else
	{
	    printf("%s\n", result);
            free((void*)result);
	}
    }


    for (i = 0; i < values_cur; i++)
        free(values[i]);
    free(values);

    mapreduce_destroy(&mr);

    return 0;
}
