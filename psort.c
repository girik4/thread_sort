#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include<sys/stat.h>>

#define KEY_S 4
#define RECORD_S 96
#define RECORD 100

int num_records;

typedef struct 
{
    char key[KEY_S];
    char record[RECORD_S];
} record_key;

record_key **record_line;

void *thread_helper(void *num_threads)
{
    
}



int main(int argc, char **argv)
{
    if (argc != 4)
    {
        printf("Usage: %s input output numThreads\n", argv[0]);
        return 1;
    }

    char *input_file = argv[1];
    char *output_file = argv[2];
    int numThreads = atoi(argv[3]);

    FILE *fp = fopen(input_file, "r");
    if (fp == NULL)
    {
        perror("Input file error");
        return -1;
    }
    struct stat file_s;
    stat(input_file, &file_s);
    int fileSize = (int)file_s.st_size;

    num_records = fileSize/RECORD;

    record_line = malloc(num_records*sizeof(record_key));
    fread(record_line, RECORD, num_records, fp);
    fclose(fp);

    pthread_t thread[numThreads];

    for (int i = 0; i < numThreads; i++)
    {
        pthread_create(&thread[i], NULL, thread_helper, NULL);
    }
    


    
    
    return 0;
}