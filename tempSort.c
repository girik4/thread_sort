#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <fcntl.h>
#include <unistd.h>

#define KEY_S 4
#define RECORD_S 96
#define RECORD 100

int num_records; // number of records
struct record_key *array;
int remain_work;
int num_processes;

struct record_key {
    char key[KEY_S+1];
    char record[RECORD_S+1];
};

int compare_keys(const void *a, const void *b) {
    return strncmp(((struct record_key *)a)->key, ((struct record_key *)b)->key, KEY_S);
}

void merge(struct record_key *array, int left, int mid, int right) {
    int lsize = mid - left + 1;
    int rsize = right - mid;

    struct record_key *left_side = malloc(lsize * sizeof(struct record_key));
    struct record_key *right_side = malloc(rsize * sizeof(struct record_key));

    memcpy(left_side, &(array[left]), lsize * sizeof(struct record_key));
    memcpy(right_side, &(array[mid + 1]), rsize * sizeof(struct record_key));

    int i = 0, j = 0, k = left;

    while (i < lsize && j < rsize) {
        if (compare_keys(&left_side[i], &right_side[j]) < 0) {
            array[k] = left_side[i];
            i++;
        } else {
            array[k] = right_side[j];
            j++;
        }
        k++;
    }

    while (i < lsize) {
        array[k] = left_side[i];
        i++;
        k++;
    }

    while (j < rsize) {
        array[k] = right_side[j];
        j++;
        k++;
    }

    free(left_side);
    free(right_side);
}

void mergeSort(struct record_key *array, int left, int right) {
    if (left < right) {
        int mid = left + (right - left) / 2;
        mergeSort(array, left, mid);
        mergeSort(array, mid + 1, right);
        merge(array, left, mid, right);
    }
}

void *thread_helper(void *numThreads) {
    int numEntries = (num_records / num_processes);
    int threads = (long)numThreads;

    int left = threads * numEntries;
    int right = (threads + 1) * numEntries - 1;

    if (threads == num_processes - 1) {
        right += remain_work;
    }

    mergeSort(array, left, right);
    return NULL;
}



int main(int argc, char **argv)
{
    // error: invalid number of args
    if (argc != 4)
    {
        printf("Usage: %s input output numThreads\n", argv[0]);
        return 1;
    }

    char *input_file;
    char *output_file;
    input_file = argv[1];
    output_file = argv[2];
    int numThreads = atoi(argv[3]);

    // trying to open input file
    FILE *fp = fopen(input_file, "r");
    if (fp == NULL)
    {
        perror("Input file error");
        exit(0);
    }

    // check for valid number of threads
    if (numThreads < 1 || numThreads > get_nprocs())
    {
        fprintf(stderr, "An error has occurred\n");
    }

    //int fd = fileno(fp);

    struct stat file_s;
    stat(input_file, &file_s);
    
    // checking size if input file
    int size = (int)file_s.st_size;
    if (size <= 0 || size % 100 != 0)
    {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }

    // grab the number of records
    int total_records = size / RECORD;
    num_records = total_records;

    // fill in our array to be sorted
    array = malloc(total_records * sizeof(struct record_key));


    for (int i = 0; i < total_records; i++)
    {
        if (fread(array[i].key, sizeof(char), KEY_S, fp) != KEY_S)
        {
            exit(0);
        }
        array[i].key[KEY_S] = '\0';
        if (fread(array[i].record, sizeof(char), RECORD_S, fp) != RECORD_S)
        {
            exit(0);
        }
        array[i].record[RECORD_S] = '\0';
    }

    

    num_processes = get_nprocs();
    // create the thread pool
    remain_work = total_records % num_processes;
    pthread_t thread[num_processes];

    // start the psort
    for (long i = 0; i < num_processes; i++)
    {
        int thread_status = pthread_create(&thread[i], NULL, thread_helper, (void*)i);
        if (thread_status != 0)
        {
            fprintf(stderr, "An error has occurred\n");
            exit(0);
        }
    }
    
    for (int i = 0; i < num_processes; i++)
    {
        pthread_join(thread[i], NULL);
    }


    FILE *fptr;
    fptr = fopen(output_file, "w");
    if (fptr == NULL)
    {
        fprintf(stderr, "An error has occurred 1\n");
        exit(0);
    }
    for (int i = 0; i < total_records; i++)
    {
        fprintf(fptr, "%s%s", array[i].key, array[i].record);
    }
    fsync(fileno(fptr));
    fclose(fptr);
    
    return 0;
}