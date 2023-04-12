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

int num_records; // number of record
struct record_key *array;
int remain_work;
int num_processes;

struct record_key
{
    char key[KEY_S];
    char record[RECORD_S];
};

void merge(struct record_key *array, int left, int mid, int right)
{
    int lsize = mid - left + 1;
    int rsize = right - mid;

    struct record_key *left_side = malloc(lsize * sizeof(struct record_key));
    struct record_key *right_side = malloc(rsize * sizeof(struct record_key));
    
    for (int i = 0; i < lsize; i++)
    {
        memcpy(&(left_side[i]), &(array[left + i]), 100);
    }
    for (int i = 0; i < rsize; i++)
    {
        memcpy(&(right_side[i]), &(array[mid + i + 1]), 100);
    }

    // merge two half
    int i = 0;
    int j = 0;
    int k = 0;
    while (i < lsize && j < rsize)
    {
        // printf("i %d ; j %d\n", i,j);
        if (left_side[i].key < right_side[j].key)
        {
            array[left + k] = left_side[i];
            i++;
        }
        else
        {
            array[left + k] = right_side[j];
            j++;
        }
        k++;
    }

    // add remaining elements if possible/needed
    while (i < lsize)
    {
        array[left + k] = left_side[i];
        i++;
        k++;
    }
    while (j < rsize)
    {
        array[left + k] = right_side[j];
        j++;
        k++;
    }

    free(left_side);
    free(right_side);
}

void mergeSort(struct record_key *array, int left, int right)
{
    int mid = left + (right - left) / 2;
    if (left < right)
    {
        mergeSort(array, left, mid);
        mergeSort(array, mid + 1, right);
        merge(array, left, mid, right);
    }
}

void *thread_helper(void *numThreads)
{
    int numEntries = (num_records / num_processes);
    int threads = (long)numThreads;

    int left = threads * numEntries;
    int right = (threads + 1) * numEntries - 1;

    if (threads == num_processes - 1)
    {
        right += remain_work;
    }
    int mid = left + (right - left) / 2;
    // mergesort on 1st half and second half then combine everything together
    if (left < right)
    {
        mergeSort(array, left, mid);
        mergeSort(array, mid + 1, right);
        merge(array, left, mid, right);
    }
    return NULL;
}

void *thread2_helper(void *args)
{
    int *args2 = (int *)args;
    merge(array, args2[0], args2[1], args2[2]);
    free(args);
    return NULL;
}


void mergeAll(struct record_key *array, int numPartitions, int partition)
{
    int num_entries = (num_records / num_processes);
    pthread_t threads[numPartitions / 2];
    for (int i = 0; i < numPartitions; i += 2)
    {
        int left = i * num_entries * partition;
        int right = ((i + 2) * num_entries * partition) - 1;
        int mid = left + (num_entries * partition) - 1;

        // base case
        if (right >= num_records)
        {
            right = num_records - 1;
        }
        if (numPartitions <= 2)
        {
            merge(array, left, mid, right);
        }
        else
        {
            // thread the merge
            int *args = (int *)malloc(3 * sizeof(int));
            args[0] = left;
            args[1] = mid;
            args[2] = right;
            int thread_status = pthread_create(&threads[(int)i / 2], NULL, thread2_helper, (void *)args);
            if (thread_status != 0)
            {
                fprintf(stderr, "error\n");
                exit(0);
            }
        }
    }

    if (numPartitions > 1)
    {
        for (int i = 0; i < numPartitions / 2; i++)
        {
            pthread_join(threads[i], NULL);
        }
    }
    // recursively merge again
    int remainingPartitions = numPartitions / 2;
    if (remainingPartitions >= 1)
    {
        mergeAll(array, remainingPartitions, partition * 2);
    }
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

    int fd = fileno(fp);
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

    char *data = mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    for (int i = 0; i < total_records; i++)
    {
        // putting memory map data (every 100 bytes) into record_key
        struct record_key *rec = (struct record_key *)(data + (i * 100));
        memcpy(&(array[i]), rec, sizeof(struct record_key));
    }
    fclose(fp);

    num_processes = get_nprocs();
    // create the thread pool
    remain_work = total_records % num_processes;
    pthread_t thread[num_processes];

    // start the psort
    for (long i = 0; i < num_processes; i++)
    {
        int thread_status = pthread_create(&thread[i], NULL, thread_helper, (void*) i);
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

    mergeAll(array, num_processes, 1);

    int fptr;
    fptr = open(output_file, O_WRONLY, O_APPEND);
    printf("%d\n", fptr);
    if (fptr == -1)
    {
        fprintf(stderr, "An error has occurred 1\n");
        exit(0);
    }
    for (int i = 0; i < total_records; i++)
    {
        int rc = write(fptr, &(array[i]), sizeof(struct record_key));
        if (rc != 100)
        {
            fprintf(stderr, "An error has occurred 2\n");
            exit(0);
        }
    }
    fsync(fptr);
    close(fptr);
    return 0;
}