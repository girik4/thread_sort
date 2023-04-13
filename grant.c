#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

// compile:                 gcc -Wall -Werror -pthread -O psort.c -o psort
// generate large file:     dd if=/dev/urandom iflag=fullblock of=test.img bs=1000000 count=1000
// run small file:          ./psort grant.txt out.txt 16
// run large file:          ./psort test.img out.txt 16
// =============================================================================================

typedef struct
{
    char key[5];
    char value[97];
} record;

typedef struct
{
    record *data;
    int start;
    int end;
} ThreadArgs;

record *parsing_input(const char *filename, int *num_records)
{
    FILE *file = fopen(filename, "r");
    if (file == NULL)
    {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }

    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    int record_count = file_size / 100;
    rewind(file);

    record *record_arr = malloc(record_count * sizeof(record));
    if (record_arr == NULL)
    {
        perror("Error allocating memory");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < record_count; i++)
    {
        if (fread(record_arr[i].key, sizeof(char), 4, file) != 4)
        {
            perror("Error reading key");
            exit(EXIT_FAILURE);
        }
        record_arr[i].key[4] = '\0';

        if (fread(record_arr[i].value, sizeof(char), 96, file) != 96)
        {
            perror("Error reading value");
            exit(EXIT_FAILURE);
        }
        record_arr[i].value[96] = '\0';
    }

    fclose(file);

    *num_records = record_count;
    return record_arr;
}
void merge(record *data, int start, int mid, int end)
{
    int left_size = mid - start + 1;
    int right_size = end - mid;

    record *left = malloc(left_size * sizeof(record));
    record *right = malloc(right_size * sizeof(record));

    for (int i = 0; i < left_size; i++)
    {
        left[i] = data[start + i];
    }

    for (int i = 0; i < right_size; i++)
    {
        right[i] = data[mid + 1 + i];
    }

    int i = 0, j = 0, k = start;
    while (i < left_size && j < right_size)
    {
        if (*(int *)left[i].key <= *(int *)right[j].key)
        {
            data[k++] = left[i++];
        }
        else
        {
            data[k++] = right[j++];
        }
    }

    while (i < left_size)
    {
        data[k++] = left[i++];
    }

    while (j < right_size)
    {
        data[k++] = right[j++];
    }

    free(left);
    free(right);
}

record *merge_sort(record *arr, int start, int end)
{
    if (start < end)
    {
        int mid = start + (end - start) / 2;
        merge_sort(arr, start, mid);
        merge_sort(arr, mid + 1, end);
        merge(arr, start, mid, end);
    }
    return arr;
}

void *thread_merge_sort(void *arg)
{
    ThreadArgs *thread_args = (ThreadArgs *)arg;
    merge_sort(thread_args->data, thread_args->start, thread_args->end);
    return NULL;
}

void merge_sections(record *data, int num_threads, ThreadArgs *thread_args)
{
    if (num_threads <= 1)
        return;

    int num_active_threads = num_threads;

    while (num_active_threads > 1)
    {
        int num_pairs = (num_active_threads + 1) / 2;

        for (int i = 0; i < num_pairs; i++)
        {
            int first = i * 2;
            int second = i * 2 + 1;

            if (second < num_active_threads)
            {
                int start = thread_args[first].start;
                int mid = thread_args[first].end;
                int end = thread_args[second].end;
                merge(data, start, mid, end);
                thread_args[i] = (ThreadArgs){.data = data, .start = start, .end = end};
            }
            else
            {
                thread_args[i] = thread_args[first];
            }
        }

        num_active_threads = num_pairs;
    }
}

void store_output(const char *output_file, record *array_records, int num_records)
{
    FILE *file = fopen(output_file, "w");
    if (file == NULL)
    {
        perror("Error opening output file");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_records; i++)
    {
        fprintf(file, "%s%s", array_records[i].key, array_records[i].value);
    }

    fclose(file);

    int output_fd = open(output_file, O_WRONLY);
    if (output_fd == -1)
    {
        perror("Error opening output file for fsync");
        exit(EXIT_FAILURE);
    }

    if (fsync(output_fd) == -1)
    {
        perror("Error syncing output file to disk");
        close(output_fd);
        exit(EXIT_FAILURE);
    }

    close(output_fd);
}

// ==================== Print for sanity check, before sorting==============
// fprintf(stdout, "Before sorting:\nNumber of records: %d\n", num_records);
// for (int i = 0; i < num_records; i++)
// {
//     printf("Record %d:\n", i + 1);
//     printf("Key: %s\n", array_records[i].key);
//     printf("Value: %s\n", array_records[i].value);
// }
// =========================================================================

// ==================== Print for sanity check, after sorting==============
// fprintf(stdout, "\n\nAfter sorting:\nNumber of records: %d\n", num_records);
// for(int i = 0; i < num_records; i++){
//     printf("Record %d:\n", i + 1);
//     printf("Key: %s\n", array_records[i].key);
//     printf("Value: %s\n", array_records[i].value);
// }
// ========================================================================

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        perror("Incorrect number of arguments");
        exit(EXIT_FAILURE);
    }

    int num_records;
    record *array_records = parsing_input(argv[1], &num_records);

    int num_threads = atoi(argv[3]);
    if (num_threads < 1)
    {
        perror("Invalid number of threads");
        exit(EXIT_FAILURE);
    }

    // struct timeval start_time, end_time;
    // gettimeofday(&start_time, NULL);
    ThreadArgs *thread_args = malloc(num_threads * sizeof(ThreadArgs));
    int records_per_thread = num_records / num_threads;

    for (int i = 0; i < num_threads; i++)
    {
        thread_args[i].data = array_records;
        thread_args[i].start = i * records_per_thread;
        int is_last_thread = (i == num_threads - 1);
        int last_record_index;

        if (is_last_thread)
        {
            last_record_index = num_records - 1;
        }
        else
        {
            last_record_index = (i + 1) * records_per_thread - 1;
        }

        thread_args[i].end = last_record_index;
    }

    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        pthread_create(&threads[i], NULL, thread_merge_sort, &thread_args[i]);
    }

    for (int i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
    }

    merge_sections(array_records, num_threads, thread_args);

    // gettimeofday(&end_time, NULL);

    // long elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1e6 + (end_time.tv_usec - start_time.tv_usec);

    // printf("Time taken for sorting (%d threads): %ld miliseconds\n", num_threads, elapsed_time / 100);

    store_output(argv[2], array_records, num_records);

    free(array_records);
    free(thread_args);
    free(threads);

    return 0;
}