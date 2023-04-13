#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <unistd.h>

#define KEY_S 4
#define RECORD_S 96
#define RECORD 100

int num_records;
struct record *array;
int num_threads;

struct record {
    char key[KEY_S+1];
    char record[RECORD_S+1];
};

typedef struct {
    struct record_key *data;
    int start;
    int end;
} ThreadArgs;

void merge(struct record *data, int start, int mid, int end)
{
    int left_size = mid - start + 1;
    int right_size = end - mid;

    struct record *left = malloc(left_size * sizeof(struct record));
    struct record *right = malloc(right_size * sizeof(struct record));

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

struct record *merge_sort(struct record *arr, int start, int end)
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

void merge_sections(struct record *data, int num_threads, ThreadArgs *thread_args)
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

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("Usage: %s input output numThreads\n", argv[0]);
        return 1;
    }

    char *input_file = argv[1];
    char *output_file = argv[2];
    num_threads = atoi(argv[3]);

    FILE *fp = fopen(input_file, "r");
    if (fp == NULL) {
        perror("Input file error");
        exit(0);
    }

    if (num_threads < 1 || num_threads > get_nprocs()) {
        fprintf(stderr, "Invalid number of threads\n");
        exit(0);
    }

    struct stat file_s;
    stat(input_file, &file_s);

    int size = (int)file_s.st_size;
    if (size <= 0 || size % RECORD != 0) {
        fprintf(stderr, "Invalid input file size\n");
        exit(0);
    }

    int total_records = size / RECORD;
    num_records = total_records;

    array = malloc(total_records * sizeof(struct record));

    for (int i = 0; i < total_records; i++) {
        if (fread(array[i].key, sizeof(char), KEY_S, fp) != KEY_S) {
            exit(0);
        }
        array[i].key[KEY_S] = '\0';
        if (fread(array[i].record, sizeof(char), RECORD_S, fp) != RECORD_S) {
            exit(0);
        }
        array[i].record[RECORD_S] = '\0';
    }

    fclose(fp);

    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
    ThreadArgs *thread_args = malloc(num_threads * sizeof(ThreadArgs));
    int records_per_thread = num_records / num_threads;

    for (int i = 0; i < num_threads; i++) {
        thread_args[i].data = array;
        thread_args[i].start = i * records_per_thread;
        thread_args[i].end = (i == num_threads - 1) ? num_records : (i + 1) * records_per_thread;
        pthread_create(&threads[i], NULL, thread_merge_sort, &thread_args[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    merge_sections(array, num_threads, thread_args);

    struct record *sorted_records = malloc(num_records * sizeof(struct record));
    int *indices = malloc(num_threads * sizeof(int));
    for (int i = 0; i < num_threads; i++) {
        indices[i] = thread_args[i].start;
    }

    for (int i = 0; i < num_records; i++) {
        int min_idx = -1;
        for (int j = 0; j < num_threads; j++) {
            if (indices[j] < thread_args[j].end) {
                if (min_idx == -1 || cmp_records(&array[indices[j]], &array[indices[min_idx]]) < 0) {
                    min_idx = j;
                }
            }
        }
        sorted_records[i] = array[indices[min_idx]];
        indices[min_idx]++;
    }

    FILE *fptr;
    fptr = fopen(output_file, "w");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening output file\n");
        exit(0);
    }
    for (int i = 0; i < total_records; i++) {
        fprintf(fptr, "%s%s", array[i].key, array[i].record);
    }
    fsync(fileno(fptr));
    fclose(fptr);
    return 0;
}