#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#define KEY_S 4
#define RECORD_S 96
#define RECORD 100

int num_records;
struct record *array;
int num_threads;

struct record
{
    char key[KEY_S];
    char record[RECORD_S];
};

typedef struct
{
    struct record *array;
    int first;
    int last;
} ThreadArgs;

int compare_keys(const void *a, const void *b)
{
    return (*(int *)a - *(int *)b);
}

void *thread_qsort(void *arg)
{
    ThreadArgs *thread_args = (ThreadArgs *)arg;
    qsort(thread_args->array + thread_args->first, thread_args->last - thread_args->first, sizeof(struct record), compare_keys);
    return NULL;
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
    num_threads = atoi(argv[3]);

    if (num_threads < 1 || num_threads > get_nprocs())
    {
        fprintf(stderr, "Invalid number of threads\n");
        exit(0);
    }

    int fd = open(input_file, O_RDONLY);
    if (fd == -1)
    {
        perror("Input file error");
        exit(0);
    }

    struct stat file_s;
    fstat(fd, &file_s);

    int size = (int)file_s.st_size;
    if (size <= 0 || size % 100 != 0)
    {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }

    int total_records = size / RECORD;
    num_records = total_records;

    void *mapped_data = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped_data == MAP_FAILED)
    {
        perror("mmap error");
        exit(0);
    }

    array = malloc(total_records * sizeof(struct record));

    for (int i = 0; i < total_records; i++)
    {
        memcpy(array[i].key, mapped_data + i * RECORD, KEY_S);
       // array[i].key[KEY_S] = '\0';
        memcpy(array[i].record, mapped_data + i * RECORD + KEY_S, RECORD_S);
        // array[i].record[RECORD_S] = '\0';
    }

    munmap(mapped_data, size);
    close(fd);

    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];
    int records_per_thread = num_records / num_threads;

    for (int i = 0; i < num_threads; i++)
    {
        thread_args[i].array = array;
        thread_args[i].first = i * records_per_thread;
        if (i == num_threads - 1)
        {
            thread_args[i].last = num_records;
        }
        else
        {
            thread_args[i].last = (i + 1) * records_per_thread;
        }
        pthread_create(&threads[i], NULL, thread_qsort, &thread_args[i]);
    }

    for (int i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
    }

    struct record *sorted_records = malloc(num_records * sizeof(struct record));

    for (int i = 0, min_idx; i < num_records; i++)
    {
        min_idx = -1;
        for (int j = 0; j < num_threads; j++)
        {
            if (thread_args[j].first < thread_args[j].last)
            {
                if (min_idx == -1 || compare_keys(&array[thread_args[j].first], &array[thread_args[min_idx].first]) < 0)
                {
                    min_idx = j;
                }
            }
        }
        sorted_records[i] = array[thread_args[min_idx].first++];
    }

    // struct record *sorted_records = malloc(num_records * sizeof(struct record));
    // int *indices = malloc(num_threads * sizeof(int));
    // for (int i = 0; i < num_threads; i++) {
    //     indices[i] = thread_args[i].start;
    // }

    // for (int i = 0; i < num_records; i++) {
    //     int min_idx = -1;
    //     for (int j = 0; j < num_threads; j++) {
    //         if (indices[j] < thread_args[j].end) {
    //             if (min_idx == -1 || compare_keys(&array[indices[j]], &array[indices[min_idx]]) < 0) {
    //                 min_idx = j;
    //             }
    //         }
    //     }
    //     sorted_records[i] = array[indices[min_idx]];
    //     indices[min_idx]++;
    // }

    int out_fd = open(output_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (out_fd == -1)
    {
        perror("Error opening output file");
        exit(0);
    }

    // Set the output file size
    if (ftruncate(out_fd, size) == -1)
    {
        perror("Error setting output file size");
        exit(0);
    }

    void *out_mapped_data = mmap(NULL, size, PROT_WRITE, MAP_SHARED, out_fd, 0);
    if (out_mapped_data == MAP_FAILED)
    {
        perror("mmap error");
        exit(0);
    }

    for (int i = 0; i < total_records; i++)
    {
        memcpy(out_mapped_data + i * RECORD, sorted_records[i].key, KEY_S);
        memcpy(out_mapped_data + i * RECORD + KEY_S, sorted_records[i].record, RECORD_S);
    }

    // Ensure the data is written to the file
    msync(out_mapped_data, size, MS_SYNC);

    munmap(out_mapped_data, size);
    close(out_fd);

    return 0;
}