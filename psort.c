#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

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
    struct record *array;
    int first;
    int last;
} ThreadArgs;

int compare_keys(const void *a, const void *b) {
    return (*(int*)a - *(int*)b);
}

void *thread_qsort(void *arg) {
    ThreadArgs *thread_args = (ThreadArgs *)arg;
    qsort(thread_args->array + thread_args->first, thread_args->last - thread_args->first, sizeof(struct record), compare_keys);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("Usage: %s input output numThreads\n", argv[0]);
        return 1;
    }

    char *input_file = argv[1];
    char *output_file = argv[2]; O_RDONLY
    num_threads = atoi(argv[3]);

    int fd = open(input_file, O_RDONLY);
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

    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];
    int records_per_thread = num_records / num_threads;

    for (int i = 0; i < num_threads; i++) {
        thread_args[i].array = array;
        thread_args[i].first = i * records_per_thread;
        if (i == num_threads-1)
        {
            thread_args[i].last = num_records;
        }
        else{
            thread_args[i].last = (i+1) * records_per_thread;
        }
        pthread_create(&threads[i], NULL, thread_qsort, &thread_args[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    struct record *sorted_records = malloc(num_records * sizeof(struct record));

    for (int i = 0, min_idx; i < num_records; i++) {
        min_idx = -1;
        for (int j = 0; j < num_threads; j++) {
            if (thread_args[j].first < thread_args[j].last) {
                if (min_idx == -1 || compare_keys(&array[thread_args[j].first], &array[thread_args[min_idx].first]) < 0) {
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

    FILE *fptr;
    fptr = fopen(output_file, "w");
    if (fptr == NULL) {
        fprintf(stderr, "Error opening output file\n");
        exit(0);
    }
    for (int i = 0; i < total_records; i++) {
        fprintf(fptr, "%s%s", sorted_records[i].key, sorted_records[i].record);
    }
    fsync(fileno(fptr));
    fclose(fptr);
    return 0;
}