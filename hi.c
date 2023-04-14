#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <sys/time.h>

int NUM_THREADS = 0;
int RECORD_SIZE = 100;
int NUM_RECORDS = 0;
int REC_PER_THREAD = 0;
int OFFSET = 0;
char *data;

void merge(char* data, int left, int middle, int right);


void merge_sections_of_array(char * data, int number, int aggregation) {
    for(int i = 0; i < number; i = i + 2) {
        int left = i * (REC_PER_THREAD * aggregation);
        int right = ((i + 2) * REC_PER_THREAD * aggregation) - 1;
        int middle = left + (REC_PER_THREAD * aggregation) - 1;
        if (right >= NUM_RECORDS) {
            right = NUM_RECORDS - 1;
        }
        merge(data, left, middle, right);
    }
    if (number / 2 >= 1) {
        merge_sections_of_array(data, number / 2, aggregation * 2);
    }
}

int key(const char *record) {
    return *(int *)record;
}

void merge(char* data, int left, int middle, int right) {
    int i = 0;
    int j = 0;
    int k = left;
    
    int left_length = middle - left + 1;
    int right_length = right - middle;
    
    char *left_array = (char *)malloc(left_length * RECORD_SIZE);
    char *right_array = (char *)malloc(right_length * RECORD_SIZE);
    
    memcpy(left_array, &data[left * RECORD_SIZE], left_length * RECORD_SIZE);
    memcpy(right_array, &data[(middle + 1) * RECORD_SIZE], right_length * RECORD_SIZE);

    while (i < left_length && j < right_length) {
        if (key((const char *)&left_array[i * RECORD_SIZE]) <= key((const char *)&right_array[j * RECORD_SIZE])) {
            memcpy(&data[k * RECORD_SIZE], &left_array[i * RECORD_SIZE], RECORD_SIZE);
            i++;
        } else {
            memcpy(&data[k * RECORD_SIZE], &right_array[j * RECORD_SIZE], RECORD_SIZE);
            j++;
        }
        k++;
    }

    while (i < left_length) {
        memcpy(&data[k * RECORD_SIZE], &left_array[i * RECORD_SIZE], RECORD_SIZE);
        i++;
        k++;
    }

    while (j < right_length) {
        memcpy(&data[k * RECORD_SIZE], &right_array[j * RECORD_SIZE], RECORD_SIZE);
        j++;
        k++;
    }

    free(left_array);
    free(right_array);
   

}

void merge_sort(char *data, int left, int right) {
    if (left < right) {
        int middle = left + (right - left) / 2;
        merge_sort(data, left, middle);
        merge_sort(data, middle + 1, right);
        merge(data, left, middle, right);
    }
}

void *thread_merge_sort(void* arg)
{
    int thread_id = (long)arg;
    int left = thread_id * (REC_PER_THREAD);
    int right = (thread_id + 1) * (REC_PER_THREAD) - 1;
    if (thread_id == NUM_THREADS - 1) {
        right += OFFSET;
    }
    int middle = left + (right - left) / 2;
    if (left < right) {
      merge_sort(data, left, right);
      merge_sort(data, middle + 1, right);
      merge(data, left, middle, right);
 
    }
    
    return NULL;
}


int main(int argc, char *argv[]) {

  struct timeval  start, end;
  double time_spent;

  if (argc != 4) {
        printf("argc != 4\n");
        return 1;
    }

    char *input_file = argv[1];
    char *output_file = argv[2];
    NUM_THREADS = atoi(argv[3]);

    FILE *in = fopen(input_file, "r");
    fseek(in, 0, SEEK_END);
    long file_size = ftell(in);
    fseek(in, 0, SEEK_SET);

    NUM_RECORDS = file_size / RECORD_SIZE;
    data = malloc(file_size);
    if (fread(data, RECORD_SIZE, NUM_RECORDS, in) != NUM_RECORDS) {
        perror("Error reading input file\n");
        fclose(in);
        free(data);
        return 1;
    }

    fclose(in);

    REC_PER_THREAD = NUM_RECORDS/NUM_THREADS;
    OFFSET = NUM_RECORDS%NUM_THREADS;

    pthread_t threads[NUM_THREADS];
    gettimeofday(&start, NULL);

    for (long i = 0; i < NUM_THREADS; i ++) {
      pthread_create(&threads[i], NULL, thread_merge_sort, (void *)i);
        
    }
    
    for(long i = 0; i < NUM_THREADS; i++) {
      pthread_join(threads[i], NULL);
    }

    
    merge_sections_of_array(data, NUM_THREADS, 1);
    gettimeofday(&end, NULL);
    time_spent = ((double) ((double) (end.tv_usec - start.tv_usec) / 1000000 +
                            (double) (end.tv_sec - start.tv_sec)));
    printf("Time taken for execution: %f seconds\n", time_spent);


    FILE *out = fopen(output_file, "w");
    if (fwrite(data, RECORD_SIZE, NUM_RECORDS, out) != NUM_RECORDS) {
        perror("Error writing to output file\n");
        fclose(out);
        free(data);
        return 1;
    }

    fsync(fileno(out));
    fclose(out);
    free(data);

    return 0;
}
