#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include<sys/stat.h>
#include <sys/mman.h>
#include <string.h>
 #include <sys/sysinfo.h>

#define KEY_S 4
#define RECORD_S 96
#define RECORD 100

int num_records; // number of record
struct record_key *array;

struct record_key
{
    char key[KEY_S];
    char record[RECORD_S];
} ;

void merge(struct record_key *array, int left, int mid, int right){
    int lsize = mid - left + 1;
    int rsize = right - mid;

    struct record_key *left_side = malloc(lsize*sizeof(struct record_key));
    struct record_key *right_side = malloc(rsize*sizeof(struct record_key));
    for (int i = 0; i < lsize; i++){
        memcpy(&(left_side[i]), &(array[left+i]), 100);
    }
    for (int i = mid; i < rsize; i++){
        memcpy(&(right_side[i]), &(array[mid+i+1]), 100);
    }

    // merge two half
    int i = 0;
    int j = 0;
    int k = 0;
    while(i < lsize && j < rsize){
    //printf("i %d ; j %d\n", i,j);
    if (left_side[i].key < right_side[j].key){
      array[left + k] = left_side[i];
      i++; 
    } else{
      array[left + k] =right_side[j];
      j++; 
    }
    k++;
  }

    // add remaining elements if possible/needed
  while (i < lsize){
    array[left + k] = left_side[i];
    i++; 
    k++; 
  }
  while (j < rsize){
    array[left + k] = right_side[j];
    j++; 
    k++; 
  }


  free(left_side);
  free(right_side);
}

void mergeSort(struct record_key *array, int left, int right){
    int mid = left + (right - left) / 2;
    if (left < right){
        mergeSort(array, left, mid);
        mergeSort(array, mid+1, right);
        merge(array, left, mid, right);
    }
}

void mergeAll(struct record_jet *array, int numPartitions, int partition){
    int num_processes = get_nprocs();
    int num_entries = (num_records/num_processes);
    pthread_t threads[numPartitions/2];
    for (int i = 0; i < numPartitions; i+=2){
        int left = i * num_entries * partition;
        int right = ((i + 2) *num_entries * partition);
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

    char *input_file = argv[1];
   // char *output_file = argv[2];
    int numThreads = atoi(argv[3]);

    // trying to open input file
    FILE *fp = fopen(input_file, "r");
    if (fp == NULL)
    {
        perror("Input file error");
        return -1;
    }

    // check for valid number of threads
    if (numThreads < 1 || numThreads > get_nprocs()){
        fprintf(stderr, "An error has occurred\n");
    }

    int fd = fileno(fp);
    struct stat file_s;
    stat(input_file, &file_s);

    // checking size if input file
    int size = (int)file_s.st_size;
    if (size <= 0 || size % 100 != 0){
        fprintf(stderr, "An error has occurred\n");
    }

    // grab the number of records
    num_records = size/RECORD;

    // fill in our array to be sorted
    array = malloc(num_records*sizeof(struct record_key));

    char* data = mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    for(int i = 0; i < num_records; i++){
        // putting memory map data (every 100 bytes) into record_key
        struct record_key* rec = (struct record_key*)(data + (i * 100));
        memcpy(&(array[i]), rec, sizeof(struct record_key));
    }
    fclose(fp);


    

    // create the thread pool
 //   pthread_t thread[numThreads];

    // start the psort
    // for (int i = 0; i < numThreads; i++)
    // {
    //     pthread_create(&thread[i], NULL, thread_helper, NULL);
    // }
    
    return 0;
}