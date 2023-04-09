#include <sys/sysinfo.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/mman.h>
#include <sys/stat.h>

struct record **array;
int numLines;
int num_proc;
int remainingWork;

struct record {
  int key;
  char *remainder;
};
void merge(struct record **array, int l, int mid, int r){
  int leftSize = mid - l + 1;
  int rightSize = r - mid;

  // declare left half and right half array
  struct record *leftHalf[leftSize];
  struct record *rightHalf[rightSize];

  // populate arrays
  for (int i = 0; i <  leftSize; i++){
    leftHalf[i] = array[l+i];
  }
  for(int i = 0; i < rightSize; i++){
    rightHalf[i] = array[mid + i + 1];
  }
  int i = 0; int j = 0; int k = 0;
  // merge both halves ie overwrite array starting from left side to right side
  while(i < leftSize && j < rightSize){
    if (leftHalf[i]->key < rightHalf[j]->key){
      array[l + k] = leftHalf[i];
      i++;
    } else{
      array[l + k] = rightHalf[j];
      j++;
    }
    k++;
  }

  // add remaining elements if possible/needed
  while (i < leftSize){
    array[l + k] = leftHalf[i];
    i++; k++;
  }
  while (j < rightSize){
    array[l + k] = rightHalf[j];
    j++; k++;
  }
}

void mergeSort(struct record **array, int l, int r){
  int mid = l + (r - l)/ 2;
  if (l < r){
    mergeSort(array,l,mid); // recursively merge 1st & second half
    mergeSort(array,mid+1,r);
    merge(array,l,mid,r);
  }
  //return NULL;
}
void *thread_helper(void *numThreads){
  int numEntries = (numLines/num_proc);
  int threads = (long)numThreads;

  int l = threads * numEntries;
  int r = (threads + 1) * numEntries- 1;

  if (threads == num_proc - 1){
    r+=remainingWork;
  }
  int mid = l + (r-l) / 2;
  // mergesort on 1st half and second half then combine everything together
  if (l < r){
    mergeSort(array,l,mid);
    mergeSort(array,mid+1,r);
    merge(array,l,mid,r);
  }
  return NULL;
}

int main(int argc, char* argv[]){
    // too little arguments
    if (argc != 4){
        return -1;
    }

    FILE *fp;
    char *inputFile;
   // char *outputFile;
    char line[256];
    int num_threads;
    //struct record **array;

    inputFile = argv[1];
   // outputFile = argv[2];
    num_threads = atoi(argv[3]);

    //numLines = 0;
    fp=fopen(inputFile,"r"); // perhaps use open
    if(fp==NULL) {
        printf("File \"%s\" does not exist!\n", inputFile);
    }
    int fd = fileno(fp);

    // get the total size in bytes for input file
    struct stat file_size;
    stat(inputFile,&file_size);
    int size = (int)file_size.st_size; // number of bytes for the file
    printf("size: %d\n", size);
    int totalRecords = size/100; // total number of records
    // potentially check for invalid entry size
    numLines = totalRecords;
    array = malloc(totalRecords*sizeof(struct record*)); // malloc our array

    for(int i = 0; i < totalRecords; i++){
      array[i] = malloc(sizeof(struct record));
    }

    char* rawInput = (char *) mmap(0,size,PROT_READ, MAP_PRIVATE,fd, 0);


    for(int i = 0; i < size-100; i+=100){
        //struct record new_record;
      int key;
      memcpy(&key, rawInput + i, 4);
      //printf("%d\n", key[0]);
      char remainder[97];

      memcpy(remainder, rawInput + i + 4,96);
      remainder[96] = '\0';

      array[i / 100]->remainder = strdup(remainder);
      array[i / 100]->key = key;

      //printf("Not segfault\n");
      //printf("Setting: %d\n", i/100);

       //printf("%d\n", array[i / 100]->key);
        //numLines++;
    }
    fclose(fp);
    
    num_proc = num_threads; //get_nprocs(); // fixed num procs need to change back 
    //printf("%d\n",num_proc);
    int sections = (totalRecords/num_proc);
    //printf("%d\n",sections);
    //int i = 0;
    remainingWork = totalRecords % num_proc;
    pthread_t threads[num_proc];

    for (long i = 0; i < num_proc; i++){
      // do the threading
      int threadStatus = pthread_create(&threads[i], NULL, thread_helper,(void *)i);
    }

    for (int i = 0; i < num_proc; i++){
      pthread_join(threads[i], NULL);
    }

    for (int i = 0; i < totalRecords; i++){
      printf("key: %d\n", array[i]->key);
    }

     // TODO: write to output file
    
    while(i < numLines){
        // current range: i to i + section
        //printf("%d\n", i);
        pthread_t thread_id;
        t_arg curr_arg = {.array = array, .low = i, .high = i + sections - 1};
        pthread_create(&thread_id, NULL, &sort, &curr_arg);
        pthread_join(thread_id, NULL);
        i += sections;
    }
    
     // TODO: write to output file
    
    while(i < numLines){
        // current range: i to i + section
        //printf("%d\n", i);
        pthread_t thread_id;
        t_arg curr_arg = {.array = array, .low = i, .high = i + sections - 1};
        pthread_create(&thread_id, NULL, &sort, &curr_arg);
        pthread_join(thread_id, NULL);
        i += sections;
    }
    
    
    for(int x = 0; x < totalRecords; x++){
      printf("%d\n", array[x].key);
    }

    int pointer_array[num_proc]; // pointer array stores pointer to start of each sorted sublist within array
    int ptrArray_size = num_proc;
    int current = 0;
    for(int k = 0; k < num_proc; k++){
        pointer_array[k] = current;
        printf("%d\n",current);
        current += sections;
    }
    //printf("Notsegfaulting\n");
    struct record *sorted_array;
    sorted_array = malloc(totalRecords*sizeof(struct record));

    // merge num_proc sorted sublists
    while (ptrArray_size > 1){
      for(int i = 0; i < num_proc; i+=2){ // merge every 2 sublists
        int list1Start = pointer_array[i];
        //int list1End = pointer_array[i] + ;       
        int list2Start = NULL;
        if (i + 1 < num_proc){
          list2Start = pointer_array[i+1];
        }
        // merge list 1 & 2 together
       // merge(list1Start, list2Start);
      }
    }
    
    
    for(int m = 0; m < numLines; m++){
        printf("%d\n", sorted_array[m].key);
    }
    */
    //quicksort(array,0,numLines-1);
    for(int x = 0; x < totalRecords; x++){
      printf("%d\n", array[x].key);
    }

    int pointer_array[num_proc]; // pointer array stores pointer to start of each sorted sublist within array
    int ptrArray_size = num_proc;
    int current = 0;
    for(int k = 0; k < num_proc; k++){
        pointer_array[k] = current;
        printf("%d\n",current);
        current += sections;
    }
    //printf("Notsegfaulting\n");
    struct record *sorted_array;
    sorted_array = malloc(totalRecords*sizeof(struct record));

    // merge num_proc sorted sublists
    while (ptrArray_size > 1){
      for(int i = 0; i < num_proc; i+=2){ // merge every 2 sublists
        int list1Start = pointer_array[i];
        //int list1End = pointer_array[i] + ;       
        int list2Start = NULL;
        if (i + 1 < num_proc){
          list2Start = pointer_array[i+1];
        }
        // merge list 1 & 2 together
       // merge(list1Start, list2Start);
      }
    }
    
    
    for(int m = 0; m < numLines; m++){
        printf("%d\n", sorted_array[m].key);
    }
    
    quicksort(array,0,numLines-1);

  return -1;
}