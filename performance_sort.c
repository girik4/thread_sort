#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
// gcc -Wall -Werror -pthread -O psort.c -o psort
// gcc -Wall -Werror -O performance_sort.c -o performance_sort
// ./performance_sort test.img output.txt
int main(int argc, char *argv[])
{

    const char *input_file = argv[1];
    const char *output_file = argv[2];

    int numThreads[] = {1, 2, 4, 8, 12, 16};
    for (int i = 0; i < sizeof(numThreads) / sizeof(numThreads[0]); i++)
    {
        char command[256];
        snprintf(command, sizeof(command), "./psort %s %s %d ", input_file, output_file, numThreads[i]);
        int ret = system(command);
        if (ret == -1)
        {
            fprintf(stderr, "system() failed\n");
            exit(EXIT_FAILURE);
        }
    }
}