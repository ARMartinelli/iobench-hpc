#include <fstream>
#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits>

void read_from_file(int* array, int num_elements, std::string file_name, int rank) {
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_RDLCK;    /* read/write (exclusive) lock */
    lock.l_whence = SEEK_SET; /* base for seek offsets */
    lock.l_start = 0;         /* 1st byte in file */
    lock.l_len = 0;           /* 0 here means 'until EOF' */
    lock.l_pid = getpid();    /* process id */

    int fd; /* file descriptor to identify a file within a process */
    while ((fd = open(file_name.c_str(), O_RDONLY)) < 0);  /* -1 signals an error */
    if (fcntl(fd, F_SETLKW, &lock) < 0)
        std::cout << "reader " << rank << " failed to lock the file" << std::endl;
    read(fd, array, sizeof(int) * num_elements);

    /* Release the lock explicitly. */
    lock.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLK, &lock) < 0)
        std::cout << "reader " << rank << " failed to unlock the file" << std::endl;
    close(fd);
}

int main(int argc, char** argv) {
    int rank, size, *array;
    MPI_Init(&argc, &argv);
    if (argc != 2) {
        std::cout << "input error: number of elements needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    const int num_elements = std::stoi(argv[1]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::string dir_name("./dir_process_" + std::to_string(rank));
    std::string file_name(dir_name + "/output_file_" + std::to_string(rank) + ".txt");
    array = new int[num_elements];
    read_from_file(array, num_elements, file_name, rank);
    int num = 0;
    for (int i = 0; i < num_elements; ++i) {
        if (num > std::numeric_limits<int>::max() - array[i]) {
            num = 0;
        }
        num += array[i];
    }
    std::ofstream output_file(dir_name + "/output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << num;
    output_file.close();
    free(array);

    MPI_Finalize();
    return 0;
}
