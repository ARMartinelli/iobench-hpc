#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

void write_to_file(int* matrix, int num_elements, const std::string& file_name, int rank) {
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    int fd;
    if ((fd = open(file_name.c_str(), O_WRONLY | O_CREAT, 0664)) == -1) {
        std::cout << "writer " << rank << " error opening file" << std::endl;
        MPI_Finalize();
        exit(1);
    }
    // lock in exclusive mode
    lock.l_type = F_WRLCK;
    // lock entire file
    lock.l_whence = SEEK_SET; // offset base is start of the file
    lock.l_start = 0;         // starting offset is zero
    lock.l_len = 0;           // len is zero, which is a special value representing end
    // of file (no matter how large the file grows in future)
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLKW, &lock) == -1) { // F_SETLK doesn't block, F_SETLKW does
        std::cout << "write " << rank << "failed to lock the file" << std::endl;
    }
    write(fd, matrix, sizeof(int) * num_elements);
    // Now release the lock explicitly.
    lock.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLKW, &lock) == - 1) {
        std::cout << "write " << rank << "failed to unlock the file" << std::endl;
    }

    close(fd); // close the file: would unlock if needed
}

bool exists_dir(const std::string &s) {
    struct stat buffer;
    return (stat (s.c_str(), &buffer) == 0);
}

int main(int argc, char** argv) {
    int rank, *array;
    MPI_Init(&argc, &argv);
    if (argc != 2) {
        std::cout << "input error: number of elements needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    const int num_elements = std::stoi(argv[1]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    std::string dir_name("dir_process_" + std::to_string(rank));

    if (!exists_dir(dir_name) && mkdir(dir_name.c_str(), 0775) == -1) {
        std::cout << "writer " << rank << " failed to create directory" << std::endl;
        MPI_Finalize();
        return 1;
    }
    array = new int[num_elements];
    std::fill_n(array, num_elements, rank);
    std::string file_name(dir_name + "/output_file_" + std::to_string(rank) + ".txt");
    write_to_file(array, num_elements, file_name, rank);
    free(array);
    MPI_Finalize();
    return 0;
}