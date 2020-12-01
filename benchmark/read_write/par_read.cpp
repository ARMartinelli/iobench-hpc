#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits>
#include "../../conf_file_reader/conf_file_reader.hpp"

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

void use_data(int* array, int num_elements, int rank, int& sum) {
    for (int i = 0; i < num_elements; ++i) {
        if (sum > std::numeric_limits<int>::max() - array[i]) {
            sum = 0;
        }
        sum += array[i];
    }

}

bool details_mode(std::string file_path, int rank, int size) {
    std::unordered_map<int, std::unordered_map<int, int>> conf;
    std::string dir_name;
    std::string file_name_prefix;
    std::string file_name;
    bool res = true;
    if (read_conf_file_reader(file_path, rank, size, conf)) {
        int sum = 0;
        for (std::pair<int, std::unordered_map<int, int>> p1 : conf) {
            for (std::pair<int, int> p2 : p1.second) {
                int writer_rank = p1.first;
                int num_elements = p2.second;
                int *array = new int[num_elements];
                dir_name = "./dir_process_" + std::to_string(writer_rank);
                file_name_prefix = dir_name + "/output_writer_" + std::to_string(writer_rank);
                file_name = file_name_prefix + "_part" + std::to_string(p2.first) + ".txt";
                std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                read_from_file(array, num_elements, file_name, rank);
                use_data(array, num_elements, rank, sum);
                free(array);
            }
        }
        std::ofstream output_file("output_read_matrixes_" + std::to_string(rank) + ".txt");
        output_file << "result of reader " << rank << ": " << sum << "\n";
        output_file.close();
    }
    else {
        res = false;
    }
    return res;
}

bool abbr_mode(std::string file_path, int rank, int size) {
    std::unordered_map<int, std::unordered_map<std::string, std::pair<int, int>>> conf;
    std::string dir_name;
    std::string file_name_prefix;
    std::string file_name;
    bool res = true;
    if (read_conf_dir_file_reader(file_path, rank, size, conf)) {
        int sum = 0;
        for (std::pair<int, std::unordered_map<std::string, std::pair<int, int>>> p1 : conf) {
            for (std::pair<std::string, std::pair<int, int>> p2 : p1.second) {
                int writer_rank = p1.first;
                int num_files_dir = p2.second.first;
                int num_elements = p2.second.second;
                int *array = new int[num_elements];
                dir_name = "./process_" + std::to_string(writer_rank) + "_" + p2.first;
                for (int i = 0; i < num_files_dir; ++i) {
                    file_name_prefix = dir_name + "/file_" + std::to_string(i);
                    file_name = file_name_prefix + "_writer_" + std::to_string(writer_rank) +  ".txt";
                    std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                    read_from_file(array, num_elements, file_name, rank);
                    use_data(array, num_elements, rank, sum);
                }

                free(array);
            }
        }
        std::ofstream output_file("output_read_matrixes_" + std::to_string(rank) + ".txt");
        output_file << "result of reader " << rank << ": " << sum << "\n";
        output_file.close();
    }
    else {
        res = false;
    }
    return res;
}

 int main(int argc, char** argv) {
    int rank, size;
    std::string dir_name;
    std::string file_name;
    bool res;
    MPI_Init(&argc, &argv);
    if (argc != 3) {
        std::cout << "input error: configuration file and mode flag needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    std::string file_path(argv[1]);
    std::string mode_flag(argv[2]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (mode_flag == "details") {
        res = details_mode(file_path, rank, size);
    }
    else {
        res = abbr_mode(file_path, rank, size);
    }

    MPI_Finalize();
    return res;
}
