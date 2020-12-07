#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits>
#include "../../conf_file_reader/conf_file_reader.hpp"

bool read_from_file(int* array, int num_elements, const std::string& file_name, int rank) {
    bool res = true;
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_RDLCK;    /* read/write (exclusive) lock */
    lock.l_whence = SEEK_SET; /* base for seek offsets */
    lock.l_start = 0;         /* 1st byte in file */
    lock.l_len = 0;           /* 0 here means 'until EOF' */
    lock.l_pid = getpid();    /* process id */

    int fd; /* file descriptor to identify a file within a process */
    while ((fd = open(file_name.c_str(), O_RDONLY)) < 0);  /* -1 signals an error */
    if (fcntl(fd, F_SETLKW, &lock) < 0) {
        std::cout << "reader " << rank << " failed to lock the file" << std::endl;
        close(fd);
        return false;
    }
    int read_res = read(fd, array, sizeof(int) * num_elements);
    if (read_res == 0) {
        std::cout << "reader " << rank << " reached eof while reading the file" << std::endl;
        res = false;
    }
    else if (read_res < 0) {
        std::cout << "reader " << rank << " failed to read the file" << std::endl;
        res = false;
    }
    /* Release the lock explicitly. */
    lock.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLK, &lock) < 0) {
        std::cout << "reader " << rank << " failed to unlock the file" << std::endl;
        res = false;
    }
    close(fd);
    return res;
}

void use_data(int* array, int num_elements, int& sum) {
    for (int i = 0; i < num_elements; ++i) {
        if (sum > std::numeric_limits<int>::max() - array[i]) {
            sum = 0;
        }
        sum += array[i];
    }

}

bool details_mode(const std::string& file_path, int rank, int size) {
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
                //std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                read_from_file(array, num_elements, file_name, rank);
                use_data(array, num_elements, sum);
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

bool streaming_mode(const std::string &data_path, int rank,
                    const std::unordered_map<int, std::unordered_map<std::string, std::pair<int, int>>>& conf) {
    std::string dir_name;
    std::string file_name_prefix;
    std::string file_name;
    int sum = 0;
    for (std::pair<int, std::unordered_map<std::string, std::pair<int, int>>> p1 : conf) {
        for (std::pair<std::string, std::pair<int, int>> p2 : p1.second) {
            int writer_rank = p1.first;
            int num_files_dir = p2.second.first;
            int num_elements = p2.second.second;
            int *array = new int[num_elements];
            dir_name = data_path + "/process_" + std::to_string(writer_rank) + "_" + p2.first;
            for (int i = 0; i < num_files_dir; ++i) {
                file_name_prefix = dir_name + "/file_" + std::to_string(i);
                file_name = file_name_prefix + "_writer_" + std::to_string(writer_rank) +  ".txt";
                //std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                if (read_from_file(array, num_elements, file_name, rank)) {
                    use_data(array, num_elements, sum);
                }
                else {
                    --i;
                }
            }

            free(array);
        }
    }
    std::ofstream output_file(data_path + "/output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << sum << "\n";
    output_file.close();
    return true;
}

bool batch_mode(const std::string &data_path,
                int rank, const std::unordered_map<int, std::unordered_map<std::string, std::pair<int, int>>>& conf) {
    std::string dir_name;
    std::string file_name_prefix;
    std::string file_name;
    int sum = 0, num_elements = 0, k = 0;
    for (auto &pair : conf) {
        for (auto& pair2 : pair.second) {
            num_elements += pair2.second.first * pair2.second.second;
        }
    }

    int *array = new int[num_elements];
    for (std::pair<int, std::unordered_map<std::string, std::pair<int, int>>> p1 : conf) {
        for (std::pair<std::string, std::pair<int, int>> p2 : p1.second) {
            int writer_rank = p1.first;
            int num_files_dir = p2.second.first;
            int num_elements_dir = p2.second.second;
            dir_name = data_path + "/process_" + std::to_string(writer_rank) + "_" + p2.first;
            for (int i = 0; i < num_files_dir; ++i) {
                file_name_prefix = dir_name + "/file_" + std::to_string(i);
                file_name = file_name_prefix + "_writer_" + std::to_string(writer_rank) +  ".txt";
                //std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                if (read_from_file(array + k, num_elements_dir, file_name, rank))
                    k += num_elements_dir;
                else
                    --i;

            }
        }
    }
    use_data(array, num_elements, sum);
    free(array);
    std::ofstream output_file(data_path + "/output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << sum << "\n";
    output_file.close();
    return true;
}

bool abbr_mode(const std::string &data_path,
               const std::string &file_path, int rank, int size, bool streaming) {
    std::unordered_map<int, std::unordered_map<std::string, std::pair<int, int>>> conf;
    bool res;
    if (read_conf_dir_file_reader(file_path, rank, size, conf)) {
        if (streaming) {
            res = streaming_mode(data_path, rank, conf);
        }
        else {
            res = batch_mode(data_path, rank, conf);
        }
    }
    else {
        res = false;
    }
    return res;
}

 int main(int argc, char** argv) {
    int rank, size;
    bool res;
    MPI_Init(&argc, &argv);
    if (argc != 5) {
        std::cout << "input error: path where storing data, configuration file, mode flag and streaming flag needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    const std::string data_path(argv[1]);
    const std::string file_path(argv[2]);
    const std::string mode_flag(argv[3]);
    const std::string streaming_flag(argv[4]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (mode_flag == "details") {
        res = details_mode(file_path, rank, size);
    }
    else {
        res = abbr_mode(data_path, file_path, rank, size, streaming_flag == "streaming");
    }

    MPI_Finalize();
    if (res)
        return 0;
    else
        return 1;
}
