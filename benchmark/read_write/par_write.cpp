#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include "../../conf_file_reader/conf_file_reader.hpp"

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

int details_mode(const std::string& file_path, int rank, int size) {
    std::vector<int> conf;
    int* array;
    std::string dir_name("dir_process_" + std::to_string(rank));
    if (!exists_dir(dir_name) && mkdir(dir_name.c_str(), 0775) == -1) {
        std::cout << "writer " << rank << " failed to create directory" << std::endl;
        MPI_Finalize();
        return 1;
    }
    bool res_conf = read_conf_file_writer(file_path, rank, size, conf);
    if (!res_conf) {
        return 1;
    }
    for (int i = 0; i < conf.size(); ++i) {
        int num_elements = conf[i];
        array = new int[num_elements];
        std::string file_name;
        std::fill_n(array, num_elements, rank + 1);
        file_name = dir_name + "/output_writer_" + std::to_string(rank) + "_part" + std::to_string(i) + ".txt";
        write_to_file(array, num_elements, file_name, rank);
        free(array);
    }
    return 0;
}

int batch_mode(const std::string& data_path,
               const std::unordered_map<std::string, std::pair<int, int>>& conf, int rank) {
    const std::string prefix(data_path + "/process_" + std::to_string(rank) + "_");
    std::cout << "batch mode" << std::endl;
    int k = 0, num_elements = 0;
    int* array;
    for (auto &pair : conf) {
        num_elements += pair.second.first * pair.second.second;
    }
    array = new int[num_elements];
    std::fill_n(array, num_elements, rank + 1);
    for (auto &pair : conf) {
        std::string dir_name = prefix + pair.first;
        if (!exists_dir(dir_name) && mkdir(dir_name.c_str(), 0775) == -1) {
            std::cout << "writer " << rank << " failed to create directory " << dir_name << std::endl;
            MPI_Finalize();
            return 1;
        }
        std::string file_name;
        int num_elements_dir = pair.second.second;
        for (int i = 0; i < pair.second.first; ++i) {
            file_name =
                    dir_name + "/file_" + std::to_string(i) + "_writer_" + std::to_string(rank) + ".txt";
            //std::cout << "writer " << std::to_string(rank) << " writing file " << file_name << std::endl;
            write_to_file(array + k, num_elements_dir, file_name, rank);
            k += num_elements_dir;
        }
    }

    free(array);
    return 0;
}

int streaming_mode(const std::string& data_path,
                   const std::unordered_map<std::string, std::pair<int, int>>& conf, int rank) {
    const std::string prefix(data_path + "/process_" + std::to_string(rank) + "_");
    std::cout << "streaming mode" << std::endl;
    for (auto &pair : conf) {
        std::string dir_name = prefix + pair.first;
        if (!exists_dir(dir_name) && mkdir(dir_name.c_str(), 0775) == -1) {
            std::cout << "writer " << rank << " failed to create directory " << dir_name << std::endl;
            MPI_Finalize();
            return 1;
        }
        std::cout << "writer " << std::to_string(rank) << " created dir " << dir_name << std::endl;
        int *array;
        std::string file_name;
        int num_elements = pair.second.second;
        array = new int[num_elements];
        for (int i = 0; i < pair.second.first; ++i) {
            std::fill_n(array, num_elements, rank + 1);
            file_name =
                    dir_name + "/file_" + std::to_string(i) + "_writer_" + std::to_string(rank) + ".txt";
            //std::cout << "writer " << std::to_string(rank) << " writing file " << file_name << std::endl;
            write_to_file(array, num_elements, file_name, rank);
        }
        free(array);

    }
    return 0;
}

int abbr_mode(const std::string& data_path, const std::string& file_path, int rank, int size, bool streaming) {
    std::unordered_map<std::string, std::pair<int, int>> conf;
    bool res_conf = read_conf_dir_file_writer(file_path, rank, size, conf);
    int res;
    if (res_conf) {
        if (streaming)
            res = streaming_mode(data_path, conf, rank);
        else
            res = batch_mode(data_path, conf, rank);

    }
    else {
        std::cout << "error reading conf file" << std::endl;
    }
    return res;
}

int main(int argc, char** argv) {
    int res, rank, size;
    MPI_Init(&argc, &argv);
    if (argc != 5) {
        std::cout << "input error: path where storing data, configuration file, mode flag and streaming flag needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    const std::string data_path(argv[1]);
    const std::string conf_file_path(argv[2]);
    const std::string mode_flag(argv[3]);
    const std::string streaming_flag(argv[4]);
    if (mode_flag == "details") {
        res = details_mode(conf_file_path, rank, size);
    }
    else {
        res = abbr_mode(data_path, conf_file_path, rank, size, streaming_flag == "streaming");
    }
    MPI_Finalize();
    return res;
}