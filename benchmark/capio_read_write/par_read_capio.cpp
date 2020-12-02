#include <iostream>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits>
#include "../../conf_file_reader/conf_file_reader.hpp"
#include "../../../capio/capio_ordered/capio_ordered.hpp"

void use_data(int* array, int num_elements, int& sum) {
    for (int i = 0; i < num_elements; ++i) {
        if (sum > std::numeric_limits<int>::max() - array[i]) {
            sum = 0;
        }
        sum += array[i];
    }

}

bool streaming_mode(capio_ordered& capio,int rank,
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
            dir_name = "./process_" + std::to_string(writer_rank) + "_" + p2.first;
            for (int i = 0; i < num_files_dir; ++i) {
                file_name_prefix = dir_name + "/file_" + std::to_string(i);
                file_name = file_name_prefix + "_writer_" + std::to_string(writer_rank) +  ".txt";
                std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                capio.capio_recv(array, num_elements);
                use_data(array, num_elements, sum);
            }

            free(array);
        }
    }
    std::ofstream output_file("output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << sum << "\n";
    output_file.close();
    return true;
}

bool batch_mode(capio_ordered& capio,
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
            dir_name = "./process_" + std::to_string(writer_rank) + "_" + p2.first;
            for (int i = 0; i < num_files_dir; ++i) {
                file_name_prefix = dir_name + "/file_" + std::to_string(i);
                file_name = file_name_prefix + "_writer_" + std::to_string(writer_rank) +  ".txt";
                std::cout << "reader " << rank << "reading file: " << file_name << std::endl;
                capio.capio_recv(array + k, num_elements_dir);
                k += num_elements_dir;
            }
        }
    }
    use_data(array, num_elements, sum);
    free(array);
    std::ofstream output_file("output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << sum << "\n";
    output_file.close();
    return true;
}

bool abbr_mode(capio_ordered& capio, const std::string &file_path, int rank, int size, bool streaming) {
    std::unordered_map<int, std::unordered_map<std::string, std::pair<int, int>>> conf;
    bool res;
    if (read_conf_dir_file_reader(file_path, rank, size, conf)) {
        if (streaming) {
            res = streaming_mode(capio, rank, conf);
        }
        else {
            res = batch_mode(capio, rank, conf);
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
        std::cout << "input error: capio configuration file, iobench configuration file, mode flag and streaming flag needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    std::string capio_config_path(argv[1]);
    std::string iobench_config_path(argv[2]);
    std::string mode_flag(argv[3]);
    std::string streaming_flag(argv[4]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    capio_ordered capio(true, false, rank, capio_config_path);
    res = abbr_mode(capio, iobench_config_path, rank, size, streaming_flag == "streaming");
    MPI_Finalize();
    if (res)
        return 0;
    else
        return 1;
}