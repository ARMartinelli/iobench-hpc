#include <iostream>
#include <mpi.h>
#include "../../conf_file_reader/conf_file_reader.hpp"

#include "../../../capio/capio_ordered/capio_ordered.hpp"

int batch_mode(capio_ordered &capio,
               const std::unordered_map<std::string, std::pair<int, int>>& dirs_info,
               const std::unordered_map<std::string, std::vector<int>>& readers_info, int rank) {
    //std::cout << "batch mode" << std::endl;
    int k = 0, num_elements = 0;
    int* array;
    for (auto &pair : dirs_info) {
            num_elements += pair.second.first * pair.second.second;

    }
    array = new int[num_elements];
    std::fill_n(array, num_elements, rank + 1);
    for (auto& pair : readers_info) {
        std::string dir(pair.first);
        int num_files = dirs_info.at(dir).first;
        int num_elements_dir = dirs_info.at(dir).second;
        for (int reader_rank: pair.second) {
            std::cout << "writer " << rank << "sending dir " << dir << "to reader " << reader_rank << std::endl;
            for (int i = 0; i < num_files; ++i) {
                //std::cout << "writer " << std::to_string(rank) << " writing file " << file_name << std::endl;
                capio.capio_send(array + k, num_elements_dir, reader_rank);
                k += num_elements_dir;
            }
        }
    }
    free(array);
    return 0;
}

int streaming_mode(capio_ordered &capio,
                   const std::unordered_map<std::string, std::pair<int, int>>& dirs_info,
                   const std::unordered_map<std::string, std::vector<int>>& readers_info, int rank) {
    //std::cout << "streaming  writer" << rank << std::endl;
    for (auto& pair : readers_info) {
        std::string dir(pair.first);
        std::cout << "writer " << rank << "sending dir " << dir << std::endl;
        int num_files = dirs_info.at(dir).first;
        int num_elements = dirs_info.at(dir).second;
        int* array = new int[num_elements];
        for (int reader_rank: pair.second) {
            std::cout << "writer " << rank << "sending dir " << dir << "to reader " << reader_rank << std::endl;
            for (int i = 0; i < num_files; ++i) {
                std::fill_n(array, num_elements, rank + 1);
                //std::cout << "writer " << std::to_string(rank) << " writing file " << file_name << std::endl;
                capio.capio_send(array, num_elements, reader_rank);

            }
        }
        free(array);
    }
    return 0;
}

int abbr_mode(capio_ordered &capio, const std::string& file_path, int rank, int size, bool streaming) {
    std::unordered_map<std::string, std::pair<int, int>> dirs_info;
    std::unordered_map<std::string, std::vector<int>> readers_info;
    bool res_conf = file_parsing_writer_capio(file_path, rank, size, dirs_info, readers_info);
    int res;
    if (res_conf) {
        if (streaming)
            res = streaming_mode(capio, dirs_info, readers_info, rank);
        else
            res = batch_mode(capio, dirs_info, readers_info, rank);

    }
    else {
        res =false;
        std::cout << "error reading conf file" << std::endl;
    }
    return res;
}

int main(int argc, char** argv) {
    int rank, size;
    int res;
    MPI_Init(&argc, &argv);
    if (argc != 6) {
        std::cout << "input error: capio buffer size, capio configuration file, iobench configuration file, mode flag and streaming flag needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    const int buf_size = std::stoi(argv[1]);
    const std::string capio_config_path(argv[2]);
    const std::string iobench_config_path(argv[3]);
    const std::string mode_flag(argv[4]);
    const std::string streaming_flag(argv[5]);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    capio_ordered capio(false, true, rank, buf_size, capio_config_path);
    res = abbr_mode(capio, iobench_config_path, rank, size, streaming_flag == "streaming");
    std::cout << "writer " << rank << " terminated" << std::endl;
    MPI_Finalize();
    return res;
}