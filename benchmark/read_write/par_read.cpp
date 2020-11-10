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

void use_data(int* array, int num_elements, int rank) {
    int num = 0;
    for (int i = 0; i < num_elements; ++i) {
        if (num > std::numeric_limits<int>::max() - array[i]) {
            num = 0;
        }
        num += array[i];
    }
    std::ofstream output_file("output_read_matrixes_" + std::to_string(rank) + ".txt");
    output_file << "result of reader " << rank << ": " << num;
    output_file.close();
}

int get_writer_rank(const int reader_rank, const int num_readers, const int num_writers,
                    const int num_files_per_writer) {
    const int tot_num_files = num_writers * num_files_per_writer;
    const int num_readers_per_file = num_readers / tot_num_files;
    const int num_readers_per_writer = num_readers_per_file * num_files_per_writer;
    return reader_rank / num_readers_per_writer;
}

int get_file_part_num(const int reader_rank, const int num_readers, const int num_writers,
                      const int num_files_per_writer) {
    const int tot_num_files = num_writers * num_files_per_writer;
    const int num_readers_per_file = num_readers / tot_num_files;
    int num_file = reader_rank / num_readers_per_file;
    int file_part_num = num_file % num_files_per_writer;
    return file_part_num;
}

int main(int argc, char** argv) {
    int rank, size, *array;
    MPI_Init(&argc, &argv);
    if (argc != 4) {
        std::cout << "input error: number of writer, files for each writer and number of elements for each file needed" << std::endl;
        MPI_Finalize();
        return 1;
    }
    const int num_writers = std::stoi(argv[1]);
    const int num_files_per_writer = std::stoi(argv[2]);
    const int num_elements = std::stoi(argv[3]);
    const int tot_num_files = num_writers * num_files_per_writer;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::string dir_name;
    std::string file_name;
    int writer_rank;
    if (tot_num_files > size) {
        int num_files_per_reader = tot_num_files / size;
        array = new int[num_elements * num_files_per_reader];
        for (int i = 0; i < num_files_per_reader; ++i) {
            int k = rank * num_files_per_reader + i;
            writer_rank = k / num_files_per_writer;
            dir_name = ("./dir_process_" + std::to_string(writer_rank));
            std::string file_name_prefix(dir_name + "/output_file_" + std::to_string(writer_rank));
            file_name = file_name_prefix + "_part" + std::to_string((i + rank * num_files_per_reader) % num_files_per_writer) + ".txt";
            read_from_file(array + i * num_elements, num_elements, file_name, rank);
        }
        use_data(array, num_elements * num_files_per_reader, rank);
    }
    else {
        writer_rank = get_writer_rank(rank, size, num_writers, num_files_per_writer);
        int file_part_num = get_file_part_num(rank, size, num_writers, num_files_per_writer);
        array = new int[num_elements];
        dir_name = ("./dir_process_" + std::to_string(writer_rank));
        std::string file_name_prefix(dir_name + "/output_file_" + std::to_string(writer_rank));
        file_name = file_name_prefix + "_part" + std::to_string(file_part_num) + ".txt";
        read_from_file(array, num_elements, file_name, rank);
        use_data(array, num_elements, rank);
    }
    free(array);
    MPI_Finalize();
    return 0;
}
