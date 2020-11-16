#include <iostream>
#include <fstream>
#include <unordered_map>
#include <algorithm>
#include <vector>

bool new_writer(const std::string& line) {
    std::string token = line.substr(line.find_first_not_of(" "), line.size());
    token = token.substr(0, token.find(" "));
    return token == "writer";
}

bool new_reader(const std::string& line) {
    std::string token = line.substr(0, line.find(" "));
    return token == "reader";
}

bool my_conf(const std::string& line, const int rank) {
    std::string token = line.substr(line.find(" ") + 1, line.length());
    return std::stoi(token) == rank;
}

bool check_valid_file_conf(std::string& line) {
    line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
    line.erase(std::remove(line.begin(), line.end(), '\t'), line.end());
    line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
    return !line.empty() && std::find_if(line.begin(),
                                      line.end(), [](unsigned char c) { return !std::isdigit(c); }) == line.end();
}

bool file_parsing_writer(std::ifstream& myfile, int rank, int& actual_num_writers, std::vector<int> &conf) {
    std::string line;
    bool is_my_conf, already_read_my_conf = false;
    int num_line = 0;
    bool res = true;
    while (getline(myfile,line) && line.substr(0, line.find(" ")) != "reader") {
        ++num_line;
        if (new_writer(line)) {
            is_my_conf = my_conf(line, rank);
            if (is_my_conf) {
                if (already_read_my_conf) {
                    std::cout << "conf file error line " + std::to_string(num_line) + ": two configurations for " + line << std::endl;
                    return false;
                }
                else {
                    already_read_my_conf = true;
                }
            }
            else {
                already_read_my_conf = false;
            }
            ++actual_num_writers;
        }
        else {
            if (actual_num_writers == 0) {
                std::cout << "conf file error line 1: the first line must be writer 0" << std::endl;
                return false;
            }
            if (check_valid_file_conf(line)) {
                if (is_my_conf) {
                    conf.push_back(std::stoi(line));
                }
            }
            else {
                std::cout << "conf file error line " + std::to_string(num_line) + ": after new writer, file size is needed" << std::endl;
                myfile.close();
                return false;
            }
        }
    }
    return res;
}

bool read_conf_file_writer(const std::string& file_path, const int rank, const int num_writers, std::vector<int> &conf) {
    std::ifstream myfile (file_path);
    bool res = true;
    int actual_num_writers = 0;
    if (myfile.is_open()) {
        file_parsing_writer(myfile, rank, actual_num_writers, conf);
        myfile.close();
        if (actual_num_writers != num_writers) {
            res = false;
        }
    }
    else {
        std::cout << "Unable to open configuration file" << std::endl;
        res = false;
    }
    return res;
}

std::string read_conf_writers_for_readers(std::ifstream& myfile, std::unordered_map<int, std::vector<int>>& conf_writers,
                                   int& num_line) {
    std::string line;
    int writer_rank = 0;
    bool res = true;
    int num_files = 0;
    while (getline(myfile,line) && line.substr(0, line.find(" ")) != "reader") {
        ++num_line;
        if (new_writer(line)) {
            std::string token = line.substr(line.find(" ") + 1, line.length());
            writer_rank = std::stoi(token);
        }
        else {
            res = check_valid_file_conf(line);
            if (res) {
                conf_writers[writer_rank].push_back(std::stoi(line));
                ++num_files;
            }
            else {
                std::cout << "conf file error line " + std::to_string(num_line) + ": after new writer, file size is needed" << std::endl;
                myfile.close();
                return "";
            }
        }
    }
    if (myfile.eof()) {
        return "";
    }
    return line;
}

int get_writer_rank(const std::string& line) {
    std::string token = line.substr(line.find_first_not_of(" "), line.size() - 1);
    return std::stoi( token.substr(token.find(" ") + 1, token.length()));
}

bool check_valid_file_conf_reader(std::string& line) {
    line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
    line.erase(std::remove(line.begin(), line.end(), '\t'), line.end());
    line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
    if (line.substr(0, 4) != "file") {
        return false;
    }
    line = line.substr(4, line.size());
    return !line.empty() && std::find_if(line.begin(),
                                         line.end(), [](unsigned char c) { return !std::isdigit(c); }) == line.end();
}

bool parser_conf_reader(std::ifstream& myfile, int rank, int num_line, bool is_my_conf, bool already_read_my_conf,
                        int& actual_num_readers, std::unordered_map<int, std::vector<int>> writers_conf,
                        std::unordered_map<int, std::unordered_map<int, int>> &conf) {
    std::string line;
    int writer_rank, phase;
    bool res = true;
    while (getline(myfile,line)) {
        ++num_line;
        if (new_reader(line)) {
            if (phase == 0) {
                std::cout << "conf file error line " + std::to_string(num_line) + ": after new reader must specify the writer" << std::endl;
                myfile.close();
                return false;
            }
            phase = 0;
            is_my_conf = my_conf(line, rank);
            if (is_my_conf) {
                if (already_read_my_conf) {
                    std::cout << "conf file error line " + std::to_string(num_line) + ": two configurations for " + line << std::endl;
                    return false;
                }
                else {
                    already_read_my_conf = true;
                }
            }
            else {
                already_read_my_conf = false;
            }
            ++actual_num_readers;
        }
        else if (new_writer(line)) {
            phase = 1;
            writer_rank = get_writer_rank(line);
        }
        else {
            if (phase ==  0) {
                std::cout << "conf file error line " + std::to_string(num_line) + ": after new reader must specify the writer" << std::endl;
                return false;
            }
            res = check_valid_file_conf_reader(line);
            if (res) {
                if (is_my_conf) {
                    int num_file = std::stoi( line.substr(line.find(" ") + 1, line.length()));
                    conf[writer_rank][num_file] = writers_conf[writer_rank][num_file];
                }
            }
            else {
                std::cout << "conf file error line " + std::to_string(num_line) + ": after new writer, file size is needed" << std::endl;
                myfile.close();
                return false;
            }
        }
    }
    return res;
}

bool read_conf_file_reader(const std::string& file_path, const int rank, const int num_readers, std::unordered_map<int, std::unordered_map<int, int>> &conf) {
    std::string line;
    std::ifstream myfile (file_path);
    bool res = true;
    bool is_my_conf, already_read_my_conf = false;
    int actual_num_readers = 0;
    int num_line = 0;
    if (myfile.is_open()) {
        std::unordered_map<int, std::vector<int>> writers_conf;
        line = read_conf_writers_for_readers(myfile, writers_conf, num_line);
        if (new_reader(line)) {
            is_my_conf = my_conf(line, rank);
            if (is_my_conf) {
                already_read_my_conf = true;
            }
            ++actual_num_readers;
        }
        else {
            std::cout << "conf file error line 1: the first line must be reader 0" << std::endl;
            myfile.close();
            return false;
        }
        parser_conf_reader(myfile, rank, num_line, is_my_conf, already_read_my_conf, actual_num_readers, writers_conf, conf);
        myfile.close();
        if(num_readers != actual_num_readers) {
            std::cout << "conf file error: number of readers in configuration file and the actual number of readers launched are not the same" << std::endl;
            res = false;
        }
    }
    else {
        std::cout << "Unable to open configuration file" << std::endl;
        res = false;
    }
    return res;
}