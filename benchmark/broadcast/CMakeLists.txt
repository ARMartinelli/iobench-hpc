add_executable(read_from_single_file read_from_single_file.cpp)
add_executable(write_one_file write_one_file.cpp)


target_link_libraries(read_from_single_file ${MPI_LIBRARIES})
target_link_libraries(write_one_file ${MPI_LIBRARIES})
# includes MPI
include_directories(${MPI_INCLUDE_PATH})
if(MPI_COMPILE_FLAGS)
    set_target_properties(read_from_single_file PROPERTIES
            COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(par_write PROPERTIES
            COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()

if(MPI_LINK_FLAGS)
    set_target_properties(read_from_single_file PROPERTIES
            LINK_FLAGS "${MPI_LINK_FLAGS}")
    set_target_properties(write_one_file PROPERTIES
            LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()