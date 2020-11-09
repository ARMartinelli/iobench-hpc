add_executable(par_read par_read.cpp)
add_executable(par_write par_write.cpp)


target_link_libraries(par_read ${MPI_LIBRARIES})
target_link_libraries(par_write ${MPI_LIBRARIES})
# includes MPI
include_directories(${MPI_INCLUDE_PATH})
if(MPI_COMPILE_FLAGS)
    set_target_properties(par_read PROPERTIES
            COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
    set_target_properties(par_write PROPERTIES
            COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()

if(MPI_LINK_FLAGS)
    set_target_properties(par_read PROPERTIES
            LINK_FLAGS "${MPI_LINK_FLAGS}")
    set_target_properties(par_write PROPERTIES
            LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()