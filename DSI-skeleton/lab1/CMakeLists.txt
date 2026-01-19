add_executable(lab1
        buzzdb_lab1.cpp
        )
target_compile_features(lab1 PUBLIC cxx_std_17)
target_include_directories(lab1 PUBLIC include)
file(COPY . DESTINATION ${CMAKE_BINARY_DIR}/lab1)