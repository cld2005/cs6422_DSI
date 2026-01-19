cmake_minimum_required(VERSION 3.22) # I recommend whatever's the latest version
project(DSI
        VERSION 0.1.0 # https://semver.org/spec/v0.1.0.html
        DESCRIPTION "dsi cmake"
        LANGUAGES CXX
        )
add_subdirectory(lab1)
#add_subdirectory(lab2)
#add whatever subdirectory you need in the future
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_COLOR_DIAGNOSTICS)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
