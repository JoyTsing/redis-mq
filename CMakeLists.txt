cmake_minimum_required(VERSION 3.20.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
    message("Setting default build type to Release")
endif()

project(redis-mq VERSION 0.0.1 LANGUAGES C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_subdirectory(./src)
