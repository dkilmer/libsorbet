cmake_minimum_required(VERSION 3.16)
project(sorbet C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")

add_library(sorbet SHARED library.c library.h test.c)
add_library(sorbetstatic STATIC library.c library.h)
set_target_properties(sorbetstatic PROPERTIES OUTPUT_NAME sorbet)
add_executable(test_sorbet test.c)
target_link_libraries(test_sorbet sorbet z)