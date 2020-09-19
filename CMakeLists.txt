cmake_minimum_required(VERSION 3.16)
project(sorbet LANGUAGES C VERSION 0.0.1 DESCRIPTION "sorbet file reader/writer")
include(GNUInstallDirs)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")

add_library(sorbet SHARED sorbet.c sorbet.h utf8_val.c utf8_val.h)
set_target_properties(sorbet PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        PUBLIC_HEADER sorbet.h)
add_library(sorbetstatic STATIC sorbet.c sorbet.h utf8_val.c utf8_val.h)
set_target_properties(sorbetstatic PROPERTIES
        OUTPUT_NAME sorbet
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
        PUBLIC_HEADER sorbet.h)
add_executable(test_sorbet test.c)
target_link_libraries(test_sorbet sorbet z)
INSTALL(TARGETS sorbet sorbetstatic
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})