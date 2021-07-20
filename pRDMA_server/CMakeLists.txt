cmake_minimum_required (VERSION 2.6)

project (DHMP)

include_directories(include)
include_directories(/usr/local/include/libxml2/)

add_subdirectory(src)

add_subdirectory(test)