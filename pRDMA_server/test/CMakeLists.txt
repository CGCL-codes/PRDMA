
# aux_source_directory(. DIR_SRCS)
set (SERVER_SRCS server.c)

#set (CMAKE_CXX_FLAGS "-Wall -lxml2 -lrdmacm -libverbs -lpthread -lnuma -lrt -lm")

 set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-msse2") 

set (EXECUTABLE_OUTPUT_PATH ${DHMP_SOURCE_DIR}/bin)


add_executable(server ${SERVER_SRCS})



target_link_libraries(server dhmp)
target_link_libraries(server xml2)
target_link_libraries(server pthread)
target_link_libraries(server rdmacm)
target_link_libraries(server ibverbs)
target_link_libraries(server numa)
target_link_libraries(server rt)
target_link_libraries(server m)
target_link_libraries(server json-c)
