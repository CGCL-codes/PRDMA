
# aux_source_directory(. DIR_SRCS)
set (CLIENT_SRCS client.c)
set (YCSB_SRCS ycsb.c)
set (ENRON_SRCS enron.c)
set (DBLP_SRCS dblp.c)
set (WORD_SRCS word.c)

#set (CMAKE_CXX_FLAGS "-Wall -lxml2 -lrdmacm -libverbs -lpthread -lnuma -lrt -lm")

 set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-msse2") 

set (EXECUTABLE_OUTPUT_PATH ${DHMP_SOURCE_DIR}/bin)


add_executable(client ${CLIENT_SRCS})
add_executable(ycsb ${YCSB_SRCS})
add_executable(enron ${ENRON_SRCS})
add_executable(dblp ${DBLP_SRCS})
add_executable(word ${WORD_SRCS})


target_link_libraries(client dhmp)
target_link_libraries(client xml2)
target_link_libraries(client pthread)
target_link_libraries(client rdmacm)
target_link_libraries(client ibverbs)
target_link_libraries(client numa)
target_link_libraries(client rt)
target_link_libraries(client m)
target_link_libraries(client json-c)


target_link_libraries(ycsb dhmp)
target_link_libraries(ycsb xml2)
target_link_libraries(ycsb pthread)
target_link_libraries(ycsb rdmacm)
target_link_libraries(ycsb ibverbs)
target_link_libraries(ycsb numa)
target_link_libraries(ycsb rt)
target_link_libraries(ycsb m)
target_link_libraries(ycsb json-c)

target_link_libraries(enron dhmp)
target_link_libraries(enron xml2)
target_link_libraries(enron pthread)
target_link_libraries(enron rdmacm)
target_link_libraries(enron ibverbs)
target_link_libraries(enron numa)
target_link_libraries(enron rt)
target_link_libraries(enron m)
target_link_libraries(enron json-c)

target_link_libraries(dblp dhmp)
target_link_libraries(dblp xml2)
target_link_libraries(dblp pthread)
target_link_libraries(dblp rdmacm)
target_link_libraries(dblp ibverbs)
target_link_libraries(dblp numa)
target_link_libraries(dblp rt)
target_link_libraries(dblp m)
target_link_libraries(dblp json-c)

target_link_libraries(word dhmp)
target_link_libraries(word xml2)
target_link_libraries(word pthread)
target_link_libraries(word rdmacm)
target_link_libraries(word ibverbs)
target_link_libraries(word numa)
target_link_libraries(word rt)
target_link_libraries(word m)
target_link_libraries(word json-c)



