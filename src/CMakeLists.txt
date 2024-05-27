# for each "src/x.cpp", generate target "x"
file(GLOB_RECURSE all_srcs CONFIGURE_DEPENDS *.cpp)

add_executable(redis-mq ${all_srcs})
target_link_libraries(redis-mq PUBLIC hiredis)