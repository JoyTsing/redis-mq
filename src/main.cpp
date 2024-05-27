#include <chrono>
#include <iostream>
#include <thread>
#include "redis-mq.h"

int main(int argc, char *argv[]) {
    std::thread t([]() {
        RedisMQ redisMQ;
        redisMQ.init_notify_handler([](int channel, std::string message) {
            std::cout << "channel: " << channel << ", message: " << message
                      << std::endl;
        });
        if (!redisMQ.connect()) { return; }
        redisMQ.subscribe(1);
        redisMQ.subscribe(2);
        redisMQ.observer_channel_message();
    });
    RedisMQ redisMQ;
    if (!redisMQ.connect()) { return 1; }
    while (true) {
        redisMQ.publish(1, "hello1");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        redisMQ.publish(2, "world2");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // t.join();
    return 0;
}
