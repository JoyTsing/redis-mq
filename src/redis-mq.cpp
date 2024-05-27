#include <iostream>
#include <thread>
#include "redis-mq.h"

RedisMQ::RedisMQ() : _publish_context(nullptr), _subscribe_context(nullptr) {
}

RedisMQ::~RedisMQ() {
    if (_publish_context != nullptr) { redisFree(_publish_context); }
    if (_subscribe_context != nullptr) { redisFree(_subscribe_context); }
}

bool RedisMQ::connect() {
    // 负责publish发布消息的上下文连接
    _publish_context = redisConnect("172.22.16.21", 6379);
    if (nullptr == _publish_context) {
        std::cerr << "connect redis failed!" << std::endl;
        return false;
    }

    // 负责subscribe订阅消息的上下文连接
    _subscribe_context = redisConnect("172.22.16.21", 6379);
    if (nullptr == _subscribe_context) {
        std::cerr << "connect redis failed!" << std::endl;
        return false;
    }

    // 在单独的线程中，监听通道上的时间，有消息给业务层进行上报
    std::thread t([&]() { observer_channel_message(); });
    t.detach();

    std::cout << "connect redis-server success!" << std::endl;

    return true;
}

bool RedisMQ::publish(int channel, std::string message) {
    redisReply *reply = (redisReply *)redisCommand(
        _publish_context, "PUBLISH %d %s", channel, message.c_str());
    if (nullptr == reply) {
        std::cerr << "publish command failed!" << std::endl;
        return false;
    }
    freeReplyObject(reply);
    return true;
}

bool RedisMQ::subscribe(int channel) {
    // SUBSCRIBE命令本身会造成线程阻塞等待通道里面发生消息，这里只做订阅通道，不接受通道消息
    // 通道消息的接受专门在observer_channel_message函数中的独立线程中进行
    // 只负责发送命令，不阻塞接受redis_server响应消息否则和notifyMsg线程抢占响应资源
    if (REDIS_ERR
        == redisAppendCommand(
            this->_subscribe_context, "SUBSCRIBE %d", channel)) {
        std::cerr << " subscribe command failed!" << std::endl;
        return false;
    }
    // redisBufferWrite可以循环发送缓冲区，直到缓冲区数据发送完毕(done被置为1)
    int done = 0;
    while (!done) {
        if (REDIS_ERR == redisBufferWrite(this->_subscribe_context, &done)) {
            std::cerr << "subscribe command failed!" << std::endl;
            return false;
        }
    }
    return true;
}

bool RedisMQ::unsubscribe(int channel) {
    if (REDIS_ERR
        == redisAppendCommand(
            this->_subscribe_context, "UNSUBSCRIBE %d", channel)) {
        std::cerr << " unsubscribe command failed!" << std::endl;
        return false;
    }
    // redisBufferWrite可以循环发送缓冲区，直到缓冲区数据发送完毕(done被置为1)
    int done = 0;
    while (!done) {
        if (REDIS_ERR == redisBufferWrite(this->_subscribe_context, &done)) {
            std::cerr << "unsubscribe command failed!" << std::endl;
            return false;
        }
    }
    return true;
}

void RedisMQ::observer_channel_message() {
    redisReply *reply = nullptr;
    while (REDIS_OK
           == redisGetReply(this->_subscribe_context, (void **)&reply)) {
        // 订阅收到的消息是一个带三元素的数组
        if (reply != nullptr && reply->element[2] != nullptr
            && reply->element[2]->str != nullptr) {
            // 给业务层上报通道上发生的消息
            _notify_message_handler(
                atoi(reply->element[1]->str), reply->element[2]->str);
        }
        freeReplyObject(reply);
    }

    std::cerr << ">>>>>>>>>> observer_channel_message quit <<<<<<<<<<<<<"
              << std::endl;
}

void RedisMQ::init_notify_handler(std::function<void(int, std::string)> fn) {
    this->_notify_message_handler = fn;
}
