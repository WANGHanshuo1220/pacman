// #ifndef __CIRCLE_QUEUE_HH__
// #define __CIRCLE_QUEUE_HH__
#include <iostream>
#include <stdlib.h>
#include "segment.h"

class CircleQueue
{
private:
    //内部元素队列指针
    long d_rear, d_front;

    size_t d_size;
    size_t d_maxsize;
    size_t full_times;
    std::vector<LogSegment *> d_arr;

public:
    //默认构造函数
    CircleQueue();

    //自定义构造函数
    // CircleQueue(size_t);
    void init(size_t);

    //析构函数
    ~CircleQueue();

    // get full times
    size_t get_full_times() { return full_times; }

    //踢队操作
    LogSegment *deque();

    //入队操作
    void enque(LogSegment *);

    // //查看队列首端元素
    // LogSegment *front();

    // //查看队列末端元素
    // LogSegment *rear();

    //队列是否非空
    bool is_empty();

    //队列是否已满
    bool is_full();

    // //返回队列已有数量
    // size_t q_size();

    // //缓存
    // LogSegment **buffer();

    //打印队列
    // template <class R>
    // friend std::ostream &operator<<(std::ostream &, CircleQueue<R> &);
};
// #endif