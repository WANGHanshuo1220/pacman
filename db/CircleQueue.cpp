#include "CircleQueue.h"
#define DEFAULT_SIZE 100

CircleQueue::CircleQueue()
{
    d_front = d_rear = -1;
    d_size = 0;
    full_times = 0;
    // d_arr = NULL;
}

//构造函数
void CircleQueue::init(size_t size)
{
    d_maxsize = (size > DEFAULT_SIZE) ? size : DEFAULT_SIZE;
    d_arr = std::vector<LogSegment *>(d_maxsize);
}

//析构函数
CircleQueue::~CircleQueue()
{

}

//检查环形队列是否为满载
bool CircleQueue::is_full()
{
    return (d_rear == d_maxsize - 1 && d_front == 0) || (d_rear == d_front - 1);
}

//判断环形队列是否为空
bool CircleQueue::is_empty()
{
    return d_front == -1 || d_rear == -1;
}

//入队操作
void CircleQueue::enque(LogSegment * value)
{
    if (is_full())
    {
        // std::cout << "环形队列已满" << std::endl;
        // full_times ++;
        return;
    }
    else if (is_empty())
    {
        d_rear = d_front = 0;
    }
    else if (d_rear == d_maxsize - 1)
    {
        d_rear = 0;
    }
    else
    {
        d_rear++;
    }
    d_arr[d_rear] = value;
    d_size++;
}

//踢队操作
LogSegment *CircleQueue::deque()
{
    if (is_empty())
    {
        // std::cout << "环形队列为空!!" << std::endl;
        return NULL;
    }

    LogSegment * data = d_arr[d_front];

    if (d_front == d_rear)
    {
        d_front = -1;
        d_rear = -1;
    }
    else if (d_front == d_maxsize - 1)
    {
        d_front = 0;
    }
    else
    {
        d_front++;
    }
    d_size--;
    return data;
}

//获取队头部元素
// LogSegment * CircleQueue::front()
// {
//     return d_arr[d_front];
// }

// //获取队尾元素
// LogSegment * CircleQueue::rear()
// {
//     return d_arr[d_rear];
// }

// //获取队列尺寸
// size_t CircleQueue::q_size()
// {
//     return d_size;
// }

// //获取队列内部的缓存指针
// LogSegment **CircleQueue::buffer()
// {
//     return d_arr;
// }

//遍历
// template <class R>
// std::ostream &operator<<(std::ostream &os, CircleQueue<R> &q)
// {

//     const R *buf = q.d_arr;

//     if (q.d_front == -1)
//     {
//         os << "CircleQueue为空" << std::endl;
//     }
//     else if (q.d_rear >= q.d_front)
//     {
//         for (auto i = q.d_front; i <= q.d_rear; i++)
//         {
//             os << buf[i] << ",";
//         }
//     }
//     else
//     {
//         for (auto i = q.d_front; i < q.d_size; i++)
//         {
//             os << buf[i] << ",";
//         }

//         for (auto i = 0; i <= q.d_rear; i++)
//         {
//             os << buf[i] << ",";
//         }
//     }
//     os << std::endl;
//     return os;
// }