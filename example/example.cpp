#include <iostream>
#include <string>
#include <experimental/filesystem>
#include <sys/time.h>

#include "config.h"
#include "db.h"
#include "../db/log_structured.h"

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

// size_t log_size = 1ul << 30;
// #define log_size 8 * SEGMENT_SIZE
#define log_size 1ul << 30
#define num_workers 1
#define num_cleaners 1
std::string db_path = std::string(PMEM_DIR) + "log_kvs";
DB *db = new DB(db_path, log_size, num_workers, num_cleaners);
std::unique_ptr<DB::Worker> worker = db->GetWorker();

struct timeval start, checkpoint1, checkpoint2;

void job0()
{ 
  printf("in job0\n");
  uint64_t key = 0x1234;
  std::string value = "hello world";
  worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));

  std::string val;
  worker->Get(Slice((const char *)&key, sizeof(uint64_t)), &val);
  std::cout << "value: " << val << std::endl;
}


int NUM_KVS = 1000000;
int dup_rate = 10;

void job1()
{
  printf("NUM_KVS = %d, key dup_rate = %d\n", NUM_KVS, dup_rate);
  std::string val;
  std::string res;
  uint64_t key, key_;
  gettimeofday(&start, NULL);
  for(int i = 0; i < NUM_KVS; i++)
  {
    key = i%dup_rate;
    std::string value = "hello world" + i%10;
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }

  gettimeofday(&checkpoint1, NULL);
  for(int j = 0; j < NUM_KVS; j ++)
  {
    key_ = j%dup_rate;
    worker->Get(Slice((const char *)&key_, sizeof(uint64_t)), &val);
    res = "hello world" + j%10;
    assert(val == res);
  }
  gettimeofday(&checkpoint2, NULL);
  long insert_time = TIMEDIFF(start, checkpoint1);
  long search_time = TIMEDIFF(checkpoint1, checkpoint2);
  printf("insert time : %ld us\n", insert_time);
  printf("search time : %ld us\n", search_time);
#ifdef GC_EVAL
  LogStructured *log = db->get_log_();
  int num_cleaners_ = log->get_num_cleaners();
  std::pair<int, long> *p = log->get_cleaners_info();
  assert(num_cleaners == num_cleaners_);
  for(int i = 0; i < num_cleaners_; i++)
  {
    std::cout << i << "th cleaner: ";
    std::cout << "GC_times = " << p[i].first;
    std::cout << ", GC_timecost = " << p[i].second << " us";
    std::cout << ", take up " << 100 * p[i].second / insert_time;
    std::cout << "% insert time" << std::endl;
  }
#endif

}

void (*jobs[])() = {job0, job1};

int main(int argc, char **argv) {
// #ifdef GC_EVAL
//   printf("in main: GC_EVAL ON\n");
// #endif
// #ifdef INTERLEAVED
//   printf("in main: INTERLEAVED ON\n");
// #endif
  int job = 0;
  char *arg;
  if(argc == 1)
  {
    std::cout << "usage: " << argv[0];
    std::cout << " [job] " << " [NUM_KVs] " << " [dup_rate]" << std::endl;
    return 0;
  }
  else
  {
    if(argc == 2)
    {
      arg = argv[1];
    }
    else if(argc == 3)
    {
      arg = argv[1];
      NUM_KVS = atoi(argv[2]);
    }
    else if(argc == 4)
    {
      arg = argv[1];
      NUM_KVS = atoi(argv[2]);
      dup_rate = atoi(argv[3]);
    }
    else
    {
      std::cout << "usage: " << argv[0];
      std::cout << " [job] " << " [NUM_KVs] " << " [dup_rate]" << std::endl;
      return 0;
    }
  }
  (*jobs[atoi(arg)])();
  return 0;
}