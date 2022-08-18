#include <algorithm>
#include <cmath>
#include <random>
#include <iostream>
#include <string>
#include <experimental/filesystem>
#include <sys/time.h>
#include <map>

#include "config.h"
#include "db.h"
#include "../db/log_structured.h"

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

// size_t log_size = 1ul << 30;
// #define log_size 5 * SEGMENT_SIZE
#define log_size 1ul << 30
int num_workers = 1;
int num_cleaners = 1;
std::string db_path = std::string(PMEM_DIR) + "log_kvs";
DB *db;
std::random_device rd;  //Will be used to obtain a seed for the random number engine
std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
std::uniform_real_distribution<> distrib(0, 1);

struct timeval start, checkpoint1, checkpoint2;
int zipf();
void init_zipf();

uint64_t NUM_KVS = 200000000;
uint64_t dup_rate = 10;

double c = 0;          // Normalization constant
double *sum_probs;     // Pre-calculated sum of probabilities
double alpha = 0.99;

void job0()
{ 
  db = new DB(db_path, log_size, num_workers, num_cleaners);
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  uint64_t key = 0x1234;
  std::string value = "hello world";
  worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));

  std::string val;
  worker->Get(Slice((const char *)&key, sizeof(uint64_t)), &val);
  std::cout << "value: " << val << std::endl;
#ifdef GC_EVAL
  worker.reset();
  delete db;
#endif
}

void job1()
{
  db = new DB(db_path, log_size, num_workers, num_cleaners);
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  printf("NUM_KVS = %ld, key dup_rate = %ld\n", NUM_KVS, dup_rate);
  std::string val;
  std::string res;
  uint64_t key, key_;
  long insert_time = 0;
  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = i%dup_rate;
    std::string value = "hello world"+ std::to_string(i%dup_rate);
    gettimeofday(&start, NULL);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    gettimeofday(&checkpoint1, NULL);
    insert_time += TIMEDIFF(start, checkpoint1);
  }

  printf("job1: insert time : %ld us\n", insert_time);
  worker.reset();
  delete db;
}

void job2()
{
  db = new DB(db_path, log_size, num_workers, num_cleaners);
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  init_zipf();
  printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);
  // std::map<std::string, std::string> kvs;
  // std::string val;
  // std::string res;
  uint64_t key;
  long insert_time = 0;
  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = zipf();
    std::string value = "hello world 22-08-16:" + std::to_string(i%100);
    // std::string value = "hello world     " + std::to_string(i%100);
    value.resize(48);
    // kvs[key_s] = value;
    gettimeofday(&start, NULL);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    gettimeofday(&checkpoint1, NULL);
    insert_time += TIMEDIFF(start, checkpoint1);
  }

  printf("insert time : \t%ld us \t(%ld s)\n", insert_time, insert_time/1000000);

  // printf("\nstart checking...\n");
  // std::string val_;
  // uint64_t key_;
  // int find_kvs = 0;
  // int false_kv = 0;
  // bool c = true;
  // for(uint64_t i = 1; i <= dup_rate; i++)
  // {
  //   key_ = i;
  //   worker->Get(Slice((const char *)&key_, sizeof(uint64_t)), &val_);
  //   if( kvs.find(key_) != kvs.end())
  //   {
  //     find_kvs ++;
  //     if(kvs[key_].compare(val_) != 0)
  //     {
  //       // printf("kvs not equal, key_ = %d\n", key_);
  //       // std::cout << "  kvs not equal. key_ = " << key_ << std::endl;
  //       // std::cout << "    right_val = " << kvs[key_] << std::endl;
  //       // std::cout << "    db_val = " << val_ << std::endl;
  //       false_kv ++;
  //       c = false;
  //     }
  //   }
  // }

  // if(c)
  // {
  //   std::cout << "all kvs correct: " << find_kvs << std::endl;
  // }else
  // {
  //   std::cout << "some kvs incorrect: " << false_kv << std::endl;
  // }
  printf("worker reset\n");
  worker.reset();
  printf("delete db\n");
  delete db;
  printf("delete db done\n");
}

std::map<uint64_t, std::string> kvs;

void *put_KVS(void *)
{
  uint64_t key;
  long insert_time = 0;
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  // printf("worker ptr = %p\n", &worker);

  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = zipf();
    std::string value = "hello world" + std::to_string(i%100);
    kvs[key] = value;
    gettimeofday(&start, NULL);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    gettimeofday(&checkpoint1, NULL);
    insert_time += TIMEDIFF(start, checkpoint1);
  }

  printf("%ld thread-> insert time : %ld us\n", pthread_self(), insert_time);

  worker.reset();
}

void job3()
{
  num_cleaners = 2;
  num_workers = 4;
  pthread_t *tid = new pthread_t[num_workers];
  int ret;

  db = new DB(db_path, log_size, num_workers, num_cleaners);
  
  init_zipf();
  printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);

  for(int i = 0; i < num_workers; i++)
  {
    ret = pthread_create(&tid[i], NULL, &put_KVS, NULL); 
    if(ret != 0)
    {
      printf("%dth thread create error\n", i);
      exit(0);
    }
  }

  for(int i = 0; i < num_workers; i++)
  {
    pthread_join(tid[i], NULL);
  }

  printf("\nstart checking...\n");
  std::string val_;
  uint64_t key_;
  int find_kvs = 0;
  int false_kv = 0;
  bool c = true;
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  for(uint64_t i = 1; i <= dup_rate; i++)
  {
    key_ = i;
    worker->Get(Slice((const char *)&key_, sizeof(uint64_t)), &val_);
    if( kvs.find(key_) != kvs.end())
    {
      find_kvs ++;
      if(kvs[key_].compare(val_) != 0)
      {
        // printf("kvs not equal, key_ = %d\n", key_);
        // std::cout << "  kvs not equal. key_ = " << key_ << std::endl;
        // std::cout << "    right_val = " << kvs[key_] << std::endl;
        // std::cout << "    db_val = " << val_ << std::endl;
        false_kv ++;
        c = false;
      }
    }
  }

  if(c)
  {
    std::cout << "all kvs correct: " << find_kvs << std::endl;
  }else
  {
    std::cout << "some kvs incorrect: " << false_kv << std::endl;
  }
  worker.reset();
  delete db;
}

void (*jobs[])() = {job0, job1, job2, job3};

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

void init_zipf()
{
  printf("init zipf...\n");
  // Compute normalization constant on first call only
  for (int i=1; i<= dup_rate; i++)
    c = c + (1.0 / pow((double) i, alpha));
  c = 1.0 / c;

  sum_probs = (double *)malloc(( dup_rate + 1)*sizeof(*sum_probs));
  sum_probs[0] = 0;
  for (int i=1; i<= dup_rate; i++) {
    sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
  }
  printf("finished...\n");
}

int zipf()
{
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int low, high, mid;           // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = distrib(gen);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = dup_rate, mid;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= dup_rate));

  return(zipf_value);
}