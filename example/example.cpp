#include <algorithm>
#include <cmath>
#include <random>
#include <iostream>
#include <string>
#include <experimental/filesystem>
#include <sys/time.h>
#include <map>
#include <set>
#include <unordered_set>

#include "../benchmarks/bench_base.h"

#include "config.h"
#include "db.h"
#include "../db/log_structured.h"

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

#define prefilling_rate 0.75
#define value_sz 32
#define kv_sz (value_sz + 20)
uint64_t log_size = 1ul << 30;
int num_workers = 24;
int num_cleaners = 1;
std::string db_path = std::string(PMEM_DIR) + "log_kvs";
DB *db;
std::random_device rd;  //Will be used to obtain a seed for the random number engine
std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
std::uniform_real_distribution<> distrib(0, 1);
std::unordered_map<uint64_t, uint64_t> re;
std::vector<uint64_t> time_eval;
std::vector<std::vector<uint64_t>> key_base;
std::vector<std::vector<uint64_t>> op_base;
pthread_t *tid;
pthread_t *tid_prefilling = new pthread_t;

struct timeval start, checkpoint1, checkpoint2;
struct timeval zipf_s, zipf_e;
long zipf_time_cost = 0;
uint64_t zipf();
uint64_t uniform();
void init_zipf();
int get_random();
void prepare_key_base();

uint64_t NUM_KVS = 0;
uint64_t dup_rate = 0;

double c = 0;          // Normalization constant
double *sum_probs;     // Pre-calculated sum of probabilities
double alpha = 0.99;

int ret;
uint64_t runtime = 0;
std::vector<int> a;

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

uint64_t uniform()
{
  uint64_t re = floor( dup_rate * (distrib(gen)));
  assert(re >= 0 && re <= dup_rate);
  return re;
}

uint64_t zipf()
{
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int low, high, mid;           // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  // gettimeofday(&zipf_s, NULL);
  do
  {
    z = distrib(gen);
  }
  while ((z < 0.00000001) || (z > 0.99999999));

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

  // gettimeofday(&zipf_e, NULL);
  // zipf_time_cost += TIMEDIFF(zipf_s, zipf_e);
  return(zipf_value);
}

Random ra((unsigned)time(NULL));
int get_random()
{
  return ra.Next()%100;
}


void prepare_key_base()
{
  key_base.resize(num_workers);
  op_base.resize(num_workers);
  for(int i = 0; i < num_workers; i++)
  {
    key_base[i].resize(NUM_KVS);
    op_base[i].resize(NUM_KVS);
    for(uint64_t j = 0; j < NUM_KVS; j++)
    {
      uint64_t key = zipf();
      key_base[i][j] = key;
      uint32_t op = get_random();
      if(op < 50) op_base[i][j] = 0;
      else op_base[i][j] = 1;
    }
  }
}

// void job0()
// { 
//   db = new DB(db_path, log_size, num_workers, num_cleaners);
//   std::unique_ptr<DB::Worker> worker = db->GetWorker();
//   uint64_t key = 0x1234;
//   std::string value = "hello world";
//   worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));

//   std::string val;
//   worker->Get(Slice((const char *)&key, sizeof(uint64_t)), &val);
//   std::cout << "value: " << val << std::endl;
// #ifdef GC_EVAL
//   worker.reset();
//   delete db;
// #endif
// }

// void job1()
// {
//   num_workers = 1;
//   num_cleaners = 1;
//   db = new DB(db_path, log_size, num_workers, num_cleaners);
//   std::unique_ptr<DB::Worker> worker = db->GetWorker();
//   printf("NUM_KVS = %ld, key dup_rate = %ld\n", NUM_KVS, dup_rate);
//   std::string val;
//   std::string res;
//   uint64_t key, key_;
//   long insert_time = 0;
//   for(uint64_t i = 0; i < NUM_KVS; i++)
//   {
//     key = i%dup_rate;
//     std::string value = "hello world"+ std::to_string(i%dup_rate);
//     gettimeofday(&start, NULL);
//     worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//     gettimeofday(&checkpoint1, NULL);
//     insert_time += TIMEDIFF(start, checkpoint1);
//   }

//   printf("job1: insert time : %ld us\n", insert_time);
//   worker.reset();
//   delete db;
// }

// void job2()
// {
//   num_workers = 1;
//   num_cleaners = 1;
//   printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);
//   std::map<uint64_t, std::string> kvs;
//   std::string val;
//   std::string res;
//   uint64_t key;
//   long insert_time = 0;
//   std::string value = "hello world 22-08-30";
//   value.resize(32);

//   db = new DB(db_path, log_size, num_workers, num_cleaners);
//   std::unique_ptr<DB::Worker> worker = db->GetWorker();

//   // printf("prefilling\n");
//   // for(uint64_t i = 0; i <  prefilling_rate * log_size / 52; i++)
//   // {
//   //   key = zipf();
//   //   value.resize(32);
//   //   kvs[key] = value;
//   //   worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//   // }

//   // printf("prefilling done...\n");

//   for(uint64_t i = 0; i < NUM_KVS; i++)
//   {
//     key = zipf();
//     value = value + std::to_string(i%10);
//     value.resize(32);
//     kvs[key] = value;
//     gettimeofday(&start, NULL);
//     // Timer time(insert_time);
//     worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//     gettimeofday(&checkpoint1, NULL);
//     insert_time += TIMEDIFF(start, checkpoint1);
//   }

//   // printf("\nstart checking...\n");
//   // std::string val_;
//   // uint64_t key_;
//   // int find_kvs = 0;
//   // int false_kv = 0;
//   // bool c = true;
//   // for(uint64_t i = 1; i <= dup_rate; i++)
//   // {
//   //   key_ = i;
//   //   worker->Get(Slice((const char *)&key_, sizeof(uint64_t)), &val_);
//   //   if( kvs.find(key_) != kvs.end())
//   //   {
//   //     find_kvs ++;
//   //     if(kvs[key_].compare(val_) != 0)
//   //     {
//   //       // printf("kvs not equal, key_ = %d\n", key_);
//   //       // std::cout << "  kvs not equal. key_ = " << key_ << std::endl;
//   //       // std::cout << "    right_val = " << kvs[key_] << std::endl;
//   //       // std::cout << "    db_val = " << val_ << std::endl;
//   //       false_kv ++;
//   //       c = false;
//   //     }
//   //   }
//   // }

//   // if(c)
//   // {
//   //   std::cout << "all kvs correct: " << find_kvs << std::endl;
//   // }else
//   // {
//   //   std::cout << "some kvs incorrect: " << false_kv << std::endl;
//   // }

//   worker.reset();
//   delete db;

//   printf("run time : %ld us \t(%.2f s)\n", 
//     insert_time, (float)insert_time/1000000);
// }

std::map<uint64_t, std::string> kvs;
SpinLock lock;

void *put_KVS(int id)
{
  uint32_t t_id = id;
  uint64_t key;
  int op;
  std::string value = "hello world";
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  value.resize(value_sz);
  struct timeval s, e;
  std::string value1;

  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = key_base[t_id][i];
    if(op_base[t_id][i])
    {
      // value = value + std::to_string(i%100);
      // value.resize(value_sz);
      // lock.lock();
      // kvs[key] = value;
      // lock.unlock();
      worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value), false);
    }
    else
    {
      bool found = worker->Get(Slice((const char *)&key, sizeof(KeyType)), &value1);
    }
  }

  worker.reset();
}

void *prefilling(void *)
{
  uint64_t key;
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  std::string value = "hello world 22-09-08";

  for(uint64_t i = 1; i <= dup_rate; i++)
  {
    key = i;
    // value.resize(value_sz);
    // lock.lock();
    // kvs[key] = value;
    // lock.unlock();
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value), true);
  }

  // for(uint64_t i = 0; i <  prefilling_rate * log_size / kv_sz - dup_rate; i++)
  // {
  //   key = zipf();
  //   // key = uniform();
  //   // value = value + std::to_string(i % 100);
  //   // value.resize(value_sz);
  //   // lock.lock();
  //   // kvs[key] = value;
  //   // lock.unlock();
  //   worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value), true);
  // }
  worker.reset();
}

void prepare()
{
  int ret;

  printf("Preparing key base\n");
  prepare_key_base();
  printf("Preparing key base done...\n");
  
  printf("prefilling\n");
  ret = pthread_create(tid_prefilling, NULL, &prefilling, NULL); 
  if(ret != 0)
  {
    printf("prefilling thread create error\n");
    exit(0);
  }
  pthread_join(*tid_prefilling, NULL);
  printf("prefilling done...\n");
}

void job3()
{
  // gettimeofday(&start, NULL);

  // for(int i = 0; i < num_workers; i++)
  // {
  //   a[i] = i;
  //   ret = pthread_create(&tid[i], NULL, &put_KVS, &a[i]);
  //   if(ret != 0)
  //   {
  //     printf("%dth thread create error\n", i);
  //     exit(0);
  //   }
  // }

  // for(int i = 0; i < num_workers; i++)
  // {
  //   pthread_join(tid[i], NULL);
  // }

  // gettimeofday(&checkpoint1, NULL);
  // runtime = TIMEDIFF(start, checkpoint1);

  // printf("runtime = %ldns (%.2f s)\n", runtime, (float)runtime/1000000);
}

// void (*jobs[])() = {job0, job1, job2, job3};

static void BM_job3(benchmark::State& st)
{
  if(st.thread_index() == 0)
  {
    NUM_KVS = 20000000;
    // dup_rate = 200000000;
    dup_rate = 5000000;
    // log_size = 
    //   (dup_rate * kv_sz * 4 + SEGMENT_SIZE[0] - 1) 
    //   / SEGMENT_SIZE[0] * SEGMENT_SIZE[0];
    a.resize(num_workers);
    printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);
    db = new DB(db_path, log_size, num_workers, num_cleaners);
    init_zipf();
    prepare();
    tid = new pthread_t[num_workers];
  }
  for(auto _ : st)
  {
    put_KVS(st.thread_index());
  }
  if(st.thread_index() == 0)
  {
    // printf("\nstart checking...\n");
    // std::unique_ptr<DB::Worker> worker = db->GetWorker();
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
    // worker.reset();
    delete db;
  }
}

BENCHMARK(BM_job3)
  ->Iterations(1)
  ->Threads(num_workers);

BENCHMARK_MAIN();