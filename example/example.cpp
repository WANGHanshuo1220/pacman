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

#define prefilling_rate 0.75
uint64_t log_size = 1ul << 30;
int num_workers = 0;
int num_cleaners = 0;
std::string db_path = std::string(PMEM_DIR) + "log_kvs";
DB *db;
std::random_device rd;  //Will be used to obtain a seed for the random number engine
std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
std::uniform_real_distribution<> distrib(0, 1);

struct timeval start, checkpoint1, checkpoint2;
struct timeval zipf_s, zipf_e;
long zipf_time_cost = 0;
uint64_t zipf();
void init_zipf();
int get_random();
class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;  // 2^31-1
    static const uint64_t A = 16807;        // bits 14, 8, 7, 5, 2, 1, 0
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  uint32_t Uniform(int n) { return Next() % n; }
  bool OneIn(int n) { return (Next() % n) == 0; }
  uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
};
uint64_t NUM_KVS = 0;
uint64_t dup_rate = 0;

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
  num_workers = 1;
  num_cleaners = 1;
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
  num_workers = 1;
  num_cleaners = 1;
  printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);
  std::map<uint64_t, std::string> kvs;
  std::string val;
  std::string res;
  uint64_t key;
  long insert_time = 0;
  std::string value = "hello world 22-08-30";
  value.resize(32);

  db = new DB(db_path, log_size, num_workers, num_cleaners);
  // db->start_GCThreads();
  std::unique_ptr<DB::Worker> worker = db->GetWorker();

  // printf("prefilling\n");
  // for(uint64_t i = 0; i <  prefilling_rate * log_size / 48; i++)
  // {
  //   key = zipf();
  //   value.resize(32);
  //   kvs[key] = value;
  //   worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  // }

  // printf("prefilling done...\n");
  // db->start_GCThreads();

  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = zipf();
    value = value + std::to_string(i%10);
    value.resize(32);
    kvs[key] = value;
    gettimeofday(&start, NULL);
    // Timer time(insert_time);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    gettimeofday(&checkpoint1, NULL);
    insert_time += TIMEDIFF(start, checkpoint1);
  }

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

  worker.reset();
  delete db;

  // printf("run time : \t(%ldus - %ldus) = %ld us \t(%ld s)\n", 
  //   insert_time, zipf_time_cost, insert_time - zipf_time_cost,
  //   (insert_time - zipf_time_cost)/1000000);
  printf("%ld keys\n", kvs.size());
  printf("run time : %ld ns \t(%.2f s)\n", 
    insert_time, (float)insert_time/1000000);
}

std::map<uint64_t, std::string> kvs;

void *put_KVS(void *)
{
  uint64_t key;
  int op;
  std::string value = "hello world 22-08-24";
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  value.resize(32);

  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = zipf();
    op = get_random();
    if(op < 50)
    {
      worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    }
    else
    {
      std::string value;
      bool found = worker->Get(Slice((const char *)&key, sizeof(KeyType)), &value);
    }
  }

  worker.reset();
}

void *prefilling(void *)
{
  uint64_t key;
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  std::string value = "hello world 22-09-08";

  for(uint64_t i = 0; i <  prefilling_rate * log_size / (52 * num_workers); i++)
  {
    key = zipf();
    value.resize(32);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }
  worker.reset();
}

void job3()
{
  num_cleaners = 1;
  num_workers = 24;
  pthread_t *tid_prefilling = new pthread_t[num_workers];
  pthread_t *tid = new pthread_t[num_workers];
  int ret;
  uint64_t runtime = 0;

  db = new DB(db_path, log_size, num_workers, num_cleaners);
  // db->start_GCThreads();
  
  printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);

  printf("prefilling\n");
  for(int i = 0; i < num_workers; i++)
  {
    ret = pthread_create(&tid_prefilling[i], NULL, &prefilling, NULL); 
    if(ret != 0)
    {
      printf("%dth thread create error\n", i);
      exit(0);
    }
  }
  for(int i = 0; i < num_workers; i++)
  {
    pthread_join(tid_prefilling[i], NULL);
  }
  printf("prefilling done...\n");

  // db->start_GCThreads();
  gettimeofday(&start, NULL);

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

  gettimeofday(&checkpoint1, NULL);
  runtime = TIMEDIFF(start, checkpoint1);

  // printf("\nstart checking...\n");
  // std::string val_;
  // uint64_t key_;
  // int find_kvs = 0;
  // int false_kv = 0;
  // bool c = true;
  // std::unique_ptr<DB::Worker> worker = db->GetWorker();
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
  printf("runtime = %ldns (%.2f s)\n", runtime, (float)runtime/1000000);
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
  init_zipf();
  for (int i = 0; i < 20; i++)
  {  
    printf("----------------%d-----------------\n", i);
    (*jobs[atoi(arg)])();
  }
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

Random re((unsigned)time(NULL));
int get_random()
{
  return re.Next()%100;
}

