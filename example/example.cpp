#include <algorithm>
#include <cmath>
#include <random>
#include <iostream>
#include <string>
#include <experimental/filesystem>
#include <sys/time.h>

#include "config.h"
#include "db.h"
#include "../db/log_structured.h"

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

size_t log_size = 1ul << 30;
// #define log_size 9 * SEGMENT_SIZE
// #define log_size 1ul << 30
#define num_workers 1
#define num_cleaners 1
std::string db_path = std::string(PMEM_DIR) + "log_kvs";
DB *db = new DB(db_path, log_size, num_workers, num_cleaners);
std::unique_ptr<DB::Worker> worker = db->GetWorker();

std::random_device rd;  //Will be used to obtain a seed for the random number engine
std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
std::uniform_int_distribution<> distrib(0, 1);

struct timeval start, checkpoint1, checkpoint2;
int zipf();

void job0()
{ 
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


uint64_t NUM_KVS = 200000000;
uint64_t dup_rate = 10;

void job1()
{
  printf("NUM_KVS = %ld, key dup_rate = %ld\n", NUM_KVS, dup_rate);
  std::string val;
  std::string res;
  uint64_t key, key_;
  gettimeofday(&start, NULL);
  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = i%dup_rate;
    std::string value = "hello world" + i%dup_rate;
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }

  gettimeofday(&checkpoint1, NULL);
  long insert_time = TIMEDIFF(start, checkpoint1);
  printf("insert time : %ld us\n", insert_time);
#ifdef GC_EVAL
  worker.reset();
  delete db;
#endif
}

void job2()
{
  printf("NUM_KVS = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS);
  std::string val;
  std::string res;
  uint64_t key, key_;
  long insert_time = 0;
  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = zipf();
    std::string value = "hello world" + key;
    gettimeofday(&start, NULL);
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    gettimeofday(&checkpoint1, NULL);
    insert_time += TIMEDIFF(start, checkpoint1);
  }

  printf("insert time : %ld us\n", insert_time);
#ifdef GC_EVAL
  worker.reset();
  delete db;
#endif
  worker.reset();
}

void (*jobs[])() = {job0, job1, job2};

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

int zipf()
{
  static int first = true;      // Static first time flag
  static double c = 0;          // Normalization constant
  static double *sum_probs;     // Pre-calculated sum of probabilities
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds
  int n = NUM_KVS;
  double alpha = 0.99;

  // Compute normalization constant on first call only
  if (first == true)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = (double *)malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
    }
    first = false;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = distrib(gen);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n, mid;
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
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
}