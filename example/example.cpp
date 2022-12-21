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
uint64_t log_size = 1ul << 30;
// std::string db_path = std::string(PMEM_DIR) + "log_kvs_IGC";
std::string db_path[4] = {std::string(PMEM_DIR[0]) + "log_kvs_IGC",
                          std::string(PMEM_DIR[1]) + "log_kvs_IGC",
                          std::string(PMEM_DIR[2]) + "log_kvs_IGC",
                          std::string(PMEM_DIR[3]) + "log_kvs_IGC"};
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

uint64_t NUM_KVS = 2000000;
uint64_t dup_rate = 500000;
int num_workers = 24;
int num_cleaners = 4;
int kv_sz = 64;
int value_sz = kv_sz - sizeof(KeyType) - sizeof(KVItem);

double c = 0;          // Normalization constant
double *sum_probs = nullptr;     // Pre-calculated sum of probabilities
double alpha = 0.99;

int ret;
uint64_t runtime = 0;
std::vector<int> a;

void init_zipf()
{
  printf("init zipf...\n");
  // Compute normalization constant on first call only
  c = 0;
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
  low = 1, high = dup_rate;
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

int get_random(Random *ra)
{
  return ra->Next()%100;
}

void prepare_key_base()
{
  key_base.resize(num_workers);
  for(int i = 0; i < num_workers; i++)
  {
    for(uint64_t j = 0; j < NUM_KVS; j++)
    {
      uint64_t key = zipf();
      key_base[i].push_back(key);
    }
  }
}

std::map<uint64_t, std::string> kvs;

void *put_KVS(void *id)
{
  uint32_t t_id = *(int *)id;
  uint64_t key;
  int op;
  std::string value = "hello world 22-08-24";
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  value.resize(value_sz);
  struct timeval s, e;
  std::string value1;
  Random ra(t_id + 512);

  for(uint64_t i = 0; i < NUM_KVS; i++)
  {
    key = key_base[t_id][i];
    if(get_random(&ra) > 50)
    {
      worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
    }
    else
    {
      bool found = worker->Get(Slice((const char *)&key, sizeof(KeyType)), &value1);
    }
  }

  worker.reset();
}

void *prefilling()
{
  uint64_t key;
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  std::string value = "hello world 22-09-08";
  value.resize(value_sz);

  for(uint64_t i = 1; i <= dup_rate; i++)
  {
    key = i;
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }

  for(uint64_t i = 0; i <  prefilling_rate * log_size / kv_sz - dup_rate; i++)
  {
    key = zipf();
    // key = uniform();
    worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }
  worker.reset();
}

void prepare()
{
  int ret;

  printf("Preparing key base\n");
  prepare_key_base();
  printf("Preparing key base done...\n");
  
  printf("prefilling\n");
  prefilling();
  printf("prefilling done...\n");
}

static void BM_job3(benchmark::State& st)
{
  if(st.thread_index() == 0)
  {
    for(int i = 0; i < 4; i ++)
    {
      std::experimental::filesystem::remove_all(db_path[i]);
      std::experimental::filesystem::create_directory(db_path[i]);
    }
    num_workers = st.threads();
    a.resize(num_workers);
    printf("NUM_KVS = %ld, dup_rate = %ld, zipf distribute (alpha = 0.99)\n", NUM_KVS, dup_rate);
    db = new DB(db_path, log_size, num_workers, num_cleaners);
    init_zipf();
    prepare();
    tid = new pthread_t[num_workers];
  }
  for(auto _ : st)
  {
    int id = st.thread_index();
    put_KVS(&id);
  }
  if(st.thread_index() == 0)
  {
    delete db;
    free(sum_probs);
    key_base.clear();
  }
}

BENCHMARK(BM_job3)
  ->Iterations(1)
  // ->DenseThreadRange(6, 12, 6)
  ->Threads(1)
  ->Unit(benchmark::kMicrosecond)
  ->UseRealTime();

BENCHMARK_MAIN();



// #include <algorithm>
// #include <cmath>
// #include <random>
// #include <iostream>
// #include <string>
// #include <experimental/filesystem>
// #include <sys/time.h>
// #include <map>
// #include <set>
// #include <fstream>
// #include <unordered_set>

// #include "../benchmarks/bench_base.h"

// #include "config.h"
// #include "db.h"
// #include "../db/log_structured.h"
// using namespace std;

// enum YCSBRunOp {
//     Get,
//     Scan,
//     Update,
//     Insert,
// };

// uint64_t nLoadOp, nRunOp;
// #define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

// #define prefilling_rate 0.75
// uint64_t log_size = 1ul << 30;
// // std::string db_path = std::string(PMEM_DIR) + "log_kvs_IGC";
// std::string db_path[4] = {std::string(PMEM_DIR[0]) + "log_kvs_IGC",
//                           std::string(PMEM_DIR[1]) + "log_kvs_IGC",
//                           std::string(PMEM_DIR[2]) + "log_kvs_IGC",
//                           std::string(PMEM_DIR[3]) + "log_kvs_IGC"};
// DB *db;
// std::random_device rd;  //Will be used to obtain a seed for the random number engine
// std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
// std::uniform_real_distribution<> distrib(0, 1);
// std::unordered_map<uint64_t, uint64_t> re;
// std::vector<uint64_t> time_eval;
// std::vector<std::vector<std::pair<uint64_t, uint64_t>>> key_base;
// std::vector<std::vector<uint64_t>> op_base;
// pthread_t *tid;
// pthread_t *tid_prefilling = new pthread_t;

// struct timeval start, checkpoint1, checkpoint2;
// struct timeval zipf_s, zipf_e;
// long zipf_time_cost = 0;
// uint64_t zipf();
// uint64_t uniform();
// void init_zipf();
// int get_random();
// void prepare_key_base();
// void parseRunFile(std::string wlName, uint64_t len);
// void parseLoadFile(std::string wlName, uint64_t len);
// void parse_line_input_int(string rawStr, uint64_t &key, uint64_t &scanNum, YCSBRunOp &op);

// uint64_t NUM_KVS = 20000000;
// uint64_t dup_rate = 5000000;
// int num_workers = 24;
// int num_cleaners = 4;
// int kv_sz = 64;
// int value_sz = kv_sz - sizeof(KeyType) - sizeof(KVItem);

// double c = 0;          // Normalization constant
// double *sum_probs = nullptr;     // Pre-calculated sum of probabilities
// double alpha = 0.99;

// int ret;
// uint64_t runtime = 0;
// std::vector<int> a;

// std::vector<std::pair<uint64_t, uint64_t>> allRunKeys;
// std::vector<uint64_t> allRunScanSize;
// std::vector<uint64_t> allRunValues;
// std::vector<uint64_t> allLoadKeys;
// std::vector<uint64_t> allLoadValues;

// void init_zipf()
// {
//   printf("init zipf...\n");
//   // Compute normalization constant on first call only
//   c = 0;
//   for (int i=1; i<= dup_rate; i++)
//     c = c + (1.0 / pow((double) i, alpha));
//   c = 1.0 / c;

//   sum_probs = (double *)malloc(( dup_rate + 1)*sizeof(*sum_probs));
//   sum_probs[0] = 0;
//   for (int i=1; i<= dup_rate; i++) {
//     sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
//   }
//   printf("finished...\n");
// }

// uint64_t uniform()
// {
//   uint64_t re = floor( dup_rate * (distrib(gen)));
//   assert(re >= 0 && re <= dup_rate);
//   return re;
// }

// uint64_t zipf()
// {
//   double z;                     // Uniform random number (0 < z < 1)
//   int zipf_value;               // Computed exponential value to be returned
//   int low, high, mid;           // Binary-search bounds

//   // Pull a uniform random number (0 < z < 1)
//   // gettimeofday(&zipf_s, NULL);
//   do
//   {
//     z = distrib(gen);
//   }
//   while ((z < 0.00000001) || (z > 0.99999999));

//   // Map z to the value
//   low = 1, high = dup_rate;
//   do {
//     mid = floor((low+high)/2);
//     if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
//       zipf_value = mid;
//       break;
//     } else if (sum_probs[mid] >= z) {
//       high = mid-1;
//     } else {
//       low = mid+1;
//     }
//   } while (low <= high);

//   // Assert that zipf_value is between 1 and N
//   assert((zipf_value >=1) && (zipf_value <= dup_rate));

//   // gettimeofday(&zipf_e, NULL);
//   // zipf_time_cost += TIMEDIFF(zipf_s, zipf_e);
//   return(zipf_value);
// }

// int get_random(Random *ra)
// {
//   return ra->Next()%100;
// }

// // void prepare_key_base()
// // {
// //   key_base.resize(num_workers);
// //   for(int i = 0; i < num_workers; i++)
// //   {
// //     for(uint64_t j = 0; j < NUM_KVS; j++)
// //     {
// //       uint64_t key = zipf();
// //       key_base[i].push_back(key);
// //     }
// //   }
// // }

// std::map<uint64_t, std::string> kvs;

// void *put_KVS(int id)
// {
//   uint64_t key;
//   int op;
//   std::string value = "hello world 22-08-24";
//   std::unique_ptr<DB::Worker> worker = db->GetWorker();
//   value.resize(value_sz);
//   struct timeval s, e;
//   std::string value1;

//   for(uint64_t i = 0; i < NUM_KVS; i++)
//   {
//     key = key_base[id][i].first;
//     if(key_base[id][i].second)
//     {
//       worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//     }
//     else
//     {
//       bool found = worker->Get(Slice((const char *)&key, sizeof(KeyType)), &value1);
//     }
//   }

//   worker.reset();
// }

// void *prefilling()
// {
//   uint64_t key;
//   std::unique_ptr<DB::Worker> worker = db->GetWorker();
//   std::string value = "hello world 22-09-08";
//   value.resize(value_sz);

//   for(uint64_t i = 1; i <= allLoadKeys.size(); i++)
//   {
//     key = allLoadKeys[i];
//     worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//   }

//   for(uint64_t i = 0; i < allRunKeys.size(); i++)
//   {
//     key = allRunKeys[i].first;
//     worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
//   }
//   worker.reset();
// }

// inline uint64_t myalign(uint64_t len, uint64_t alignment) {
//     uint64_t quotient = len / alignment;
//     uint64_t remainder = len % alignment;
//     return quotient * alignment + alignment * (remainder != 0);
// }

// void parse_line_input_int(string rawStr, uint64_t &key, uint64_t &scanNum, YCSBRunOp &op) {
//     std::string opStr = rawStr.substr(0, 4);
//     uint32_t t = rawStr.find(" ");
//     uint32_t t1 = rawStr.find(" ", t + 1);
//     string keyStr = (opStr == "SCAN") ? rawStr.substr(t) : rawStr.substr(t, t1);
//     char *p;
//     key = strtoull(keyStr.c_str(), &p, 10);
//     if (opStr == "READ") {
//         op = YCSBRunOp::Get;
//     } else if (opStr == "INSE") {
//         op = YCSBRunOp::Insert;
//     } else if (opStr == "UPDA") {
//         op = YCSBRunOp::Update;
//     } else if (opStr == "SCAN") {
//         op = YCSBRunOp::Scan;
//         string scanNumStr = rawStr.substr(t1);
//         scanNum = strtoull(scanNumStr.c_str(), &p, 10);
//     }
// }

// string dir = "wiki";
// // string dir = "amazon";
// uint64_t _testNum = 100000000;
// string type = "a";
// void prepare()
// {
//   printf("Preparing key base\n");
//   parseLoadFile("/scorpio/home/liyiwei/aep-research/testbench/real-world/" + dir + "/load_workloada", _testNum);
//   parseRunFile("/scorpio/home/liyiwei/aep-research/testbench/real-world/" + dir + "/txn_workload" + type, _testNum / 5);

//   key_base.resize(num_workers);
//   for(int i = 0; i < num_workers; i++)
//   {
//     std::random_device rand;
//     std::mt19937 rng(rand());

//     key_base[i] = allRunKeys;
//     std::shuffle(key_base[i].begin(), key_base[i].end(), rng);
//   }

//   printf("Preparing key base done...\n");
// }


// void parseLoadFile(std::string wlName, uint64_t len = 0) {
//     // type 0: uint64_t
//     // type 1: string
//     std::string rawStr;
//     uint64_t opCnt = 0;
//     uint64_t scanNum = 0;
//     YCSBRunOp op;
//     printf("Loading %s...\n", wlName.c_str());
//     fflush(stdout);
//     std::ifstream loadFile(wlName);
//     assert(loadFile.is_open());
//     cout << "ok" << endl;
//     while (opCnt < len && std::getline(loadFile, rawStr)) {
//       uint64_t hashKey;
//       parse_line_input_int(rawStr, hashKey, scanNum, op);
//       if (op == YCSBRunOp::Insert || op == YCSBRunOp::Update) {
//         allLoadKeys.push_back(hashKey);
//         opCnt++;
//       }
//     }
//     loadFile.close();
//     // info("Finish parse load file");
//     cout << "loaded" << endl;
//     nLoadOp = opCnt;
// }

// void parseRunFile(std::string wlName, uint64_t len = 0) {
//     std::string rawStr;
//     YCSBRunOp op;

//     std::ifstream runFile(wlName);
//     assert(runFile.is_open());
//     uint64_t opCnt = 0;
//     uint64_t scanNum = 0;
//     while (opCnt < len && std::getline(runFile, rawStr)) {
//       uint64_t hashKey;
//       parse_line_input_int(rawStr, hashKey, scanNum, op);
//       allRunKeys.push_back(std::pair<uint64_t, uint64_t>(hashKey, op));
//       if (op == YCSBRunOp::Scan) {
//         continue;
//       }
//       opCnt++;
//     }
//     runFile.close();
//     nRunOp = opCnt;
//     // info("Finish parse run file");
//     cout << "run" << endl;
// }

// static void BM_job3(benchmark::State& st)
// {
//   if(st.thread_index() == 0)
//   {
//     for(int i = 0; i < 4; i ++)
//     {
//       std::experimental::filesystem::remove_all(db_path[i]);
//       std::experimental::filesystem::create_directory(db_path[i]);
//     }

//     num_workers = st.threads();

//     prepare();
//     printf("NUM_KVS = %ld\n", allLoadKeys.size());

//     log_size = allLoadKeys.size() * kv_sz * 2 / SEGMENT_SIZE[0] * SEGMENT_SIZE[0];
//     db = new DB(db_path, log_size, num_workers, num_cleaners);
  
//     printf("Loading key base\n");
//     prefilling();
//     printf("Loading key base done...\n");
//   }
//   for(auto _ : st)
//   {
//     int id = st.thread_index();
//     put_KVS(id);
//   }
//   if(st.thread_index() == 0)
//   {
//     delete db;
//     free(sum_probs);
//     key_base.clear();
//   }
// }

// BENCHMARK(BM_job3)
//   ->Iterations(1)
//   // ->DenseThreadRange(6, 12, 6)
//   ->Threads(24)
//   ->Unit(benchmark::kMicrosecond)
//   ->UseRealTime();

// BENCHMARK_MAIN();