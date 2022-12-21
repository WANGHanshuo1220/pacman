// Author: Ke Wang, Yiwei Li
// The following code is extracted from ERT, SIGMOD'22

#include <iostream>
#include <fstream>
#include <cstring>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <unordered_map>
using namespace std;

enum YCSBRunOp {
    Get,
    Scan,
    Update,
    Insert,
};

std::vector<uint64_t> allRunKeys;
std::vector<uint64_t> allRunScanSize;
std::vector<uint64_t> allRunValues;
std::vector<uint64_t> allLoadKeys;
std::vector<uint64_t> allLoadValues;

std::vector<char *> allRunKeysStr;
std::vector<uint64_t> allRunKeysLenStr;
std::vector<char *> allLoadKeysStr;
std::vector<uint64_t> allLoadKeysLenStr;

std::vector<YCSBRunOp> allRunOp;

std::unordered_map<uint64_t, uint64_t> *oracleMap;

uint64_t nLoadOp, nRunOp;

void parse_line_input_string(string rawStr, string &key, uint64_t &scanNum, YCSBRunOp &op) {
    std::string opStr = rawStr.substr(0, 4);
    uint32_t t = rawStr.find(" ");
    uint32_t t1 = rawStr.find(" ", t + 1);
    key = (opStr == "SCAN") ? rawStr.substr(t + 1) : rawStr.substr(t + 1, t1);
    char *p;
    if (opStr == "READ") {
        op = YCSBRunOp::Get;
    } else if (opStr == "INSE") {
        op = YCSBRunOp::Insert;
    } else if (opStr == "UPDA") {
        op = YCSBRunOp::Update;
    } else if (opStr == "SCAN") {
        op = YCSBRunOp::Scan;
        string scanNumStr = rawStr.substr(t1);
        scanNum = strtoull(scanNumStr.c_str(), &p, 10);
    }
}

void parse_line_input_int(string rawStr, uint64_t &key, uint64_t &scanNum, YCSBRunOp &op) {
    std::string opStr = rawStr.substr(0, 4);
    uint32_t t = rawStr.find(" ");
    uint32_t t1 = rawStr.find(" ", t + 1);
    string keyStr = (opStr == "SCAN") ? rawStr.substr(t) : rawStr.substr(t, t1);
    char *p;
    key = strtoull(keyStr.c_str(), &p, 10);
    if (opStr == "READ") {
        op = YCSBRunOp::Get;
    } else if (opStr == "INSE") {
        op = YCSBRunOp::Insert;
    } else if (opStr == "UPDA") {
        op = YCSBRunOp::Update;
    } else if (opStr == "SCAN") {
        op = YCSBRunOp::Scan;
        string scanNumStr = rawStr.substr(t1);
        scanNum = strtoull(scanNumStr.c_str(), &p, 10);
    }
}

void parseLine(std::string rawStr, uint64_t &hashKey, uint64_t &hashValue, uint32_t &scanNum, bool isScan) {
    uint32_t t = rawStr.find(" ");
    uint32_t t1 = rawStr.find(" ", t + 1);
    std::string tableName = rawStr.substr(t, t1 - t);
    uint32_t t2 = rawStr.find(" ", t1 + 1);
    uint32_t t3 = rawStr.find(" ", t2 + 1);
    std::string keyName = rawStr.substr(t1, t2 - t1);
    // std::string fullKey = tableName + keyName; // key option1: table+key concat
    // std::string fullKey = keyName; // key option2: only key
    std::string fullKey = keyName.substr(5); // key option3: remove 'user' prefix, out of range
    if (fullKey.size() > 19) // key option4: key's tail, in range uint64_t
        fullKey = fullKey.substr(fullKey.size() - 19); // tail
    std::string fullValue = rawStr.substr(t2);
    std::string scanNumStr;
    if (isScan) {
        scanNumStr = rawStr.substr(t2, t3 - t2);
        fullValue = rawStr.substr(t3);
    }
    static std::hash<std::string> hash_str;
    // info("%s", fullKey.c_str());
    // hashKey = hash_str(fullKey); // use hash, or
    hashKey = std::stoll(fullKey); // just convert to int
    hashValue = hash_str(fullValue);
    scanNum = 0;
    if (isScan) {
        scanNum = std::stoi(scanNumStr);
    }
    // info("key:%s, hashed_key: %lx, hashed_value: %lx, scanNum: %d", fullKey.c_str(), hashKey, hashValue, scanNum);
    // info("hashed_key: %lx, hashed_value: %lx", hashKey, hashValue);
}

inline uint64_t myalign(uint64_t len, uint64_t alignment) {
    uint64_t quotient = len / alignment;
    uint64_t remainder = len % alignment;
    return quotient * alignment + alignment * (remainder != 0);
}

// Function 1: parse YCSB file

void parseYCSBRunFile(std::string wlName, bool correctCheck = false) {
    std::string rawStr;
    uint64_t hashKey, hashValue;

    std::ifstream runFile(wlName + ".run");
    assert(runFile.is_open());
    uint64_t opCnt = 0;
    uint32_t scanNum = 0;
    while (std::getline(runFile, rawStr)) {
        assert(rawStr.size() > 4);
        std::string opStr = rawStr.substr(0, 4);
        if (opStr == "READ") {
            // info("%s", rawStr.c_str());
            parseLine(rawStr, hashKey, hashValue, scanNum, false);
            allRunKeys.push_back(hashKey); // buffer this query
            allRunValues.push_back(0);
            allRunOp.push_back(YCSBRunOp::Get);
            opCnt++;
        } else if (opStr == "INSE" || opStr == "UPDA") {
            // info("%s", rawStr.c_str());
            parseLine(rawStr, hashKey, hashValue, scanNum, false);
            allRunKeys.push_back(hashKey);
            allRunValues.push_back(hashValue);
            allRunOp.push_back(YCSBRunOp::Update);
            if (correctCheck) (*oracleMap)[hashKey] = hashValue;
            opCnt++;
        } else if (opStr == "SCAN") {
            parseLine(rawStr, hashKey, hashValue, scanNum, true);
            allRunKeys.push_back(hashKey);
            // allRunValues.push_back(1024);
            allRunValues.push_back(scanNum);
            allRunOp.push_back(YCSBRunOp::Scan);
            opCnt++;
        } else {
            // other info
        }
    }
    runFile.close();
    nRunOp = opCnt;
    // info("Finish parse run file");
    cout << "run" << endl;
    if (correctCheck) {
        // TODO: compare results between oracleMap and your own index
    }
}

void parseYCSBLoadFile(std::string wlName, bool correctCheck) {
    std::string rawStr;
    uint64_t opCnt = 0;
    uint32_t scanNum = 0;
    uint64_t hashKey, hashValue;
    std::ifstream loadFile(wlName + ".load");
    assert(loadFile.is_open());
    cout << "ok" << endl;
    while (std::getline(loadFile, rawStr)) {
        assert(rawStr.size() > 4);
        std::string opStr = rawStr.substr(0, 4);
        if (opStr == "INSE" || opStr == "UPDA") {
            // info("%s", rawStr.c_str());
            parseLine(rawStr, hashKey, hashValue, scanNum, false);
            allLoadKeys.push_back(hashKey);
            allLoadValues.push_back(hashValue);
            if (correctCheck) (*oracleMap)[hashKey] = hashValue;
            opCnt++;
        } else {
            // other info
        }
    }
    loadFile.close();
    // info("Finish parse load file");
    cout << "loaded" << endl;
    nLoadOp = allLoadKeys.size();
}

// Function 2: parse real-world SOSD file: wiki, facebook

void parseLoadFile(std::string wlName, uint64_t len = 0, uint64_t type = 0) {
    // type 0: uint64_t
    // type 1: string
    std::string rawStr;
    uint64_t opCnt = 0;
    uint64_t scanNum = 0;
    YCSBRunOp op;
    printf("Loading %s...\n", wlName.c_str());
    fflush(stdout);
    std::ifstream loadFile(wlName);
    assert(loadFile.is_open());
    cout << "ok" << endl;
    while (opCnt < len && std::getline(loadFile, rawStr)) {
        if (type == 0) {
            uint64_t hashKey;
            parse_line_input_int(rawStr, hashKey, scanNum, op);
            if (op == YCSBRunOp::Insert || op == YCSBRunOp::Update) {
                allLoadKeys.push_back(hashKey);
                opCnt++;
            }
        } else if (type == 1) {
            string hashKey;
            parse_line_input_string(rawStr, hashKey, scanNum, op);
            if (op == YCSBRunOp::Insert || op == YCSBRunOp::Update) {
                int tmplen = myalign(hashKey.size(), 4);
                if (tmplen) {
                    char *tmp = new char[tmplen];
                    memcpy(tmp, hashKey.c_str(), hashKey.size());
                    for (int i = hashKey.size(); i < tmplen; i++)
                        tmp[i] = rand() % 256;
                    allLoadKeysStr.push_back(tmp);
                    allLoadKeysLenStr.push_back(tmplen - 1);

                    opCnt++;
                }
            }
        }
    }
    loadFile.close();
    // info("Finish parse load file");
    cout << "loaded" << endl;
    nLoadOp = opCnt;
}

void parseRunFile(std::string wlName, uint64_t len = 0, uint64_t type = 0) {
    std::string rawStr;
    YCSBRunOp op;

    std::ifstream runFile(wlName);
    assert(runFile.is_open());
    uint64_t opCnt = 0;
    uint64_t scanNum = 0;
    while (opCnt < len && std::getline(runFile, rawStr)) {
        if (type == 0) {
            uint64_t hashKey;
            parse_line_input_int(rawStr, hashKey, scanNum, op);
            allRunKeys.push_back(hashKey);
            allRunOp.push_back(op);
            opCnt++;
            if (op == YCSBRunOp::Scan) {
                allRunScanSize.push_back(scanNum);
            }
        } else if (type == 1) {
            string hashKey;
            parse_line_input_string(rawStr, hashKey, scanNum, op);
            int tmplen = myalign(hashKey.size(), 4) + 1;
            char *tmp = new char[tmplen];
            memcpy(tmp, hashKey.c_str(), hashKey.size());
            for (int i = hashKey.size(); i < tmplen; i++)
                tmp[i] = 1;
            tmp[tmplen] = '\0';
            allRunKeysStr.push_back(tmp);
            allRunKeysLenStr.push_back(tmplen - 1);
            allRunOp.push_back(op);
            opCnt++;
            if (op == YCSBRunOp::Scan) {
                allRunScanSize.push_back(scanNum);
            }
        }
    }
    runFile.close();
    nRunOp = opCnt;
    // info("Finish parse run file");
    cout << "run" << endl;
}

// Function 3: parse amazon review dataset
void amazon_review(const string fileName) {
    ifstream file(fileName);
    if (!file.good()) {
        cout << fileName << " not existed!" << endl;
        return;
    }
    int testNum = 15023059;
    unsigned char **keys = new unsigned char *[testNum];
    int *lengths = new int[testNum];

    const int keyLength = 16;
    string buffer;
    int pos = 0;
    while (getline(file, buffer)) {
        lengths[pos] = keyLength;
        keys[pos] = static_cast<unsigned char *>( malloc(lengths[pos]));
//        memset(keys[pos],0,lengths[pos]);
        int j = lengths[pos] - 1;
        for (int i = buffer.size() - 1; i >= 0; i--, j--) {
            keys[pos][j] = (unsigned char) buffer[i];
        }
        for (; j >= 0; j--) {
            keys[pos][j] = 1;
        }
        pos++;
    }
    file.close();
    cout << "Finish reading the file and make the dataset! Contains " << pos << " keys" << endl;

}


void SOSD(uint64_t _testNum = 100000000, string dir = "facebook", string type = "a") {
    assert(allLoadKeys.size() == 0);
    parseLoadFile("/scorpio/home/liyiwei/aep-research/testbench/real-world/" + dir + "/load_workloada", _testNum);
    parseRunFile("/scorpio/home/liyiwei/aep-research/testbench/real-world/" + dir + "/txn_workload" + type, _testNum / 5);

    printf("workload %s: load-phase load %ld keys, txn-phase load %ld keys\n", dir.c_str(), allLoadKeys.size(), allRunKeys.size());
}

int main() {
    SOSD(100000000, "amazon", "a");

    // amazon_review("az.txt");
    // SOSD(100000000, "facebook", "a");
    // SOSD(10000000, "facebook", "c");
    // SOSD(10000000, "facebook", "e");
    // SOSD(20000000, "amazon", "a");
    // SOSD(20000000, "amazon", "c");
    // SOSD(20000000, "amazon", "e");
    // SOSD(50000000, "wiki", "a");
    // SOSD(50000000, "wiki", "c");
    // SOSD(50000000, "wiki", "e");

}
