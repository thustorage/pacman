#include <iostream>
#include <string>
#include <filesystem>

#include "config.h"
#include "db.h"


int main() {
  size_t log_size = 1ul << 30;
  int num_workers = 1;
  int num_cleaners = 1;
  std::string db_path = std::string(PMEM_DIR) + "log_kvs";
  DB *db = new DB(db_path, log_size, num_workers, num_cleaners);
  std::unique_ptr<DB::Worker> worker = db->GetWorker();

  uint64_t key = 0x1234;
  std::string value = "hello world";
  worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));

  std::string val;
  worker->Get(Slice((const char *)&key, sizeof(uint64_t)), &val);
  std::cout << "value: " << val << std::endl;
  return 0;
}
