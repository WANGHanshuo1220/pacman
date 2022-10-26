#! /bin/bash -e

PMEM_DIR0="/mnt/aep0"
PMEM_DIR1="/mnt/aep1"

DB_PATH=(
  "${PMEM_DIR0}"/log_kvs
  "${PMEM_DIR0}"/log_kvs_IGC
  "${PMEM_DIR1}"/log_kvs
  "${PMEM_DIR1}"/log_kvs_IGC
  # "${PMEM_DIR}"/viper
  # "${PMEM_DIR}"/chameleondb
  # "${PMEM_DIR}"/pmem_rocksdb
  # "${PMEM_DIR}"/pmemkv_pool
)

for path in "${DB_PATH[@]}"; do
  if [[ -e ${path} ]]; then
    echo "remove ${path}"
    rm -rf ${path}
  fi
done
