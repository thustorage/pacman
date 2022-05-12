#! /bin/bash -e

PMEM_DIR="/mnt/pmem0"

DB_PATH=(
  "${PMEM_DIR}"/log_kvs
  "${PMEM_DIR}"/viper
  "${PMEM_DIR}"/chameleondb
  "${PMEM_DIR}"/pmem_rocksdb
  "${PMEM_DIR}"/pmemkv_pool
)

for path in "${DB_PATH[@]}"; do
  if [[ -e ${path} ]]; then
    echo "remove ${path}"
    rm -rf ${path}
  fi
done
