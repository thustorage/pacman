#include "masstree_wrapper.h"

__thread typename MasstreeWrapper::table_params::threadinfo_type*
    MasstreeWrapper::ti = nullptr;
bool MasstreeWrapper::stopping = false;
uint32_t MasstreeWrapper::printing = 0;
kvtimestamp_t initial_timestamp;

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
