#include "ff_btree.h"


/*
 * class btree
 */
btree::btree() {
  root = (char *)new page();
  height = 1;
}

void btree::setNewRoot(char *new_root) {
  this->root = (char *)new_root;
  idx_clflush((char *)&(this->root), sizeof(char *));
  ++height;
}

char *btree::btree_search(entry_key_t key) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p) {
      break;
    }
  }

  if (!t) {
    // printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, LogEntryHelper &le_helper) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }
  
  char *right = (char *)le_helper.new_val;
  if (!p->store(this, NULL, key, right, true, true, nullptr, &le_helper)) {
    btree_insert(key, le_helper);
  }
}

bool btree::btree_try_update(entry_key_t key, LogEntryHelper &le_helper) {
  bool ret = false;
  page *p = reinterpret_cast<page *>(le_helper.shortcut.GetNodeAddr());
  int entry_offset = le_helper.shortcut.GetPos();
  entry *record = &p->records[entry_offset];
  ValueType old_val = le_helper.old_val;
  char *right = (char *)le_helper.new_val;
  assert(p->hdr.leftmost_ptr == NULL);
  p->hdr.mtx->lock();
  if (!p->hdr.is_deleted) {
    // int num_entries = p->count();
    int num_entries = p->hdr.last_index + 1;
    if (entry_offset < num_entries && record->key == key) {
      le_helper.fast_path = true;
      if ((ValueType)record->ptr == old_val) {
        record->ptr = right;
#ifdef BATCH_FLUSH_INDEX_ENTRY
        le_helper.index_entry = reinterpret_cast<char *>(record);
#else
        idx_clwb_fence(record, sizeof(entry));
#endif
      } else {
        // means others update this entry before gc
        le_helper.old_val = (ValueType)right;
      }
      ret = true;
    } else {
      if (key >= p->records[0].key && key <= p->records[num_entries - 1].key) {
        // still in this page
        for (int i = 0; i < num_entries; i++) {
          if (p->records[i].key == key) {
            Shortcut shortcut(reinterpret_cast<char *>(p), i);
            le_helper.shortcut = shortcut;
            le_helper.fast_path = true;
            if ((ValueType)p->records[i].ptr == old_val) {
              p->records[i].ptr = right;
#ifdef BATCH_FLUSH_INDEX_ENTRY
              le_helper.index_entry = reinterpret_cast<char *>(&p->records[i]);
#else
              idx_clwb_fence(&p->records[i], sizeof(entry));
#endif
            } else {
              le_helper.old_val = (ValueType)right;
            }
            ret = true;
            le_helper.shortcut = shortcut;
            break;
          }
        }
        assert(ret == true);
      }

      page *sibling_page = p->hdr.sibling_ptr;
      if (sibling_page && key >= sibling_page->records[0].key) {
        int sib_num_entries = sibling_page->hdr.last_index + 1;
        if (key <= sibling_page->records[sib_num_entries - 1].key) {
          p->hdr.mtx->unlock();
          le_helper.fast_path = true;
          sibling_page->store(this, NULL, key, right, true, true, nullptr,
                              &le_helper);
          return true;
        }
      }
    }
  }
  p->hdr.mtx->unlock();
  return ret;
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right,
                                  uint32_t level) {
  if (level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level)
    p = (page *)p->linear_search(key);

  if (!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p)
      break;
  }

  if (p) {
    if (!p->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    // printf("not found the key to delete %lu\n", key);
  }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level,
                                  entry_key_t *deleted_key,
                                  bool *is_leftmost_node, page **left_sibling) {
  if (level > ((page *)this->root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level) {
    p = (page *)p->linear_search(key);
  }

  p->hdr.mtx->lock();

  if ((char *)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; p->records[i].ptr != NULL; ++i) {
    if (p->records[i].ptr == ptr) {
      if (i == 0) {
        if ((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      } else {
        if (p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (page *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max,
                               unsigned long *buf) {
  page *p = (page *)root;

  while (p) {
    if (p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (page *)p->linear_search(min);
    } else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

void btree::btree_search_range(entry_key_t min, int cnt,
                               std::vector<uint64_t> &vec) {
  page *p = (page *)root;

  while (p) {
    if (p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (page *)p->linear_search(min);
    } else {
      // Found a leaf
      p->linear_search_range(min, cnt, vec);

      break;
    }
  }
}

// void btree::printAll() {
//   pthread_mutex_lock(&print_mtx);
//   int total_keys = 0;
//   page *leftmost = (page *)root;
//   printf("root: %p\n", root);
//   do {
//     page *sibling = leftmost;
//     while (sibling) {
//       if (sibling->hdr.level == 0) {
//         total_keys += sibling->hdr.last_index + 1;
//       }
//       sibling->print();
//       sibling = sibling->hdr.sibling_ptr;
//     }
//     printf("-----------------------------------------\n");
//     leftmost = leftmost->hdr.leftmost_ptr;
//   } while (leftmost);

//   printf("total number of keys: %d\n", total_keys);
//   pthread_mutex_unlock(&print_mtx);
// }

