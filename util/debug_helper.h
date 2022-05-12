#pragma once

#include <cstdio>
#include <cstdlib>

// #define LOGGING

#ifdef LOGGING
#define LOG(fmt, ...)                                                          \
  fprintf(stderr, "\033[1;31mLOG(<%s>:%d %s): \033[0m" fmt "\n", __FILE__,     \
          __LINE__, __func__, ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

#define ERROR_EXIT(fmt, ...)                                                   \
  do {                                                                         \
    fprintf(stderr, "\033[1;31mError(<%s>:%d %s): \033[0m" fmt "\n", __FILE__, \
            __LINE__, __func__, ##__VA_ARGS__);                                \
    abort();                                                                   \
  } while (0)
