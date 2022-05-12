#pragma once

#include <immintrin.h>
#include <assert.h>
#include "config.h"

#define CACHE_LINE_SIZE 64
#define FLUSH_ALIGN ((uintptr_t)CACHE_LINE_SIZE)
#define force_inline __attribute__((always_inline)) inline

static inline void mfence() { asm volatile("mfence" ::: "memory"); }

static inline void sfence() { _mm_sfence(); }

static inline void clflush(const void *data, int len) {
  volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE - 1));
  for (; ptr < (char *)data + len; ptr += CACHE_LINE_SIZE) {
    asm volatile("clflush %0" : "+m"(*(volatile char *)ptr));
  }
  sfence();
}

static inline void idx_clflush(const void *data, int len) {
#ifdef IDX_PERSISTENT
  clflush(data, len);
#endif
}

static force_inline void
pmem_clflushopt(const void *addr)
{
	asm volatile(".byte 0x66; clflush %0" : "+m" \
		(*(volatile char *)(addr)));
}
static force_inline void
pmem_clwb(const void *addr)
{
	asm volatile(".byte 0x66; xsaveopt %0" : "+m" \
		(*(volatile char *)(addr)));
}

typedef void flush_fn(const void *, size_t);

static force_inline void
flush_clflush_nolog(const void *addr, size_t len)
{
	uintptr_t uptr;

	/*
	 * Loop through cache-line-size (typically 64B) aligned chunks
	 * covering the given range.
	 */
	for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
		uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
		_mm_clflush((char *)uptr);
}

static force_inline void
clflush_fence(const void *addr, size_t len)
{
	uintptr_t uptr;
	for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
		uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
		_mm_clflush((char *)uptr);
	_mm_sfence();
}

static force_inline void
clflushopt_fence(const void *addr, size_t len)
{
	uintptr_t uptr;
	for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
		uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
		pmem_clflushopt((char *)uptr);
	_mm_sfence();
}

static force_inline void
clwb_fence(const void *addr, size_t len)
{
	uintptr_t uptr;
	for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
		uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
		pmem_clwb((char *)uptr);
	_mm_sfence();
}

static force_inline void
idx_clflush_fence(const void *addr, size_t len)
{
#ifdef IDX_PERSISTENT
	clflush_fence(addr, len);
#endif
}

static force_inline void
idx_clflushopt_fence(const void *addr, size_t len)
{
#ifdef IDX_PERSISTENT
	clflushopt_fence(addr, len);
#endif
}

static force_inline void
idx_clwb_fence(const void *addr, size_t len)
{
#ifdef IDX_PERSISTENT
	clwb_fence(addr, len);
#endif
}
