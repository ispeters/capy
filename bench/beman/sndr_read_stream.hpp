//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

//
// No-op sender stream for benchmarks.
//
// The stream holds a sender_executor (I/O context handle),
// analogous to how a socket holds a reference to io_context.
// read_some() returns a sender that captures this handle.
// The sender provides both as_awaitable (for coroutine
// consumption) and connect (for sender pipeline consumption).
//

#ifndef BOOST_CAPY_BENCH_SNDR_READ_STREAM_HPP
#define BOOST_CAPY_BENCH_SNDR_READ_STREAM_HPP

#include <stdexec/execution.hpp>

#include <cstddef>
#include <utility>

namespace ex = STDEXEC;

struct sndr_read_stream {
  auto read_some(auto) noexcept {
    return STDEXEC::read_env(STDEXEC::get_scheduler) |
           STDEXEC::let_value([](auto sched) noexcept {
             return STDEXEC::then(STDEXEC::schedule(std::move(sched)),
                                  []() noexcept -> std::size_t { return 0; });
           });
  }
};

#endif
