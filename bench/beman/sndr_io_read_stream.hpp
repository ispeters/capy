//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_BENCH_SNDR_IO_READ_STREAM_HPP
#define BOOST_CAPY_BENCH_SNDR_IO_READ_STREAM_HPP

#include "sndr_read_stream.hpp"

#include <boost/capy/buffers.hpp>

#include <exec/function.hpp>
#include <stdexec/execution.hpp>

/// Abstract interface for sender-based read streams.
struct sndr_io_read_stream {
  // using sched_t =
  // decltype(std::declval<STDEXEC::run_loop>().get_scheduler());
  using sched_t = STDEXEC::task_scheduler;

  using read_some_t =
      exec::function<std::size_t(boost::capy::mutable_buffer),
                     exec::queries<sched_t(STDEXEC::get_scheduler_t) noexcept,
                                   std::pmr::polymorphic_allocator<std::byte>(
                                       exec::get_frame_allocator_t) noexcept>>;
  virtual read_some_t read_some(boost::capy::mutable_buffer) = 0;
  virtual ~sndr_io_read_stream() = default;
};

/// Concrete implementation wrapping sndr_read_stream.
struct sndr_io_read_stream_impl : sndr_io_read_stream {
  sndr_read_stream stream_;

  read_some_t read_some(boost::capy::mutable_buffer buf) override {
    return read_some_t(std::move(buf), [this](auto buf) noexcept {
      return stream_.read_some(buf);
    });
  }
};

#endif
