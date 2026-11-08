//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_BENCH_SNDR_ANY_READ_STREAM_HPP
#define BOOST_CAPY_BENCH_SNDR_ANY_READ_STREAM_HPP

#include <boost/capy/buffers.hpp>

#include "task_scheduler_adapter.hpp"

#include <exec/function.hpp>
#include <stdexec/execution.hpp>

#include <utility>

/// Standalone value-type erased sender stream.
///
/// Mirrors capy::any_read_stream: stores any sender stream behind
/// a vtable, heap-allocated. Does NOT inherit from
/// sndr_io_read_stream — this is a fully independent erasure
/// mechanism.
class sndr_any_read_stream {
  using sched_t = task_scheduler_adaptor;

  using read_some_t = exec::function<
      std::size_t(boost::capy::mutable_buffer) &,
      exec::queries<sched_t(STDEXEC::get_start_scheduler_t) noexcept,
                    std::pmr::polymorphic_allocator<std::byte>(
                        exec::get_frame_allocator_t) noexcept>>;

  using read_some_fn = read_some_t (*)(void *,
                                       boost::capy::mutable_buffer) noexcept;
  using destroy_fn = void (*)(void *) noexcept;

  void *stream_;
  read_some_fn read_some_;
  destroy_fn destroy_;

public:
  template <class Stream>
  explicit sndr_any_read_stream(Stream s)
      : stream_(new Stream(std::move(s))),
        read_some_([](void *stor, boost::capy::mutable_buffer buf) noexcept {
          auto &stream = *static_cast<Stream *>(stor);
          return read_some_t(stream, std::move(buf),
                             [](auto& stream, boost::capy::mutable_buffer buf) noexcept {
                               return stream.read_some(buf);
                             });
        }),
        destroy_(
            [](void *stor) noexcept { delete static_cast<Stream *>(stor); }) {}

  ~sndr_any_read_stream() { destroy_(stream_); }

  sndr_any_read_stream(sndr_any_read_stream const &) = delete;
  sndr_any_read_stream &operator=(sndr_any_read_stream const &) = delete;
  sndr_any_read_stream(sndr_any_read_stream &&) = delete;
  sndr_any_read_stream &operator=(sndr_any_read_stream &&) = delete;

  read_some_t read_some(boost::capy::mutable_buffer buf) {
    return read_some_(stream_, buf);
  }
};

#endif
