//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_BENCH_IOAW_READ_STREAM_HPP
#define BOOST_CAPY_BENCH_IOAW_READ_STREAM_HPP

#include <boost/capy/buffers.hpp>
#include <boost/capy/concept/read_stream.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/io_result.hpp>
#include <coroutine>
#include <cstddef>

#include <exec/function.hpp>
#include <stdexec/execution.hpp>

/// No-op ReadStream for benchmarking.
///
/// Uses the executor from io_env (passed by capy::task's
/// transform_awaiter) to post the coroutine back. Satisfies
/// ReadStream so it can be wrapped by capy::any_read_stream.
struct ioaw_read_stream {

  struct read_awaitable {
    struct receiver {
      using receiver_concept = STDEXEC::receiver_tag;

      std::coroutine_handle<> h;
      read_awaitable *a;

      void set_value() && noexcept {
        auto coro = h;
        std::destroy_at(&a->op_);
        coro.resume();
      }

      void set_stopped() && noexcept { std::terminate(); }
    };

    using func_t = exec::function<void(void *) noexcept>;

    using op_t = STDEXEC::connect_result_t<func_t, receiver>;

    union {
      op_t op_;
      boost::capy::continuation cont_;
    };

    read_awaitable() noexcept {}

    read_awaitable(const read_awaitable &other) noexcept {}

    ~read_awaitable() {}

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h,
                                          boost::capy::io_env const *env) {
      static_assert(
          std::is_trivially_destructible_v<boost::capy::continuation>);
      new ((void *)&cont_) boost::capy::continuation();

      cont_.h = h;
      env->executor.post(cont_);
      return std::noop_coroutine();
    }

    template <class Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) {
      func_t func = func_t(h.address(), [](void *ptr) noexcept {
        auto h = std::coroutine_handle<Promise>::from_address(ptr);
        auto &promise = h.promise();

        auto sch = STDEXEC::get_start_scheduler(STDEXEC::get_env(promise));
        return STDEXEC::schedule(std::move(sch));
      });

      new ((void *)&op_)
          op_t(STDEXEC::connect(std::move(func), receiver{h, this}));
      STDEXEC::start(op_);

      return std::noop_coroutine();
    }

    boost::capy::io_result<std::size_t> await_resume() noexcept {
      return {{}, 0};
    }
  };

  template <boost::capy::MutableBufferSequence MB>
  read_awaitable read_some(MB) {
    return {};
  }
};

static_assert(boost::capy::ReadStream<ioaw_read_stream>);

#endif
