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

#include <exec/completion_behavior.hpp>

#include <cstddef>
#include <utility>

namespace ex = STDEXEC;

inline auto make_sender() noexcept {
  return STDEXEC::read_env(STDEXEC::get_scheduler) |
         STDEXEC::let_value([](auto sched) noexcept {
           return STDEXEC::then(STDEXEC::schedule(std::move(sched)),
                                []() noexcept -> std::size_t { return 0; });
         });
}

class sndr_read_stream {
  using sender_t = decltype(make_sender());

  struct sender {
    using sender_concept = STDEXEC::sender_tag;

    template <class Sender, class Env>
    static consteval auto get_completion_signatures() noexcept {
      return STDEXEC::completion_signatures_of_t<sender_t, Env>{};
    }

    auto get_env() const noexcept {
      return STDEXEC::prop(
          exec::get_completion_behavior_t<STDEXEC::set_value_t>(),
          exec::completion_behavior::asynchronous);
    }

    template <class Receiver>
    auto connect(Receiver rcvr) const
        noexcept(STDEXEC::__nothrow_connectable<sender_t, Receiver>) {
      return STDEXEC::connect(make_sender(), std::move(rcvr));
    }
  };

public:
  constexpr auto read_some(auto) noexcept { return sender{}; }
};

#endif
