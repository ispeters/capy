//
// Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EXAMPLE_SENDER_AWAITABLE_HPP
#define BOOST_CAPY_EXAMPLE_SENDER_AWAITABLE_HPP

#include <boost/capy/continuation.hpp>
#include <boost/capy/error.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/io_result.hpp>

#include <stdexec/execution.hpp>

#include <coroutine>
#include <exception>
#include <new>
#include <stop_token>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <variant>

namespace boost::capy {

namespace detail {

template <class T> struct result {
  union {
    T value_;
    std::exception_ptr error_;
  };

  enum class type { value, error, stopped, empty };

  type type_{type::empty};

  result() noexcept {}

  ~result() {
    if (type_ == type::value) {
      std::destroy_at(&value_);
    } else if (type_ == type::error) {
      std::destroy_at(&error_);
    }
  }

  T &&value() && {
    if (type_ == type::value) {
      return std::move(value_);
    } else if (type_ == type::error) {
      std::rethrow_exception(std::move(error_));
    } else {
      throw std::runtime_error("no value");
    }
  }

  void set_value(T &&t) noexcept {
    try {
      new (&value_) T(std::move(t));
      type_ = type::value;
    } catch (...) {
      set_error(std::current_exception());
    }
  }

  void set_error(std::exception_ptr e) noexcept {
    new (&error_) std::exception_ptr(std::move(e));
    type_ = type::error;
  }

  void set_stopped() noexcept { type_ = type::stopped; }
};

template <> struct result<void> {
  union {
    std::exception_ptr error_;
  };

  enum class type { value, error, stopped, empty };

  type type_{type::empty};

  result() noexcept {}

  ~result() {
    if (type_ == type::error) {
      std::destroy_at(&error_);
    }
  }

  void value() && {
    if (type_ == type::value) {
      return;
    } else if (type_ == type::error) {
      std::rethrow_exception(std::move(error_));
    } else {
      throw std::runtime_error("no value");
    }
  }

  void set_value() noexcept { type_ = type::value; }

  void set_error(std::exception_ptr e) noexcept {
    new (&error_) std::exception_ptr(std::move(e));
    type_ = type::error;
  }

  void set_stopped() noexcept { type_ = type::stopped; }
};

struct io_env_env {
  const boost::capy::io_env *env_;

  struct scheduler {
    using scheduler_concept = STDEXEC::scheduler_tag;

    const boost::capy::io_env *env_;

    template <class Receiver> struct op {
      using operation_state_concept = STDEXEC::operation_state_tag;

      class promise {
        void (*resume_)(void *) noexcept = resume;
        void (*destroy_)(void *) noexcept = [](void *) noexcept {};
        Receiver rcvr_;

        static void resume(void *p) noexcept {
          STDEXEC::set_value(std::move(static_cast<promise *>(p)->rcvr_));
        }

      public:
        explicit promise(Receiver rcvr) noexcept : rcvr_(std::move(rcvr)) {}
      };

      const boost::capy::io_env *env_;
      promise promise_;
      boost::capy::continuation cont_{
          std::coroutine_handle<>::from_address(&promise_)};

      explicit op(const boost::capy::io_env *env, Receiver rcvr) noexcept
          : env_(env), promise_(std::move(rcvr)) {}

      op(op &&) = delete;

      ~op() = default;

      void start() & noexcept { env_->executor.post(cont_); }
    };

    struct sender {
      using sender_concept = STDEXEC::sender_tag;

      const boost::capy::io_env *env_;

      template <class Sender, class... Env>
      static consteval auto get_completion_signatures() noexcept {
        return STDEXEC::completion_signatures<STDEXEC::set_value_t()>{};
      }

      template <class Receiver>
      op<std::remove_cvref_t<Receiver>> connect(Receiver rcvr) const noexcept {
        return op<std::remove_cvref_t<Receiver>>(env_, std::move(rcvr));
      }
    };

    auto schedule() const noexcept { return sender{env_}; }

    friend bool operator==(scheduler, scheduler) noexcept = default;
  };

  std::pmr::polymorphic_allocator<std::byte>
  query(exec::get_frame_allocator_t) const noexcept {
    return std::pmr::polymorphic_allocator<std::byte>(env_->frame_allocator);
  }

  template <class Query>
    requires STDEXEC::__one_of<Query, STDEXEC::get_scheduler_t,
                               STDEXEC::get_start_scheduler_t>
  scheduler query(Query) const noexcept {
    return scheduler{env_};
  }
};

template <class T> struct receiver {
  using receiver_concept = STDEXEC::receiver_tag;

  io_env_env env_;
  result<T> *result_;
  std::coroutine_handle<> coro_;

  template <class... Values>
    requires(sizeof...(Values) == !std::is_void_v<T>)
  void set_value(Values &&...values) && noexcept {
    result_->set_value(std::forward<Values>(values)...);
    coro_.resume();
  }

  void set_error(std::exception_ptr e) && noexcept {
    result_->set_error(std::move(e));
    coro_.resume();
  }

  void set_stopped() && noexcept {
    result_->set_stopped();
    coro_.resume();
  }

  auto get_env() const noexcept { return env_; }
};

template <class... T> struct _value_t {
  using type = std::tuple<T...>;
};

template <class T> struct _value_t<T> {
  using type = T;
};

template <> struct _value_t<> {
  using type = void;
};

template <class... T> using value_t = _value_t<T...>::type;

template <class Sender>
using sender_result_t = STDEXEC::value_types_of_t<Sender, io_env_env, value_t,
                                                  std::type_identity_t>;

template <class Sender> class awaitable {
  Sender sndr_;
  using result_t = sender_result_t<Sender>;
  using op_t = STDEXEC::connect_result_t<Sender, receiver<result_t>>;

  result<result_t> result_;
  union {
    op_t op_;
  };
  bool started_{false};

public:
  awaitable(Sender &&sndr) noexcept : sndr_(std::move(sndr)) {}

  awaitable(const Sender &sndr) noexcept : sndr_(sndr) {}

  awaitable(awaitable &&other) noexcept : sndr_(std::move(other).sndr_) {}

  ~awaitable() {
    if (started_) {
      std::destroy_at(&op_);
    }
  }

  constexpr bool await_ready() const noexcept { return false; }

  template <class Promise>
  std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h,
                                        const boost::capy::io_env *env) {
    static_assert(!std::is_void_v<Promise>);

    new ((void *)&op_) op_t(STDEXEC::connect(
        std::move(sndr_), receiver<result_t>{env, &result_, h}));

    STDEXEC::start(op_);

    started_ = true;

    return std::noop_coroutine();
  }

  decltype(auto) await_resume() noexcept { return std::move(result_).value(); }
};

} // namespace detail

/** Create an IoAwaitable from a
    STDEXEC sender.

    If the sender can complete with
    set_error(std::error_code), the returned
    awaitable yields io_result so the error code
    is a value, not an exception. Otherwise the
    awaitable yields the value directly.

    @par Example
    @code
    capy::task<int> compute(auto sched)
    {
        auto result = co_await await_sender(
            STDEXEC::schedule(sched)
                | STDEXEC::then(
                    [] { return 42; }));
        co_return result;
    }
    @endcode

    @param sndr The sender to bridge.
    @return An IoAwaitable that can be co_awaited
        in a capy::task.
*/
template <class Sender> auto await_sender(Sender &&sndr) {
  return detail::awaitable<std::remove_cvref_t<Sender>>{
      std::forward<Sender>(sndr)};
}

} // namespace boost::capy

#endif
