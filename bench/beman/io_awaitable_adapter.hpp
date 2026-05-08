#ifndef BOOST_CAPY_BENCH_IO_AWAITABLE_ADAPTER_HPP
#define BOOST_CAPY_BENCH_IO_AWAITABLE_ADAPTER_HPP

#include <boost/capy/concept/io_awaitable.hpp>
#include <boost/capy/ex/execution_context.hpp>
#include <boost/capy/ex/io_env.hpp>

#include <exec/function.hpp>
#include <stdexec/execution.hpp>

#include <coroutine>

class executor_adapter {
  struct receiver {
    using receiver_concept = STDEXEC::receiver_tag;

    const executor_adapter *self;

    void set_value(std::coroutine_handle<> h) && noexcept {
      std::destroy_at(&self->op_);
      h.resume();
    }

    void set_error(std::exception_ptr) && noexcept { std::terminate(); }

    void set_stopped() && noexcept { std::terminate(); }

    auto get_env() const noexcept {
      assert(self->alloc_.has_value());
      return STDEXEC::prop(exec::get_frame_allocator, *self->alloc_);
    }
  };

  using func_t = exec::function<std::coroutine_handle<>(void *)>;

  struct ctx : boost::capy::execution_context {
    ctx() noexcept : boost::capy::execution_context(this) {}

    ctx(const ctx &) noexcept {}
  } ctx_;

  using op_t = STDEXEC::connect_result_t<func_t, receiver>;

  std::optional<std::pmr::polymorphic_allocator<std::byte>> alloc_;
  mutable std::optional<func_t> scheduler_;
  union {
    mutable op_t op_;
  };

public:
  executor_adapter() noexcept {}

  ~executor_adapter() {}

  const ctx &context() const noexcept { return ctx_; }

  ctx &context() noexcept { return ctx_; }

  void on_work_started() const noexcept {}

  void on_work_finished() const noexcept {}

  void post(boost::capy::continuation &) const noexcept {
    assert(scheduler_.has_value());
    new (&op_) op_t(STDEXEC::connect(std::move(*scheduler_), receiver{this}));
    scheduler_.reset();
    STDEXEC::start(op_);
  }

  std::coroutine_handle<>
  dispatch(boost::capy::continuation &cont) const noexcept {
    post(cont);
    return std::noop_coroutine();
  }

  friend bool operator==(const executor_adapter &,
                         const executor_adapter &) noexcept {
    return true;
  }

  template <boost::capy::IoAwaitable Awaitable> friend class ioaw_adapter;
};

template <boost::capy::IoAwaitable Awaitable> class ioaw_adapter {
  Awaitable awaitable_;

  struct awaitable {
    Awaitable awaitable_;
    executor_adapter exec_;
    boost::capy::io_env env_{exec_};

    constexpr bool await_ready() noexcept(noexcept(awaitable_.await_ready())) {
      return awaitable_.await_ready();
    }

    template <class Promise>
    constexpr std::coroutine_handle<>
    await_suspend(std::coroutine_handle<Promise> h) {
      const auto &env = STDEXEC::get_env(h.promise());
      if constexpr (requires {
                      env_.stop_token = STDEXEC::get_stop_token(env);
                    }) {
        env_.stop_token = STDEXEC::get_stop_token(env);
      }
      exec_.alloc_.emplace(exec::get_frame_allocator(env));
      env_.frame_allocator = exec_.alloc_->resource();

      exec_.scheduler_.emplace(h.address(), [](void *p) noexcept {
        auto h = std::coroutine_handle<Promise>::from_address(p);
        const auto &env = STDEXEC::get_env(h.promise());

        auto sched = STDEXEC::get_start_scheduler(env);

        return STDEXEC::schedule(std::move(sched)) |
               STDEXEC::then([p]() noexcept {
                 return std::coroutine_handle<>::from_address(p);
               });
      });

      return awaitable_.await_suspend(h, &env_);
    }

    constexpr auto await_resume() noexcept(noexcept(awaitable_.await_resume()))
        -> decltype(awaitable_.await_resume()) {
      return awaitable_.await_resume();
    }
  };

public:
  ioaw_adapter(Awaitable awaitable) noexcept(
      std::is_nothrow_move_constructible_v<Awaitable>)
      : awaitable_(std::move(awaitable)) {}

  awaitable operator co_await() noexcept(
      std::is_nothrow_move_constructible_v<Awaitable>) {
    return awaitable{std::move(awaitable_)};
  }
};

#endif
