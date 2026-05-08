#ifndef BOOST_CAPY_BENCH_TASK_SCHEDULER_ADAPTER_HPP
#define BOOST_CAPY_BENCH_TASK_SCHEDULER_ADAPTER_HPP

#include <exec/function.hpp>
#include <stdexec/execution.hpp>

#include <utility>

struct task_scheduler_adaptor : STDEXEC::task_scheduler {
  template <STDEXEC::scheduler Sched>
    requires STDEXEC::__not_decays_to<Sched, task_scheduler_adaptor>
  task_scheduler_adaptor(Sched sched)
      : STDEXEC::task_scheduler(std::move(sched)) {}
};

#endif
