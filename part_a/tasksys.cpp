#include "tasksys.h"
#include <vector>
#include <thread>
#include <deque>
#include <functional>
#include <condition_variable>
#include <random>
#include <memory>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), num_threads_(num_threads), total_tasks_remaining_(0), mutexes_(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    for (int i = 0; i < num_threads; i++)
    {
        task_queues_.push_back(std::deque<std::function<void()>>());
    }
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

// 静态调度
//  void TaskSystemParallelSpawn::runPartly(IRunnable *runnable, int thread_index, int num_total_tasks) {
//      for (int i = 2 * thread_index; i < num_total_tasks; i += 2 * num_threads_) {
//          runnable->runTask(i, num_total_tasks);
//          if (i + 1 < num_total_tasks) runnable->runTask(i + 1, num_total_tasks);
//      }
//  }
// 动态调度
// void TaskSystemParallelSpawn::runPartly(IRunnable *runnable, int thread_index, int num_total_tasks)
// {
//     auto &queue = task_queues_[thread_index];
//     int random_num = 0;
//     for (int i = thread_index; i < num_total_tasks; i += num_threads_)
//     {
//         queue.emplace_back([runnable, i, num_total_tasks]() -> void
//                            { runnable->runTask(i, num_total_tasks); });
//     }
//     while (!isAllDone())
//     {
//         std::lock_guard<std::mutex> lock(mutexes_[thread_index]);
//         while (!queue.empty())
//         {
//             queue.back()();
//             queue.pop_back();
//             --total_tasks_remaining_;
//         }
//         // 开偷
//         if (queue.empty() && thread_index >= 1)
//         {
//             std::random_device rd;
//             std::mt19937 gen(rd());
//             std::uniform_int_distribution<> dist(0, thread_index - 1);
//             random_num = dist(gen);

//             auto &other_queue = task_queues_[random_num];
//             std::lock_guard<std::mutex> lock(mutexes_[random_num]);
//             if (!other_queue.empty())
//             {
//                 queue.emplace_front(other_queue.front());
//                 other_queue.pop_front();
//             }
//         }
//     }
// }

bool TaskSystemParallelSpawn::isAllDone()
{
    return total_tasks_remaining_.load() == 0;
}
// void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
// {

//     //
//     // TODO: CS149 students will modify the implementation of this
//     // method in Part A.  The implementation provided below runs all
//     // tasks sequentially on the calling thread.
//     //
//     total_tasks_remaining_ = num_total_tasks;
//     std::vector<std::thread> threads;
//     for (int i = 0; i < num_threads_; i++)
//     {
//         threads.push_back(std::thread(&TaskSystemParallelSpawn::runPartly, this, runnable, i, num_total_tasks));
//     }
//     for (auto &thread : threads)
//     {
//         thread.join();
//     }
// }
// 高级动态调度
void TaskSystemParallelSpawn::runPartly(IRunnable *runnable, int num_total_tasks, std::atomic<int> &curr_task)
{
    while (true)
    {
        int task_index = curr_task.fetch_add(1);
        if (task_index >= num_total_tasks)
        {
            break;
        }
        runnable->runTask(task_index, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    std::atomic<int> curr_task(0);
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads_; ++i)
    {
        threads.emplace_back([this, runnable, num_total_tasks, &curr_task]()
                             { runPartly(runnable, num_total_tasks, curr_task); });
    }

    for (auto &thread : threads)
    {
        thread.join();
    }
}
TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */
const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}
TasksState::TasksState() : runnable_(nullptr), finished_tasks_(0), left_tasks_(0), num_total_tasks_(0)
{
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads), state_(), killed_(false), num_threads_(num_threads)
{
    threads_pool_.resize(num_threads);
    for (int i = 0; i < num_threads; i++)
    {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    killed_ = true;
    for (int i = 0; i < num_threads_; i++)
    {
        threads_pool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::spinningThread()
{
    int id;
    int total;
    while (true)
    {
        if (state_.runnable_ == nullptr)
            continue;
        if (killed_)
            break;
        state_.mutex_.lock();
        total = state_.num_total_tasks_;
        id = total - state_.left_tasks_;
        if (id < total)
            state_.left_tasks_--;
        state_.mutex_.unlock();
        if (id < total)
        {
            state_.runnable_->runTask(id, total);
            state_.mutex_.lock();
            state_.finished_tasks_++;
            if (state_.finished_tasks_ == total)
            {
                state_.mutex_.unlock();
                state_.finished_mutex_.lock();
                state_.finished_mutex_.unlock();
                state_.finished_.notify_all();
            }
            else
            {
                state_.mutex_.unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    std::unique_lock<std::mutex> lk(state_.finished_mutex_);
    state_.mutex_.lock();
    state_.finished_tasks_ = 0;
    state_.left_tasks_ = num_total_tasks;
    state_.num_total_tasks_ = num_total_tasks;
    state_.runnable_ = runnable;
    state_.mutex_.unlock();
    state_.finished_.wait(lk);
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads), state_(), killed_(false), num_threads_(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    threads_pool_.resize(num_threads);
    for (int i = 0; i < num_threads; i++)
    {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    killed_ = true;
    // for (int i = 0; i < num_threads_; i++) {
        has_tasks_.notify_all();
    // }
    for (int i = 0; i < num_threads_; i++)
    {
        threads_pool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread()
{
    int id;
    int total;
    while (true)
    {
        if (state_.runnable_ == nullptr)
            continue;
        if (killed_)
            break;
        state_.mutex_.lock();
        total = state_.num_total_tasks_;
        id = total - state_.left_tasks_;
        if (id < total)
            state_.left_tasks_--;
        state_.mutex_.unlock();
        if (id < total)
        {
            state_.runnable_->runTask(id, total);
            state_.mutex_.lock();
            state_.finished_tasks_++;
            if (state_.finished_tasks_ == total)
            {
                state_.mutex_.unlock();
                state_.finished_mutex_.lock();
                state_.finished_mutex_.unlock();
                state_.finished_.notify_all();
            }
            else
            {
                state_.mutex_.unlock();
            }
        }
        else
        {
            std::unique_lock<std::mutex> lk(has_tasks_mutex_);
            has_tasks_.wait(lk);
            lk.unlock();
        }
    }
}
void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lk(state_.finished_mutex_);
    state_.mutex_.lock();
    state_.finished_tasks_ = 0;
    state_.left_tasks_ = num_total_tasks;
    state_.num_total_tasks_ = num_total_tasks;
    state_.runnable_ = runnable;
    state_.mutex_.unlock();
// for (int i = 0; i < num_total_tasks; i++)
     has_tasks_.notify_all();
    state_.finished_.wait(lk);
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
