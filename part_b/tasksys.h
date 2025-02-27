#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <iostream>
#include <map>
#include <vector>
#include <thread>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * WaitingTask
 */
struct WaitingTask {
    TaskID _id; // the TaskID of this task
    TaskID _depend_TaskID; // the maximum TaskID of this task's dependencies
    IRunnable* runnable_; // the bulk task
    int num_total_tasks_;
    WaitingTask(TaskID id, TaskID dependency, IRunnable* runnable, int num_total_tasks):_id{id}, _depend_TaskID{dependency}, runnable_{runnable}, num_total_tasks_{num_total_tasks}{};
    bool operator<(const WaitingTask& other) const {
        return _depend_TaskID > other._depend_TaskID;
    }
};

/*
 * Ready Task
 */
struct ReadyTask {
    TaskID _id;
    IRunnable* runnable_;
    int current_task_;
    int num_total_tasks_;
    ReadyTask(TaskID id, IRunnable* runnable, int num_total_tasks):_id(id), runnable_(runnable), current_task_{0}, num_total_tasks_(num_total_tasks) {}
    ReadyTask(){}
};


/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    private:
        // the maximum number of workerThread this TaskSystem can use
        int num_threads_;

        // record the process of tasks which are in executing
        std::map<TaskID, std::pair<int, int> > task_process_;
        std::mutex  meta_data_mutex_; // protect task_process_ and finished_TaskID_

        // task in the waiting queue with _depend_TaskID <= finished_TaskID_ can be pushed into ready queue
        TaskID finished_TaskID_;         
        // wait/notify syn() thread
        std::condition_variable finished_;



        // the next call to run or runAsyncWithDeps with get this TaskID back
        TaskID next_TaskID_;         

        // notify workerThreads to kill themselves
        bool killed_;         
        
        // worker thread pool
        std::vector<std::thread> threads_pool_;

        // waiting task queue
        std::priority_queue<WaitingTask, std::vector<WaitingTask> > _waiting_queue;
        std::mutex _waiting_queue_mutex;

        // ready task queue
        std::queue<ReadyTask> ready_queue_;
        std::mutex ready_queue_mutex_;


    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void workerThread();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif