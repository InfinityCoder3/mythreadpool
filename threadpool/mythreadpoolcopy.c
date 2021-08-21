#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "threadpool.h"

#define DEFAULT_TIME 5                 /*10s检测一次*/
#define MIN_WAIT_TASK_NUM 10            /*如果queue_size > MIN_WAIT_TASK_NUM 添加新的线程到线程池*/ 
#define DEFAULT_THREAD_VARY 10          /*每次创建和销毁线程的个数*/
#define true 1
#define false 0

typedef struct {
    void *(*function)(void *);
    void *arg;
} threadpool_task_t;

struct threadpool_t {
    pthread_mutex_t lock;
    pthread_mutex_t thread_counter;
    pthread_cond_t queue_not_full;
    pthread_cond_t queue_not_empty;

    pthread_t *threads;
    pthread_t manager_tid;
    threadpool_task_t *task_queue;

    int min_thr;
    int max_thr;
    int live_thr;
    int busy_thr;
    int wait_exit_thr;

    int queue_front;
    int queue_rear;
    int queue_size;
    int queue_max_size;

    int shutdown;
};

int is_thread_alive(pthread_t tid);
int threadpool_free(threadpool_t *pool);


void *threadpool_worker(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;
    threadpool_task_t task;

    while (true) {

        pthread_mutex_lock(&pool->lock);

        while ((pool->queue_size == 0) && (!pool->shutdown)) {
            printf("thread 0x%x is waiting\n", (unsigned int)pthread_self());
            pthread_cond_wait(&pool->queue_not_empty, &pool->lock);

            //delete threads 
            if (pool->wait_exit_thr > 0) {
                pool->wait_exit_thr--;

                //make sure live_thr == min_thr
                if (pool->live_thr > pool->min_thr) {
                    printf("thread 0x%x is exiting by live--\n", (unsigned int)pthread_self());
                    pool->live_thr--;
                    pthread_mutex_unlock(&pool->lock);
                    pthread_exit(NULL);
                }
            }
        }
    
        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->lock);
            printf("thread 0x%x is exiting in shutdown\n", (unsigned int)pthread_self());
            pthread_exit(NULL); 
        }
        
        /* pool->task_queue != NULL, if program execute here.
         * cause if pool->queue_size == 0, program would block in pthread_con_wait()
        */
        task.function = pool->task_queue[pool->queue_front].function;
        task.arg = pool->task_queue[pool->queue_front].arg;

        pool->queue_front = (pool->queue_front + 1) % pool->queue_max_size;
        pool->queue_size--;

        pthread_cond_broadcast(&pool->queue_not_full);

        pthread_mutex_unlock(&pool->lock);

        //execute task
        printf("thread 0x%x start working\n", (unsigned int)pthread_self());
        pthread_mutex_lock(&pool->thread_counter);
        pool->busy_thr++;
        pthread_mutex_unlock(&pool->thread_counter);
        //((*task.function))(task.arg);
        task.function(task.arg);

        //end task
        printf("thread 0x%x end working\n", (unsigned int)pthread_self());
        pthread_mutex_lock(&pool->thread_counter);
        pool->busy_thr--;
        pthread_mutex_unlock(&pool->thread_counter);
    
    }

    pthread_exit(NULL);

}

void *manager_thread(void *threadpool)
{
    int i;
    threadpool_t *pool = (threadpool_t *)threadpool;

    while (!pool->shutdown) {
        sleep(DEFAULT_TIME);    //timing to manager threadpool do somethinf per 10s 

        pthread_mutex_lock(&pool->lock);
        int queue_size = pool->size;
        int live_thr = pool->live_thr;
        pthread_mutex_unlock(&pool->lock);

        pthread_mutex_lock(&pool->thread_counter);
        int busy_thr = pool->busy_thr;
        pthread_mutex_unlock(&pool->thread_counter);

        //add threads
        if (queue_size >= MIN_WAIT_TASK_NUM && live_thr < pool->queue_max_size) {
            pthread_mutex_lock(&pool->lock);
            int add = 0;

            for (i = 0; i < pool->max_thr && add < DEFAULT_THREAD_VARY
                && pool->live_thr < pool->max_thr; i++) {
                    if (pool->threads[i] == 0 || !is_thread_alive(pool->threads[i])) {  //memset pool->threads[i] = 0
                        pthread_create(&pool->threads[i], NULL, threadpool_worker, (void *)pool);
                        printf("------------manager add a new threads------------\n");
                        add++;
                        pool->live_thr++;
                    }
            }

            pthread_mutex_unlock(&pool->lock);
        }

        //delete threads
        if ((busy_thr * 2) < live_thr && live_thr > pool->min_thr) {
            pthread_mutex_lock(&pool->lock);
            pool->wait_exit_thr = DEFAULT_THREAD_VARY;
            pthread_mutex_unlock(&pool->lock);

            for (i = 0; i < DEFAULT_THREAD_VARY; i++) {
                pthread_cond_signal(&pool->queue_not_empty);
                printf("---------------manager delete a thread-------------\n");
            }
        }
    }
    return NULL;
}


threadpool_t *threadpool_create(int min_thr, int max_thr, int queue_max_size)
{
    int i;
    threadpool_t *pool = NULL;
    do {
        if ((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {
            printf("malloc threadpool fail\n");
            break;
        }

        //initialize pool member
        pool->min_thr = min_thr;
        pool->max_thr = max_thr;
        pool->live_thr = min_thr;
        pool->busy_thr = 0;

        pool->queue_front = 0;
        pool->queue_rear = 0;
        pool->size = 0;
        pool->queue_max_size = queue_max_size;
        
        pool->shutdown = fasle;

        //process threads
        pool->threads = (thread_t *)malloc(sizeof(pthread_t)*max_thr);
        if (pool->threads == NULL) {
            printf("malloc threads fail\n");
            break;
        }
        memset(pool->threads, 0, sizeof(pthread_t)*max_thr);

        //process task queue
        pool->task_queue = (threadpool_task_t *)malloc(sizeof(threadpool_task_t)*queue_max_size);
        if (pool->task_queue == NULL) {
            printf("malloc task queue fail\n");
            break;
        }

        //mutex and cond
        if (pthread_mutex_init(&(pool->lock), NULL) != 0
            || pthread_mutex_init(&(pool->thread_counter), NULL) != 0
            || pthread_cond_init(&(pool->queue_not_empty), NULL) != 0
            || pthread_cond_init(&(pool->queue_not_full), NULL) != 0)
        {
            printf("init the lock or cond fail");
            break;
        }

        //launch min the threads
        for (i = 0; i < min_thr; i++) {
            pthread_create(&pool->threads[i], NULL, threadpool_worker, (void *)pool);
            printf("start thread 0x%x...\n", (unsigned int)pool->threads[i]);
        }
        pthread_create(&pool->manager_tid, NULL, manager_thread, (void *)pool);
        printf("---start manager thread 0x%x...\n", (unsigned int)pool->manager_tid);

        return pool;

    } while (0);
    
    threadpool_free(pool);

    return NULL;
}


int threadpool_add(threadpool_t *pool, void*(*function)(void *), void *arg)
{
    pthread_mutex_lock(&pool->lock);
    
    while ((pool->queue_size == pool->queue_max_size) && (!pool->shutdown)) {
        pthread_cond_wait(&pool->queue_not_full, pool->lock);
    }

    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->lock);
    }

    if (pool->task_queue[pool->queue_rear].arg != NULL) {
        free(pool->task_queue[pool->queue_rear].arg);
        pool->task_queue[pool->queue_rear].arg = NULL;
    }

    pool->task_queue[pool->queue_rear].function = function;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->queue_rear = (pool->queue_rear + 1) % pool->queue_max_size;
    pool->queue_size++;

    pthread_con_signal(&pool->queue_not_empty);
    pthread_mutex_unlock(&pool->lock);

    return 0;
}


int threadpool_destory(threadpool_t *pool)
{
    int i;
    if (pool == NULL)
        return -1;

    pool->shutdown;

    //destroy manager thread first
    pthread_join(pool->manager_tid, NULL);
    printf("---manager thread exit...");

    for (i = 0; i < pool->live_thr; i++) {
        //notify every free thread, then they would not wait all the time
        pthread_cond_broadcast(&pool->queue_not_empty);
    }
    //destroy every worker thread
    for (i = 0; i < pool->live_thr; i++) {
        pthread_join[pool->threads[i], NULL];
    }

    threadpool_free(pool);

    return 0;
}

int threadpool_free(threadpool_t *pool)
{
    //release resources that created in heap
    if (pool == NULL)
        return -1;

    if (pool->task_queue)
        free(pool->task_queue);

    if (pool->threads) {
        free(pool->threads);
        pthread_mutex_lock(&pool->lock);
        pthread_mutex_destroy(&pool->lock);
        pthread_mutex_lock(&pool->thread_coounter);
        pthread_mutex_destroy(&pool->thread_counter);
        pthread_cond_destroy(&pool->queue_not_full);
        pthread_cond_destroy(&pool->queue_not_empty);
    }
    
    free(pool);
    pool = NULL;

    return 0;
}

int threadpool_all_threadnum(threadpool_t *pool)
{
    int all_threadnum = -1;
    
    pthread_mutex_lock(&pool->lock);
    all_threadnum = pool->live_thr;
    pthread_mutex_unlock(&pool->lock);

    return all_threadnum;
}

int threadpool_busy_threadnum(threadpool_t *pool)
{
    int busy_threadnum = -1;

    pthread_mutex_lock(&pool->lock);
    busy_threadnum = pool->busy_thr;
    pthread_mutex_unlock(&pool->lock);

    return busy_threadnum;
}

int is_thread_alive(pthread_t tid) 
{
    int kill_rc = thread_kill(tid, 0);  //send signale 0, to checkout the thread ia alive or not
    if (kiil_rc == ESRCH) {
        return false;   //no such process
    }
    return true;
}

void *process(void *arg)
{
    printf("thread 0x%x working on task %d\n", (unsigned int)pthread_self(), *(int *)arg);
    sleep(1);   //do something
    printf("task %d is end...\n", *(int *)arg);

    return NULL;
}

int main()
{
    //create threadpool
    threadpool_t *thp = threadpool_create(2, 100, 100);

    int n = 90, i;
    int nums[n];

    for (i = 0; i < n; i++) {
        nums[i] = i;
        printf("--main add task %d--\n", i+1);
        threadpool_add(thp, process, (void *)&nums[i]);
    }
    sleep(22);  //main thread do something
    threadpool_destroy(thp);

    return 0;
}