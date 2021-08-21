#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>

#include "threadpool.h"

#define DEFAULT_TIME 10
#define MIN_WAIT_TASK_NUM 10
#define DEFAULT_THREAD_VARY 10  //the number ofcreate or destriy threads everytime

#define true 1
#define false 0


typedef struct {
    void *(*function)(void *arg);
    void *arg;
}threadpool_task_t;

struct threadpool_t {
    pthread_mutex_t lock;
    pthread_mutex_t thread_counter;
    pthread_cond_t queue_not_full;
    pthread_cond_t queue_not_empty;

    pthread_t *threads;
    pthread_t adjust_tid;
    threadpool_task_t *task_queue;

    int min_thr_num;
    int max_thr_num;
    int live_thr_num;
    int busy_thr_num;
    int wait_exit_thr_num;

    int queue_front;
    int queue_rear;
    int queue_size;
    int queue_max_size;

    int shutdown;
};


//worker threads
void *threadpool_thread(void *threadpool);

//manager threads
void *adjust_thead(void *threadpool);

int is_thread_alive(pthread_t tid);
int threadpool_free(threadpool_t *pool);

threadpool_t *threadpool_create(int min_thr_num, int max_thr_num, int queue_max_size)
{
    int i;
    threadpool_t *pool = NULL;
    do {
        if ((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {
            printf("mallco threadpool fail\n");
            break;  //jump out do while
        }

        pool->min_thr_num = min_thr_num;
        pool->max_thr_num = max_thr_num;
        pool->live_thr_num = min_thr_num;
        pool->busy_thr_num = 0;

        pool->queue_front = 0;
        pool->queue_rear = 0;
        pool->queue_max_size = queue_max_size;
        pool->queue_size = 0;

        pool->shutdown = false;

        pool->task_queue = (threadpool_task_t *)malloc(sizeof(threadpool_task_t)*queue_max_size);
        if (pool->task_queue == NULL) {
            printf("malloc task_queue fail\n");
            break;
        }

        if (pthread_mutex_init(&(pool->lock), NULL) != 0
            || pthread_mutex_init(&(pool->lock), NULL) != 0
            || pthread_cond_init(&(pool->queue_not_empty), NULL) != 0
            || pthread_cond_init(&(pool->queue_not_full), NULL) != 0) {
                printf("init the lock or cond fail\n");
                break;
            }

        for (i = 0; i < min_thr_num; i++) {
            pthread_create(&(pool->threads[i]), NULL, threadpool_thread, (void *)pool);
            printf("start thread 0x%x...\n", (unsigned int)pool->threads[i]);
        }

        pthread_create(&(pool->adjust_tid), NULL, adjust_thead, (void *)pool);  //manager thread

        return pool;

    } while (0);

    threadpool_free(pool);  //fail process

    return NULL;
}


int threadpool_add(threadpool_t *pool, void*(*function)(void *arg), void *arg)
{
    pthread_mutex_lock(&pool->lock);

    while ((pool->queue_size == pool->queue_max_size) && (!pool->shutdown)) {
        pthread_cond_wait(&(pool->queue_not_full), &(pool->lock));
    }
    //if hteradpool is shutdown, unlock
    if (pool->shutdown) {
        pthread_mutex_unlock(&(pool->lock));
    }

    if (pool->task_queue[pool->queue_rear].arg != NULL) {
        free(pool->task_queue[pool->queue_rear].arg);
        pool->task_queue[pool->queue_rear].arg = NULL;
    }

    //add a task into queue
    pool->task_queue[pool->queue_rear].function = function;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->queue_rear = (pool->queue_rear + 1) % pool->max_thr_num;
    pool->queue_size++;

    //queue not empty after add a task, so wake up threads
    pthread_cond_signal(&pool->queue_not_empty);

    //unlock
    pthread_mutex_unlock(&pool->lock);

    return 0;
}


//work threads
void *threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;
    threadpool_task_t task;

    while (true) {
        //
        pthread_mutex_lock(&pool->lock);

        while ((pool->queue_size == 0) && (!pool->shutdown)) {
            printf("therad 0x%x is waiting\n", (unsigned int)pthread_self());
            pthread_cond_wait(&pool->queue_not_empty, &pool->lock);

            if (pool->wait_exit_thr_num > 0) {
                pool->wait_exit_thr_num--;

                if (pool->live_thr_num > pool->min_thr_num) {
                    printf("thread 0x%x is exiting\n", (unsigned int)pthread_self());
                    pool->live_thr_num--;
                    pthread_mutex_unlock(&pool->lock);
                    pthread_exit(NULL);
                }
            }
        }

        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->lock);
            printf("thread 0x%x is exiting\n", (unsigned int)pthread_self());
            pthread_exit(NULL);
        }

        task.function = pool->task_queue[pool->queue_front].function;
        task.arg = pool->task_queue[pool->queue_front].arg;

        pool->queue_front = (pool->queue_front + 1) % pool->queue_max_size; //like circular queue
        pool->queue_size--;

        //notify can add new task, the queue is not full
        pthread_cond_broadcast(&pool->queue_not_full);

        pthread_mutex_unlock(&pool->lock);

        //execute task
        printf("theread 0x%x start working\n", (unsigned int)pthread_self());
        pthread_mutex_lock(&pool->thread_counter);
        pool->busy_thr_num++;
        pthread_mutex_unlock(&pool->thread_counter);
        (*(task.function))(task.arg);
        //task.function(task.arg);

        //task finish
        printf("thread 0x%x end working\n", (unsigned int)pthread_self());
        pthread_mutex_lock(&pool->lock);
        pool->busy_thr_num--;
        pthread_mutex_unlock(&pool->lock);
    }
    pthread_exit(NULL);
}

//manager threads
void *adjust_thead(void *threadpool)
{
    int i;
    threadpool_t *pool = (threadpool_t *)threadpool;

    while (!pool->shutdown) {
        sleep(DEFAULT_TIME);

        pthread_mutex_lock(&(pool->lock));
        int queue_size = pool->queue_size;
        int live_thr_num = pool->live_thr_num;
        pthread_mutex_unlock(&pool->lock);

        pthread_mutex_lock(&pool->thread_counter);
        int busy_thr_num = pool->busy_thr_num;
        pthread_mutex_unlock(&pool->thread_counter);

        if (queue_size >= MIN_WAIT_TASK_NUM && live_thr_num < pool->max_thr_num) {
            pthread_mutex_lock(&pool->lock);
            int add = 0;

            for (i = 0; i < pool->max_thr_num && add < DEFAULT_THREAD_VARY 
                && pool->live_thr_num < pool->max_thr_num; i++) {
                    if (pool->threads[i] == 0 || !is_thread_alive(pool->threads[i])) {
                        pthread_create(&pool->threads[i], NULL, threadpool_thread, (void *)pool);
                        add++;
                        pool->live_thr_num++;
                    }
            }
            pthread_mutex_unlock(&pool->lock);
        }

        //destory some threads
        if ((busy_thr_num*2) < live_thr_num && live_thr_num > pool->min_thr_num) {
            pthread_mutex_lock(&pool->lock);
            pool->wait_exit_thr_num = DEFAULT_THREAD_VARY;  //destroy number is 10
            pthread_mutex_unlock(&pool->lock);

            for (i = 0; i < DEFAULT_THREAD_VARY; i++) {
                pthread_cond_signal(&pool->queue_not_empty);
            }
        }
    }
    return NULL;
}


int threadpool_destroy(threadpool_t *pool)
{
    int i;
    if (pool == NULL) {
        return -1;
    }
    pool->shutdown = true;

    //destory manager thread fisrt
    pthread_join(pool->adjust_tid, NULL);

    for (i = 0; i < pool->live_thr_num; i++) {
        pthread_cond_broadcast(&pool->queue_not_empty);
    }
    for (i = 0; i< pool->live_thr_num; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    threadpool_free(pool);

    return 0;
}

int threadpool_free(threadpool_t *pool)
{
    if (pool == NULL) {
        return -1;
    }
    if (pool->task_queue) {
        free(pool->task_queue);
    }
    if (pool->threads) {
        free(pool->threads);
        pthread_mutex_lock(&pool->lock);
        pthread_mutex_destroy(&pool->lock);
        pthread_mutex_lock(&pool->thread_counter);
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
    all_threadnum = pool->live_thr_num;
    pthread_mutex_unlock(&pool->lock);
    
    return all_threadnum;
}


int threadpool_busy_threadnum(threadpool_t *pool)
{
    int busy_threadnum = -1;
    pthread_mutex_lock(&pool->lock);
    busy_threadnum = pool->busy_thr_num;
    pthread_mutex_unlock(&pool->lock);

    return busy_threadnum;
}

int is_thread_alive(pthread_t tid) 
{
    int kill_rc = pthread_kill(tid, 0);
    if (kill_rc == ESRCH) {
        return false;
    }

    return true;
}


#if 1
void *process(void *arg) 
{
    printf("trhread 0x%x working on task %d\n", (unsigned int)pthread_self(), *(int *)arg);
    sleep(1);
    printf("task %d is end\n", *(int *)arg);

    return NULL;
}

int main()
{
    threadpool_t *thp = threadpool_create(3, 100, 100);
    printf("pool inited");

    int num[20], i;
    for (i = 0; i < 20; i++) {
        num[i] = i;
        printf("add task %d\n", i);
        threadpool_add(thp, process, (void *)&num[i]);
    }
    sleep(10);
    threadpool_destroy(thp);



    return 0;
}

#endif