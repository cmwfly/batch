package com.mike.boot.web.task;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.task.TaskExecutor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批处理服务
 *
 * @param <T> the type parameter
 * @since 2020 /12/04
 */
@Slf4j
public abstract class AbstractBatchService<T> implements ApplicationRunner {
    private final int maxProcessCount;
    private final TaskExecutor taskExecutor;
    private final boolean autoStart;
    private final int initProcessCount;
    private AtomicInteger counter = new AtomicInteger();
    private boolean started;
    private boolean running = true;

    /**
     * Instantiates a new Abstract batch service.
     *
     * @param autoStart        是否自启动
     * @param initProcessCount 初始的处理线程数
     * @param maxProcessCount  最大的处理线程数
     * @param taskExecutor     线程池
     */
    public AbstractBatchService(boolean autoStart, int initProcessCount, int maxProcessCount, TaskExecutor taskExecutor) {
        this.autoStart = autoStart;
        this.initProcessCount = initProcessCount;
        if (maxProcessCount < 1)
            throw new IllegalArgumentException("maxProcessCount");
        if (initProcessCount < 1 || initProcessCount > maxProcessCount)
            throw new IllegalArgumentException("initProcessCount");
        this.maxProcessCount = maxProcessCount;
        this.taskExecutor = taskExecutor;
    }

    /**
     * 自启动
     *
     * @param args
     * @throws Exception
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (autoStart)
            start();
    }

    /**
     * 启动批处理（仅能调用一次）
     */
    public synchronized void start() {
        if (!started) {
            started = true;
            log.info("Start batch service");
            for (int i = 0; i < initProcessCount; i++)
                newAsyncProcess();
        }
    }

    /**
     * 激活批处理，如果当前未达到线程的上限，则会启动新的线程
     * Awake.
     */
    public synchronized void awake() {
        if (counter.get() < maxProcessCount) {
            log.info("Invoke awake by caller");
            newAsyncProcess();
        }
    }

    /**
     * 停止批处理
     */
    public void stop() {
        this.running = false;
    }

    private void newAsyncProcess() {
        taskExecutor.execute(this::doBatch);
    }

    /**
     * 获取下一个处理对象，如果没有则返回空
     *
     * @param batchContext the batch context
     * @return the t
     */
    protected abstract T nextItem(ThreadBatchContext batchContext);

    /**
     * 锁定对象并返回是否锁定成功
     *
     * @param batchContext the batch context
     * @param item         the item
     * @return the int
     */
    protected abstract boolean lockItem(ThreadBatchContext batchContext, T item);

    /**
     * 处理对象
     *
     * @param batchContext the batch context
     * @param item         the item
     */
    protected abstract void processItem(ThreadBatchContext batchContext, T item);

    /**
     * 准备开启新的批处理线程时触发
     *
     * @param batchContext the batch context
     */
    protected void startingBatchThread(ThreadBatchContext batchContext) {

    }

    /**
     * 准备结束批处理线程时触发。如果返回false，则取消结束，继续批处理。
     *
     * @param batchContext the batch context
     * @return the boolean
     */
    protected boolean endingBatchThread(ThreadBatchContext batchContext) {
        return true;
    }

    /**
     * Do batch.
     */
    protected void doBatch() {
        int ic = counter.incrementAndGet();
        log.info("Start a new process. current process is {}", ic);
        ThreadBatchContext batchContext = new ThreadBatchContext();
        startingBatchThread(batchContext);
        do {
            try {
                T item = nextItem(batchContext);
                if (item == null) {
                    if (!endingBatchThread(batchContext)) {
                        log.info("Cancel ending process.");
                        continue;
                    }
                    int ec = counter.getAndDecrement();
                    log.info("Finish a new process. current process is {}", ec);
                    return;
                }
                boolean locked = lockItem(batchContext, item);
                if (!locked)
                    continue;

                processItem(batchContext, item);
            } catch (Exception ex) {
                log.warn("Some error for async task.", ex);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("Sleep thread error.", e);
                }
            }
        }
        while (running);
    }
}
