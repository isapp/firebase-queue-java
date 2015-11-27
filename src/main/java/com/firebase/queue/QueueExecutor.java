package com.firebase.queue;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QueueExecutor extends ThreadPoolExecutor {
  /*package*/ interface TaskStateListener {
    void onTaskStart(Thread thread, QueueTask task);
    void onTaskFinished(QueueTask task, Throwable error);
  }

  private TaskStateListener taskStateListener;

  public QueueExecutor(int numWorkers) {
    this(numWorkers, numWorkers, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  public QueueExecutor(int corePoolSize, int maximumPoolSize) {
    this(corePoolSize, maximumPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  public QueueExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
    this(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<Runnable>());
  }

  public QueueExecutor(int corePoolSize, int maximumPoolSize, BlockingQueue<Runnable> workQueue) {
    this(corePoolSize, maximumPoolSize, 0L, TimeUnit.MILLISECONDS, workQueue);
  }

  public QueueExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, new ThreadFactory(), new RejectedExecutionHandler());
  }

  /*package*/ void setTaskStateListener(TaskStateListener taskStateListener) {
    this.taskStateListener = taskStateListener;
  }

  @Override
  public void setThreadFactory(java.util.concurrent.ThreadFactory threadFactory) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("The ThreadFactory of a QueueExecutor cannot be changed");
  }

  @Override
  public void setRejectedExecutionHandler(java.util.concurrent.RejectedExecutionHandler handler) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("The RejectedExecutionHandler of a QueueExecutor cannot be changed");
  }

  @Override
  protected final void beforeExecute(Thread t, Runnable r) {
    if(r instanceof QueueTask) {
      if(taskStateListener != null) taskStateListener.onTaskStart(t, (QueueTask) r);
    }
  }

  @Override
  protected final void afterExecute(Runnable r, Throwable t) {
    if(r instanceof QueueTask) {
      if(taskStateListener != null) taskStateListener.onTaskFinished((QueueTask) r, t);
    }
  }

  /*package*/ static final class ThreadFactory implements java.util.concurrent.ThreadFactory {
    @Override
    public Thread newThread(@NotNull Runnable r) {
      Thread t = new Thread(r);
      t.setName(UuidUtils.getUUID());
      return t;
    }
  }

  /*package*/ static final class RejectedExecutionHandler implements java.util.concurrent.RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      Log.debug("Task " + r.toString() + " rejected from " + executor.toString());
    }
  }

  public interface Factory {
    QueueExecutor get();
  }
}
