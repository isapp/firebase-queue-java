package com.firebase.queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
    UuidUtils.class
)
public class QueueExecutorTest {
  private QueueExecutor subject;

  @Before
  public void setUp() throws Exception {
    subject = new QueueExecutor(1);
  }

  @Test
  public void constructorsDelegateToThreadPoolExecutor() {
    // using constructor in setUp
    assertThat(subject.getCorePoolSize()).isEqualTo(1);
    assertThat(subject.getMaximumPoolSize()).isEqualTo(1);
    assertThat(subject.getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(0L);
    assertThat(subject.getQueue()).isInstanceOfAny(LinkedBlockingQueue.class);
    assertThat(subject.getThreadFactory()).isInstanceOfAny(QueueExecutor.ThreadFactory.class);
    assertThat(subject.getRejectedExecutionHandler()).isInstanceOfAny(QueueExecutor.RejectedExecutionHandler.class);
    subject.shutdownNow();

    subject = new QueueExecutor(2, 3);
    assertThat(subject.getCorePoolSize()).isEqualTo(2);
    assertThat(subject.getMaximumPoolSize()).isEqualTo(3);
    assertThat(subject.getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(0L);
    assertThat(subject.getQueue()).isInstanceOfAny(LinkedBlockingQueue.class);
    assertThat(subject.getThreadFactory()).isInstanceOfAny(QueueExecutor.ThreadFactory.class);
    assertThat(subject.getRejectedExecutionHandler()).isInstanceOfAny(QueueExecutor.RejectedExecutionHandler.class);
    subject.shutdownNow();

    subject = new QueueExecutor(2, 3, 1, TimeUnit.MILLISECONDS);
    assertThat(subject.getCorePoolSize()).isEqualTo(2);
    assertThat(subject.getMaximumPoolSize()).isEqualTo(3);
    assertThat(subject.getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(1L);
    assertThat(subject.getQueue()).isInstanceOfAny(LinkedBlockingQueue.class);
    assertThat(subject.getThreadFactory()).isInstanceOfAny(QueueExecutor.ThreadFactory.class);
    assertThat(subject.getRejectedExecutionHandler()).isInstanceOfAny(QueueExecutor.RejectedExecutionHandler.class);
    subject.shutdownNow();

    subject = new QueueExecutor(2, 3, new ArrayBlockingQueue<Runnable>(1));
    assertThat(subject.getCorePoolSize()).isEqualTo(2);
    assertThat(subject.getMaximumPoolSize()).isEqualTo(3);
    assertThat(subject.getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(0L);
    assertThat(subject.getQueue()).isInstanceOfAny(ArrayBlockingQueue.class);
    assertThat(subject.getThreadFactory()).isInstanceOfAny(QueueExecutor.ThreadFactory.class);
    assertThat(subject.getRejectedExecutionHandler()).isInstanceOfAny(QueueExecutor.RejectedExecutionHandler.class);
    subject.shutdownNow();

    subject = new QueueExecutor(2, 3, 1, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1));
    assertThat(subject.getCorePoolSize()).isEqualTo(2);
    assertThat(subject.getMaximumPoolSize()).isEqualTo(3);
    assertThat(subject.getKeepAliveTime(TimeUnit.MILLISECONDS)).isEqualTo(1L);
    assertThat(subject.getQueue()).isInstanceOfAny(ArrayBlockingQueue.class);
    assertThat(subject.getThreadFactory()).isInstanceOfAny(QueueExecutor.ThreadFactory.class);
    assertThat(subject.getRejectedExecutionHandler()).isInstanceOfAny(QueueExecutor.RejectedExecutionHandler.class);
    subject.shutdownNow();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void callingSetThreadFactory_throwsAnException() {
    subject.setThreadFactory(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void callingSetRejectedExecutionHandler_throwsAnException() {
    subject.setRejectedExecutionHandler(null);
  }

  @Test
  public void beforeExecute_ifThereIsATaskStateListener_andTheRunnableIsAQueueTask_callsOnTaskStart() {
    QueueExecutor.TaskStateListener listener = mock(QueueExecutor.TaskStateListener.class);
    subject.setTaskStateListener(listener);
    QueueTask task = mock(QueueTask.class);
    subject.beforeExecute(Thread.currentThread(), task);
    verify(listener).onTaskStart(Thread.currentThread(), task);
  }

  @Test
  public void beforeExecute_ifThereIsATaskStateListener_butTheRunnableIsNotAQueueTask_doesNotCallOnTaskStart() {
    QueueExecutor.TaskStateListener listener = mock(QueueExecutor.TaskStateListener.class);
    subject.setTaskStateListener(listener);
    subject.beforeExecute(Thread.currentThread(), mock(Runnable.class));
    verify(listener, never()).onTaskStart(eq(Thread.currentThread()), any(QueueTask.class));
  }

  @Test
  public void beforeExecute_ifThereIsNoTaskStateListener_nothingHappens() {
    subject.beforeExecute(Thread.currentThread(), mock(Runnable.class));
  }

  @Test
  public void afterExecute_ifThereIsATaskStateListener_andTheRunnableIsAQueueTask_callsOnTaskFinished() {
    QueueExecutor.TaskStateListener listener = mock(QueueExecutor.TaskStateListener.class);
    subject.setTaskStateListener(listener);
    QueueTask task = mock(QueueTask.class);
    subject.afterExecute(task, null);
    verify(listener).onTaskFinished(task, null);
  }

  @Test
  public void afterExecute_ifThereIsATaskStateListener_butTheRunnableIsNotAQueueTask_doesNotCallOnTaskFinished() {
    QueueExecutor.TaskStateListener listener = mock(QueueExecutor.TaskStateListener.class);
    subject.setTaskStateListener(listener);
    subject.afterExecute(mock(Runnable.class), null);
    verify(listener, never()).onTaskFinished(any(QueueTask.class), isNull(Throwable.class));
  }

  @Test
  public void afterExecute_ifThereIsNoTaskStateListener_nothingHappens() {
    subject.afterExecute(mock(Runnable.class), null);
  }

  @Test
  public void threadsCreatedByAQueueExecutor_haveTheirNameSetToARandomUUID() {
    mockStatic(UuidUtils.class);
    when(UuidUtils.getUUID()).thenReturn("uuid");
    Thread t = subject.getThreadFactory().newThread(new Runnable() {
      @Override
      public void run() {

      }
    });
    assertThat(t.getName()).isEqualTo("uuid");
  }

  @Test
  public void causingATaskToGetRejected_getsLogged() {
    Log.Logger logger = mock(Log.Logger.class);
    stub(logger.shouldLogDebug(Log.Level.INFO)).toReturn(true);
    Log.setLogger(logger);
    subject.shutdownNow();
    subject.execute(new Runnable() {
      @Override
      public void run() {}

      @Override
      public String toString() {
        return "bloop";
      }
    });
    verify(logger).debug("Task bloop rejected from " + subject.toString(), Log.Level.INFO);
  }

  @Test
  public void causingATaskToGetRejected_doesNotGetLogged_ifDebugLoggingIsNotOnForTheInfoLevel() {
    Log.Logger logger = mock(Log.Logger.class);
    stub(logger.shouldLogDebug(Log.Level.INFO)).toReturn(false);
    Log.setLogger(logger);
    subject.shutdownNow();
    subject.execute(new Runnable() {
      @Override
      public void run() {}

      @Override
      public String toString() {
        return "bloop";
      }
    });
    verify(logger, never()).debug("Task bloop rejected from " + subject.toString(), Log.Level.INFO);
  }

  @Test
  public void causingATaskToGetRejected_doesNotGetLogged_ifDebugLoggingIsNotOn() {
    Log.DEBUG = false;
    Log.Logger logger = mock(Log.Logger.class);
    Log.setLogger(logger);
    subject.shutdownNow();
    subject.execute(new Runnable() {
      @Override
      public void run() {

      }

      @Override
      public String toString() {
        return "bloop";
      }
    });
    verify(logger, never()).debug("Task bloop rejected from " + subject.toString(), Log.Level.INFO);
  }

  @After
  public void tearDown() throws Exception {
    subject.shutdownNow();
  }
}
