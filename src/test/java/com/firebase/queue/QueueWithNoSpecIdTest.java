package com.firebase.queue;

import com.firebase.client.ChildEventListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(JUnit4.class)
public class QueueWithNoSpecIdTest extends QueueTest {
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void buildingAQueue_doesNotUseTheSpecChild() {
    instantiateQueue();

    verify(firebaseMock.getRoot(), never()).child(Queue.SPEC_CHILD);
  }

  @Test
  public void buildingAQueue_behavesLikeANewSpecCameIn() {
    instantiateQueue();

    InOrder inOrder = inOrder(queueHelper.taskReset, queueHelper.queueExecutorFactory, queueHelper.timeoutExecutorFactory, queueHelper.queueExecutor, firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    verify(queueHelper.taskReset).onNewTaskSpec(eq(new TaskSpec()));

    inOrder.verify(queueHelper.queueExecutorFactory).get();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(notNull(QueueExecutor.TaskStateListener.class));
    inOrder.verify(queueHelper.timeoutExecutorFactory).get();

    inOrder.verify(firebaseMock.getTasksQuery()).addChildEventListener(firebaseMock.getTasksChildEventListener());
    inOrder.verify(firebaseMock.getTimeoutQuery()).addChildEventListener(firebaseMock.getTimeoutChildEventListener());
  }

  @Test
  public void buildingAQueue_doesNotShutDownTheExecutors() {
    instantiateQueue();

    verify(queueHelper.queueExecutor, never()).shutdownNow();
    verify(queueHelper.timeoutExecutor, never()).shutdownNow();
  }

  @Test
  public void buildingAQueue_doesNotRemoveChildListenersFromTheFirebaseQueries() {
    instantiateQueue();

    verify(firebaseMock.getTasksQuery(), never()).removeEventListener(firebaseMock.getTasksChildEventListener());
    verify(firebaseMock.getTimeoutQuery(), never()).removeEventListener(firebaseMock.getTimeoutChildEventListener());
  }

  @Test
  public void whenTheQueueIsShutdown_theSpecEventListenerIsNotRemoved() {
    Queue queue = instantiateQueue();

    queue.shutdown();

    verifyZeroInteractions(firebaseMock.getSpecIdRef());
  }

  @Test
  public void whenTheQueueIsShutdown_theTaskQueriesAreRemoved_andThenTheExecutorsAreShutDown() {
    Queue queue = instantiateQueue();

    // grab a reference to these here because they will be nulled out on shutdown
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    InOrder inOrder = inOrder(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery(), queueHelper.queueExecutor, queueHelper.timeoutExecutor);

    queue.shutdown();

    inOrder.verify(firebaseMock.getTasksQuery()).removeEventListener(taskQueryListener);
    inOrder.verify(firebaseMock.getTimeoutQuery()).removeEventListener(timeoutQueryListener);
    inOrder.verify(queueHelper.queueExecutor).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor).shutdownNow();
  }

  @Test
  public void whenTheQueueIsShutdownTwice_theTaskQueriesAreRemoved_andThenTheExecutorsAreShutDown_onlyOnce() {
    Queue queue = instantiateQueue();

    // grab a reference to these here because they will be nulled out on shutdown
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    InOrder inOrder = inOrder(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery(), queueHelper.queueExecutor, queueHelper.timeoutExecutor);

    queue.shutdown();
    queue.shutdown();

    inOrder.verify(firebaseMock.getTasksQuery(), times(1)).removeEventListener(taskQueryListener);
    inOrder.verify(firebaseMock.getTimeoutQuery(), times(1)).removeEventListener(timeoutQueryListener);
    inOrder.verify(queueHelper.queueExecutor, times(1)).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor, times(1)).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor, times(1)).shutdownNow();
  }

  @Override
  protected Queue instantiateQueue() {
    return queueHelper.getNewBuilder().build();
  }
}
