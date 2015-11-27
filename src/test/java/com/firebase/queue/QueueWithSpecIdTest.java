package com.firebase.queue;

import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;

import static com.firebase.queue.TestUtils.getInvalidTaskSpecSnapshot;
import static com.firebase.queue.TestUtils.getValidTaskSpecSnapshot;
import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class QueueWithSpecIdTest extends QueueTest {
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void buildingAQueue_getsThatSpecChild_andAddsAValueEventListenerToIt() {
    instantiateQueueWithoutTaskSpec();

    InOrder inOrder = inOrder(firebaseMock.getSpec(), firebaseMock.getSpecIdRef());

    inOrder.verify(firebaseMock.getSpec()).child("some_spec");

    inOrder.verify(firebaseMock.getSpecIdRef()).addValueEventListener(firebaseMock.getSpecIdValueEventListener());
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_doesNotUseTheDefaultTaskSpecForTheTaskReset() {
    instantiateQueueWithoutTaskSpec();

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_doesNotInteractWithTheExecutors() {
    instantiateQueueWithoutTaskSpec();

    verifyZeroInteractions(queueHelper.queueExecutorFactory, queueHelper.queueExecutor,
        queueHelper.timeoutExecutorFactory, queueHelper.timeoutExecutor);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_doesNotAddChildListenersToTheFirebaseQueries() {
    instantiateQueueWithoutTaskSpec();

    verifyZeroInteractions(firebaseMock.getTasksQuery());
    verifyZeroInteractions(firebaseMock.getTimeoutQuery());
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingValidSpecComesIn_doesNotWriteAnInfoLog() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    TaskSpec taskSpecToUse = new TaskSpec(specSnapshot);

    firebaseMock.restubQueries(taskSpecToUse);
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    reset(logger);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingValidSpecComesIn_doesNotUseThatTaskSpec() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingValidSpecComesIn_doesNotGetNewExecutors() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(queueHelper.queueExecutorFactory);
    verifyZeroInteractions(queueHelper.timeoutExecutorFactory);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingValidSpecComesIn_doesNotSetATaskListenerOnTheQueueExecutor() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    reset(queueHelper.queueExecutor);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(queueHelper.queueExecutor);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingValidSpecComesIn_doesNotAddChildListenersToTheFirebaseQueries() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    reset(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(firebaseMock.getTasksQuery());
    verifyZeroInteractions(firebaseMock.getTimeoutQuery());
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingInvalidSpecComesIn_doesNotWriteAWarningLog() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getInvalidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingInvalidSpecComesIn_doesNotRemoveChildListenersFromTheFirebaseQueries() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getInvalidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    reset(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(firebaseMock.getTasksQuery());
    verifyZeroInteractions(firebaseMock.getTimeoutQuery());
  }

  @Test
  public void buildingAQueue_beforeAMatchingSpecComesIn_andTheQueueIsShutdown_andThenAMatchingInvalidSpecComesIn_doesNotShutDownTheExecutors() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    DataSnapshot specSnapshot = getInvalidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    simulateQueueShutdown(queue);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    verifyZeroInteractions(queueHelper.queueExecutorFactory);
    verifyZeroInteractions(queueHelper.timeoutExecutorFactory);
  }

  @Test
  public void buildingAQueue_andAValidMatchingSpecComesIn_writesAnInfoLog() {
    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();

    instantiateQueue(specSnapshot, true);

    verify(logger).log("Got a new spec - " + new TaskSpec(specSnapshot), Log.Level.INFO);
  }

  @Test
  public void buildingAQueue_andAValidMatchingSpecComesIn_behavesLikeANewSpecCameIn() {
    instantiateQueue(getValidTaskSpecSnapshot(), true);

    InOrder inOrder = inOrder(queueHelper.taskReset, queueHelper.queueExecutorFactory, queueHelper.timeoutExecutorFactory, queueHelper.queueExecutor, firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    verify(queueHelper.taskReset).onNewTaskSpec(eq(queueHelper.taskSpec));

    inOrder.verify(queueHelper.queueExecutorFactory).get();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(notNull(QueueExecutor.TaskStateListener.class));
    inOrder.verify(queueHelper.timeoutExecutorFactory).get();

    inOrder.verify(firebaseMock.getTasksQuery()).addChildEventListener(firebaseMock.getTasksChildEventListener());
    inOrder.verify(firebaseMock.getTimeoutQuery()).addChildEventListener(firebaseMock.getTimeoutChildEventListener());
  }

  @Test
  public void buildingAQueue_andAValidMatchingSpecComesIn_doesNotShutDownExecutors() {
    instantiateQueue(getValidTaskSpecSnapshot(), true);

    verify(queueHelper.queueExecutor, never()).shutdownNow();
    verify(queueHelper.timeoutExecutor, never()).shutdownNow();
  }

  @Test
  public void buildingAQueue_andAValidMatchingSpecComesIn_doesNotRemoveChildListenersFromTheFirebaseQueries() {
    instantiateQueue(getValidTaskSpecSnapshot(), true);

    verify(firebaseMock.getTasksQuery(), never()).removeEventListener(firebaseMock.getTasksChildEventListener());
    verify(firebaseMock.getTimeoutQuery(), never()).removeEventListener(firebaseMock.getTimeoutChildEventListener());
  }

  @Test
  public void buildingAQueue_andAnInvalidMatchingSpecComesIn_writesAWarningLog() {
    DataSnapshot specSnapshot = getInvalidTaskSpecSnapshot();
    instantiateQueue(specSnapshot, false);

    verify(logger).log("Got a new spec, but it was not valid - " + new TaskSpec(specSnapshot), Log.Level.WARN);
  }

  @Test
  public void buildingAQueue_andAnInvalidMatchingSpecComesIn_doesNotUseThatTaskSpecForTheTaskReset() {
    instantiateQueue(getInvalidTaskSpecSnapshot(), false);

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void buildingAQueue_andAnInvalidMatchingSpecComesIn_doesNotGetNewExecutors() {
    instantiateQueue(getInvalidTaskSpecSnapshot(), false);

    verifyZeroInteractions(queueHelper.queueExecutorFactory, queueHelper.timeoutExecutorFactory);
  }

  @Test
  public void buildingAQueue_andAnInvalidMatchingSpecComesIn_doesNotSetATaskListenerOnTheQueueExecutor() {
    instantiateQueue(getInvalidTaskSpecSnapshot(), false);

    verifyZeroInteractions(queueHelper.queueExecutorFactory);
  }

  @Test
  public void buildingAQueue_andAnInvalidMatchingSpecComesIn_doesNotAddChildListenersToTheFirebaseQueries() {
    instantiateQueue(getInvalidTaskSpecSnapshot(), false);

    verifyZeroInteractions(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());
  }

  @Test
  public void whenListeningForANewSpecIsCancelled_beforeAMatchingSpecComesIn_itIsLogged() {
    instantiateQueueWithoutTaskSpec();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("Something went wrong"));

    firebaseMock.getSpecIdValueEventListener().onCancelled(error);

    verify(logger).log("There was an error listening for value events on " + Queue.SPEC_CHILD, error);
  }

  @Test
  public void whenListeningForANewSpecIsCancelled_afterAMatchingSpecComesIn_itIsLogged() {
    instantiateQueue();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("Something went wrong"));

    firebaseMock.getSpecIdValueEventListener().onCancelled(error);

    verify(logger).log("There was an error listening for value events on " + Queue.SPEC_CHILD, error);
  }

  @Test
  public void whenANewValidMatchingSpecComesIn_writesAnInfoLog() {
    instantiateQueue();

    reset(logger);

    DataSnapshot specSnapshot = simulateNewSpec(true);

    verify(logger).log("Got a new spec - " + new TaskSpec(specSnapshot), Log.Level.INFO);
  }

  @Test
  public void whenANewValidMatchingSpecComesIn_behavesLikeANewSpecCameIn() {
    instantiateQueue();

    // grab a reference to these here because they will be nulled out on new spec
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    reset(queueHelper.taskReset);

    simulateNewSpec(true);

    InOrder inOrder = inOrder(queueHelper.taskReset, queueHelper.queueExecutorFactory, queueHelper.timeoutExecutorFactory, queueHelper.queueExecutor, queueHelper.timeoutExecutor, firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    verify(queueHelper.taskReset).onNewTaskSpec(eq(queueHelper.taskSpec));

    verify(firebaseMock.getTasksQuery()).removeEventListener(taskQueryListener);
    verify(firebaseMock.getTimeoutQuery()).removeEventListener(timeoutQueryListener);

    inOrder.verify(queueHelper.queueExecutor).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor).shutdownNow();

    inOrder.verify(queueHelper.queueExecutorFactory).get();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(notNull(QueueExecutor.TaskStateListener.class));
    inOrder.verify(queueHelper.timeoutExecutorFactory).get();

    inOrder.verify(firebaseMock.getTasksQuery()).addChildEventListener(firebaseMock.getTasksChildEventListener());
    inOrder.verify(firebaseMock.getTimeoutQuery()).addChildEventListener(firebaseMock.getTimeoutChildEventListener());
  }

  @Test
  public void whenANewInvalidMatchingSpecComesIn_writesAWarningLog() {
    instantiateQueue();

    DataSnapshot specSnapshot = getInvalidTaskSpecSnapshot();
    simulateNewSpec(specSnapshot, false);

    verify(logger).log("Got a new spec, but it was not valid - " + new TaskSpec(specSnapshot), Log.Level.WARN);
  }

  @Test
  public void whenANewInvalidMatchingSpecComesIn_doesNotUseThatTaskSpecForTheTaskReset() {
    instantiateQueue();

    reset(queueHelper.taskReset);

    simulateNewSpec(getInvalidTaskSpecSnapshot(), false);

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void whenANewInvalidMatchingSpecComesIn_behavesLikeAnInvalidSpecCameIn() {
    instantiateQueue();

    // grab a reference to these here because they will be nulled out on new spec
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    simulateNewSpec(getInvalidTaskSpecSnapshot(), false);

    InOrder inOrder = inOrder(queueHelper.queueExecutor, queueHelper.timeoutExecutor, firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());

    verify(firebaseMock.getTasksQuery()).removeEventListener(taskQueryListener);
    verify(firebaseMock.getTimeoutQuery()).removeEventListener(timeoutQueryListener);

    inOrder.verify(queueHelper.queueExecutor).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor).shutdownNow();
  }

  @Test
  public void whenTheQueueIsShutdown_beforeAMatchingSpecComesIn_theTaskQueriesAreNotRemoved() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    queue.shutdown();

    verifyZeroInteractions(firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery());
  }

  @Test
  public void whenTheQueueIsShutdown_beforeAMatchingSpecComesIn_theExecutorsAreNotShutDown() {
    Queue queue = instantiateQueueWithoutTaskSpec();

    queue.shutdown();

    verifyZeroInteractions(queueHelper.queueExecutor, queueHelper.timeoutExecutor);
  }

  @Test
  public void whenTheQueueIsShutdown_ifNewTasksWereBeingListenedFor_theSpecEventListenerIsRemoved_thenTheTaskQueriesAreRemoved_andThenTheExecutorsAreShutDown() {
    Queue queue = instantiateQueue();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    // grab a reference to these here because they will be nulled out on shutdown
    ValueEventListener specValueListener = firebaseMock.getSpecIdValueEventListener();
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    InOrder inOrder = inOrder(firebaseMock.getSpecIdRef(), firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery(), queueHelper.queueExecutor, queueHelper.timeoutExecutor);

    queue.shutdown();

    verify(firebaseMock.getSpecIdRef()).removeEventListener(specValueListener);
    inOrder.verify(firebaseMock.getTasksQuery()).removeEventListener(taskQueryListener);
    inOrder.verify(firebaseMock.getTimeoutQuery()).removeEventListener(timeoutQueryListener);
    inOrder.verify(queueHelper.queueExecutor).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor).shutdownNow();
  }

  @Test
  public void whenTheQueueIsShutdownTwice_ifNewTasksWereBeingListenedFor_theSpecEventListenerIsRemoved_thenTheTaskQueriesAreRemoved_andThenTheExecutorsAreShutDown_onlyOnce() {
    Queue queue = instantiateQueue();

    DataSnapshot specSnapshot = getValidTaskSpecSnapshot();
    queueHelper.stubTaskSpec(specSnapshot, true);

    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);

    // grab a reference to these here because they will be nulled out on shutdown
    ValueEventListener specValueListener = firebaseMock.getSpecIdValueEventListener();
    ChildEventListener taskQueryListener = firebaseMock.getTasksChildEventListener();
    ChildEventListener timeoutQueryListener = firebaseMock.getTimeoutChildEventListener();

    InOrder inOrder = inOrder(firebaseMock.getSpecIdRef(), firebaseMock.getTasksQuery(), firebaseMock.getTimeoutQuery(), queueHelper.queueExecutor, queueHelper.timeoutExecutor);

    queue.shutdown();
    queue.shutdown();

    verify(firebaseMock.getSpecIdRef()).removeEventListener(specValueListener);
    inOrder.verify(firebaseMock.getTasksQuery(), times(1)).removeEventListener(taskQueryListener);
    inOrder.verify(firebaseMock.getTimeoutQuery(), times(1)).removeEventListener(timeoutQueryListener);
    inOrder.verify(queueHelper.queueExecutor, times(1)).shutdownNow();
    inOrder.verify(queueHelper.queueExecutor, times(1)).setTaskStateListener(null);
    inOrder.verify(queueHelper.timeoutExecutor, times(1)).shutdownNow();
  }

  private Queue instantiateQueue(DataSnapshot specSnapshot, boolean isValid) {
    Queue queue = queueHelper.getNewBuilder()
        .specId("some_spec")
        .build();

    simulateNewSpec(specSnapshot, isValid);

    return queue;
  }

  protected Queue instantiateQueueWithoutTaskSpec() {
    return queueHelper.getNewBuilder()
        .specId("some_spec")
        .build();
  }

  @Override
  protected Queue instantiateQueue() {
    Queue queue = queueHelper.getNewBuilder()
        .specId("some_spec")
        .build();

    simulateNewSpec();

    return queue;
  }

  private DataSnapshot simulateNewSpec() {
    return simulateNewSpec(getValidTaskSpecSnapshot(), true);
  }

  private DataSnapshot simulateNewSpec(boolean isValid) {
    return simulateNewSpec(getValidTaskSpecSnapshot(), isValid);
  }

  private DataSnapshot simulateNewSpec(DataSnapshot specSnapshot, boolean isValid) {
    queueHelper.stubTaskSpec(specSnapshot, isValid);
    firebaseMock.getSpecIdValueEventListener().onDataChange(specSnapshot);
    return specSnapshot;
  }
}
