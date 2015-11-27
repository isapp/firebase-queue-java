package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import org.mockito.Matchers;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.firebase.queue.TestUtils.getDefaultTaskSpecSnapshot;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

public class QueueHelper {
  public final TaskProcessor taskProcessor;
  public final QueueExecutor.Factory queueExecutorFactory;
  public final QueueExecutor queueExecutor;
  public final Queue.TimeoutExecutorFactory timeoutExecutorFactory;
  public final ScheduledThreadPoolExecutor timeoutExecutor;
  public final Queue.TaskSpecFactory taskSpecFactory;
  public final TaskSpec taskSpec;
  public final TaskReset taskReset;

  private final FirebaseMock firebaseMock;

  public QueueHelper(FirebaseMock firebaseMock) {
    taskProcessor = mock(TaskProcessor.class);
    queueExecutorFactory = mock(QueueExecutor.Factory.class);
    queueExecutor = mock(QueueExecutor.class);
    timeoutExecutorFactory = mock(Queue.TimeoutExecutorFactory.class);
    timeoutExecutor = mock(ScheduledThreadPoolExecutor.class);
    taskSpecFactory = mock(Queue.TaskSpecFactory.class);
    taskSpec = mock(TaskSpec.class);
    taskReset = mock(TaskReset.class);

    this.firebaseMock = firebaseMock;

    stubTaskSpec(getDefaultTaskSpecSnapshot(), true);

    stub(queueExecutorFactory.get()).toReturn(queueExecutor);
    stub(timeoutExecutorFactory.get()).toReturn(timeoutExecutor);
    stub(taskSpecFactory.get(Matchers.<DataSnapshot>any())).toReturn(taskSpec);
  }

  public Queue.Builder getNewBuilder() {
    Queue.Builder builder = new Queue.Builder(firebaseMock.getRoot(), taskProcessor)
        .executorFactory(queueExecutorFactory);
    // dirty quasi dependency injection
    Whitebox.setInternalState(builder, "timeoutExecutorFactory", timeoutExecutorFactory);
    Whitebox.setInternalState(builder, "taskSpecFactory", taskSpecFactory);
    Whitebox.setInternalState(builder, "taskReset", taskReset);

    firebaseMock.restubQueries(taskSpec);

    return builder;
  }

  public void stubTaskSpec(DataSnapshot specSnapshot, boolean isValid) {
    TaskSpec tempSpec = new TaskSpec(specSnapshot);

    stub(taskSpec.getStartState()).toReturn(tempSpec.getStartState());
    stub(taskSpec.getInProgressState()).toReturn(tempSpec.getInProgressState());
    stub(taskSpec.getFinishedState()).toReturn(tempSpec.getFinishedState());
    stub(taskSpec.getErrorState()).toReturn(tempSpec.getErrorState());
    stub(taskSpec.getTimeout()).toReturn(tempSpec.getTimeout());
    stub(taskSpec.getRetries()).toReturn(tempSpec.getRetries());
    stub(taskSpec.validate()).toReturn(isValid);
    stub(taskSpec.toString()).toReturn(tempSpec.toString());

    firebaseMock.restubQueries(taskSpec);
  }
}