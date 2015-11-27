package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.MutableData;
import com.firebase.client.ServerValue;
import com.firebase.client.Transaction;
import com.firebase.client.snapshot.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static com.firebase.queue.TestUtils.getBasicTaskSnapshotWithKey;
import static com.firebase.queue.TestUtils.getDefaultTaskSpecSnapshot;
import static com.firebase.queue.TestUtils.getTaskSpecSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
    Transaction.class
)
public class TaskResetTest {
  private static final String TASK_KEY = "task_key";
  private static final String OWNER_ID = "owner";
  private static final String OTHER_OWNER_ID = "other_owner";
  public static final String OTHER_IN_PROGRESS_STATE = "other_in_progress_state";

  private TaskReset subject;

  private Firebase taskRef;
  private Log.Logger logger;

  @Before
  public void setUp() throws Exception {
    subject = new TaskReset();
    subject.onNewTaskSpec(new TaskSpec(getDefaultTaskSpecSnapshot()));

    taskRef = mock(Firebase.class);
    stub(taskRef.getKey()).toReturn(TASK_KEY);

    logger = mock(Log.Logger.class);
    Log.setLogger(logger);
  }

  @Test
  public void reset_whenTheDataAtTheRefIsNull_causesTheTransactionToSucceed() {
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(null, true);
    Transaction.Result result = handler.doTransaction(data);
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getNode().getValue()).isNull();
  }

  @Test
  public void reset_whenTheDataAtTheRefIsNotNull_writesALog() {
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    handler.doTransaction(mockTransactionResult(getTaskValues(), true));

    verify(logger).log("Resetting task " + TASK_KEY, Log.Level.INFO);
  }

  @Test
  public void reset_whenOwnerIdIsNull_andInProgressStateMatchesTheState_causesTheTransactionToSucceed() {
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), true);
    Transaction.Result result = handler.doTransaction(data);
    assertThat(result.isSuccess());
  }

  @Test
  public void reset_whenOwnerIdIsNull_andInProgressStateMatchesTheState_resetsTheTask() {
    subject.onNewTaskSpec(new TaskSpec(getTaskSpecSnapshot("start_state", TaskSpec.DEFAULT_IN_PROGRESS_STATE, null, null, null, null)));
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), true);
    Transaction.Result result = handler.doTransaction(data);
    Map<String, Object> resultValue = (Map<String, Object>) result.getNode().getValue();
    assertThat(resultValue.get(Task.STATE_KEY)).isEqualTo("start_state");
    assertThat(resultValue.get(Task.STATE_CHANGED_KEY)).isEqualTo(ServerValue.TIMESTAMP);
    assertThat(resultValue.get(Task.OWNER_KEY)).isNull();
    assertThat(resultValue.get(Task.ERROR_DETAILS_KEY)).isNull();
  }

  @Test
  public void reset_whenTheOwnerIdIsNull_andInProgressStateDoesNotMatchTheState_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(listener).onResetFailed("Couldn't reset this task because it is owned by another worker", false);
  }

  @Test
  public void reset_whenTheOwnerIdIsNull_andInProgressStateDoesNotMatchTheState_writesALog() {
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(logger).log("Can't reset task " + taskRef.getKey() + " - _state != in_progress_state", Log.Level.INFO);
  }

  @Test
  public void reset_whenTheOwnerIdIsNull_andInProgressStateDoesNotMatchTheState_abortsTheTransaction() {
    subject.reset(taskRef, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void reset_whenOwnerIdMatches_andInProgressStateMatchesTheState_causesTheTransactionToSucceed() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), true);
    Transaction.Result result = handler.doTransaction(data);
    assertThat(result.isSuccess());
  }

  @Test
  public void reset_whenOwnerIdMatches_andInProgressStateMatchesTheState_resetsTheTask() {
    subject.onNewTaskSpec(new TaskSpec(getTaskSpecSnapshot("start_state", TaskSpec.DEFAULT_IN_PROGRESS_STATE, null, null, null, null)));
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), true);
    Transaction.Result result = handler.doTransaction(data);
    Map<String, Object> resultValue = (Map<String, Object>) result.getNode().getValue();
    assertThat(resultValue.get(Task.STATE_KEY)).isEqualTo("start_state");
    assertThat(resultValue.get(Task.STATE_CHANGED_KEY)).isEqualTo(ServerValue.TIMESTAMP);
    assertThat(resultValue.get(Task.OWNER_KEY)).isNull();
    assertThat(resultValue.get(Task.ERROR_DETAILS_KEY)).isNull();
  }

  @Test
  public void reset_whenTheOwnerIdMatches_andInProgressStateDoesNotMatchTheState_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(listener).onResetFailed("Couldn't reset this task because it is owned by another worker", false);
  }

  @Test
  public void reset_whenTheOwnerIdMatches_andInProgressStateDoesNotMatchTheState_writesALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(logger).log("Can't reset task " + taskRef.getKey() + " - _state != in_progress_state", Log.Level.INFO);
  }

  @Test
  public void reset_whenTheOwnerIdMatches_andInProgressStateDoesNotMatchTheState_abortsTheTransaction() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void reset_whenOwnerIdDoesNotMatch_andInProgressStateMatchesTheState_abortsTheTransaction() {
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);
    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void reset_whenOwnerIdDoesNotMatch_andInProgressStateMatchesTheState_doesNotResetTheTask() {
    subject.onNewTaskSpec(new TaskSpec(getTaskSpecSnapshot("start_state", TaskSpec.DEFAULT_IN_PROGRESS_STATE, null, null, null, null)));
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(TaskSpec.DEFAULT_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);
    Map<String, Object> resultValue = (Map<String, Object>) result.getNode().getValue();
    assertThat(resultValue.get(Task.STATE_KEY)).isEqualTo(TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    assertThat(resultValue.get(Task.OWNER_KEY)).isEqualTo(OWNER_ID);
  }

  @Test
  public void reset_whenTheOwnerIdDoesNotMatch_andInProgressStateDoesNotMatchTheState_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(listener).onResetFailed("Couldn't reset this task because it is owned by another worker", false);
  }

  @Test
  public void reset_whenTheOwnerIdDoesNotMatch_andInProgressStateDoesNotMatchTheState_writesALog() {
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    handler.doTransaction(data);

    verify(logger).log("Can't reset task " + taskRef.getKey() + " on " + OTHER_OWNER_ID + " because it is owned by " + OWNER_ID, Log.Level.INFO);
  }

  @Test
  public void reset_whenTheOwnerIdDoesNotMatch_andInProgressStateDoesNotMatchTheState_abortsTheTransaction() {
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void reset_whenOwnerIdDoesNotMatch_andInProgressStateDoesNotMatchTheState_doesNotResetTheTask() {
    subject.onNewTaskSpec(new TaskSpec(getTaskSpecSnapshot("start_state", TaskSpec.DEFAULT_IN_PROGRESS_STATE, null, null, null, null)));
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(getTaskValues(OTHER_IN_PROGRESS_STATE, OWNER_ID), false);
    Transaction.Result result = handler.doTransaction(data);
    Map<String, Object> resultValue = (Map<String, Object>) result.getNode().getValue();
    assertThat(resultValue.get(Task.STATE_KEY)).isEqualTo(OTHER_IN_PROGRESS_STATE);
    assertThat(resultValue.get(Task.OWNER_KEY)).isEqualTo(OWNER_ID);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasNotBeenReached_writesALog() {
    subject.reset(taskRef, OTHER_OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();
    mockTransactionResult(getTaskValues(), false);

    FirebaseError error = FirebaseError.fromException(new RuntimeException("AHHHHHHH!!!!!"));
    handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));
    verify(logger).log("Received error while resetting task " + taskRef.getKey() + "...retrying", error);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasNotBeenReached_runsTheTransactionAgain() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("AHHHHHHH!!!!!"));
    handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));
    verify(taskRef).runTransaction(handler, false);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasBeenReached_writesALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("AHHHHHHH!!!!!"));
    for(int i = 0; i < Queue.MAX_TRANSACTION_RETRIES; i++) {
      handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));
      handler = verifyTransactionStartedAtLeastOnce();
    }

    verify(logger).log("Can't reset task " + taskRef.getKey() + " - transaction errored too many times, no longer retrying", error);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasBeenReached_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("AHHHHHHH!!!!!"));
    for(int i = 0; i < Queue.MAX_TRANSACTION_RETRIES; i++) {
      handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));
      handler = verifyTransactionStartedAtLeastOnce();
    }

    verify(listener).onResetFailed("Can't reset task - transaction errored too many times, no longer retrying - " + error, true);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereCommitted_andTheSnapshotExists_writesALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    verify(logger).log("reset " + taskRef.getKey(), Log.Level.INFO);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereCommitted_andTheSnapshotExists_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    verify(listener).onReset();
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereNotCommitted_andTheSnapshotExists_doesNotWriteALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, false, snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereNotCommitted_andTheSnapshotExists_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, false, snapshot);

    verify(listener).onReset();
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereCommitted_andTheSnapshotDoesNotExist_doesNotWriteALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, true, snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereCommitted_andTheSnapshotDoesNotExist_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, true, snapshot);

    verify(listener).onReset();
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereNotCommitted_andTheSnapshotDoesNotExist_doesNotWriteALog() {
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, false, snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void reset_whenTheTransactionIsComplete_andThereIsNoError_andTheChangesWereNotCommitted_andTheSnapshotDoesNotExist_invokesTheListener() {
    TaskReset.Listener listener = mock(TaskReset.Listener.class);
    subject.reset(taskRef, OWNER_ID, TaskSpec.DEFAULT_IN_PROGRESS_STATE, listener);
    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, false, snapshot);

    verify(listener).onReset();
  }

  private Transaction.Handler verifyTransactionStarted() {
    ArgumentCaptor<Transaction.Handler> captor = ArgumentCaptor.forClass(Transaction.Handler.class);
    verify(taskRef).runTransaction(captor.capture(), eq(false));
    return captor.getValue();
  }

  private Transaction.Handler verifyTransactionStartedAtLeastOnce() {
    ArgumentCaptor<Transaction.Handler> captor = ArgumentCaptor.forClass(Transaction.Handler.class);
    verify(taskRef, atLeastOnce()).runTransaction(captor.capture(), eq(false));
    return captor.getValue();
  }

  private MutableData mockTransactionResult(Map<String, Object> value, boolean success) {
    mockStatic(Transaction.class);
    MutableData data = mock(MutableData.class);
    stub(data.getValue()).toReturn(value);
    stub(data.getValue(Map.class)).toReturn(value);
    Transaction.Result result = mock(Transaction.Result.class);
    stub(result.isSuccess()).toReturn(success);
    Node node = mock(Node.class);
    stub(node.getValue()).toReturn(value);
    stub(result.getNode()).toReturn(node);
    if(success) {
      when(Transaction.success(data)).thenReturn(result);
    }
    else {
      when(Transaction.abort()).thenReturn(result);
    }
    return data;
  }

  private Map<String, Object> getTaskValues() {
    return getTaskValues("state", "owner");
  }

  private Map<String, Object> getTaskValues(final String state, final String owner) {
    return new HashMap<String, Object>(2) {{
      put(Task.STATE_KEY, state);
      put(Task.OWNER_KEY, owner);
    }};
  }
}
