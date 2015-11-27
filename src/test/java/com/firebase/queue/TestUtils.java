package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.when;

public class TestUtils {
  private TestUtils() {}

  public static DataSnapshot getDefaultTaskSpecSnapshot() {
    return getTaskSpecSnapshot(null, TaskSpec.DEFAULT_IN_PROGRESS_STATE, null, TaskSpec.DEFAULT_ERROR_STATE, TaskSpec.DEFAULT_TIMEOUT, TaskSpec.DEFAULT_RETRIES);
  }

  public static DataSnapshot getValidTaskSpecSnapshot() {
    return getTaskSpecSnapshot("start_state", "in_progress_state", "finshed_state", "error_state", TimeUnit.MILLISECONDS.convert(40, TimeUnit.MINUTES), 1L);
  }

  public static DataSnapshot getInvalidTaskSpecSnapshot() {
    return getTaskSpecSnapshot("start_state", "start_state", "start_state", "start_state", 0L, -1L);
  }

  public static DataSnapshot getTaskSpecSnapshot(String startState, String inProgressState, String finishedState, String errorState, long timeout, long retries) {
    return getTaskSpecSnapshot((Object) startState, inProgressState, finishedState, errorState, timeout, retries);
  }

  public static DataSnapshot getTaskSpecSnapshot(Object startState, Object inProgressState, Object finishedState, Object errorState, Object timeout, Object retries) {
    DataSnapshot snapshot = mock(DataSnapshot.class, RETURNS_DEEP_STUBS);
    when(snapshot.child(TaskSpec.START_STATE).getValue()).thenReturn(startState);
    when(snapshot.child(TaskSpec.IN_PROGRESS_STATE).getValue()).thenReturn(inProgressState);
    when(snapshot.child(TaskSpec.FINISHED_STATE).getValue()).thenReturn(finishedState);
    when(snapshot.child(TaskSpec.ERROR_STATE).getValue()).thenReturn(errorState);
    when(snapshot.child(TaskSpec.TIMEOUT).getValue()).thenReturn(timeout);
    when(snapshot.child(TaskSpec.RETRIES).getValue()).thenReturn(retries);
    return snapshot;
  }

  public static DataSnapshot getBasicTaskSnapshot() {
    DataSnapshot snapshot = mock(DataSnapshot.class);
    stub(snapshot.getRef()).toReturn(mock(Firebase.class));
    stub(snapshot.getValue(Map.class)).toReturn(Collections.<String, Object> emptyMap());
    return snapshot;
  }

  public static DataSnapshot getBasicTaskSnapshotWithKey(String key) {
    DataSnapshot snapshot = mock(DataSnapshot.class);
    stub(snapshot.getRef()).toReturn(mock(Firebase.class));
    stub(snapshot.getValue(Map.class)).toReturn(Collections.<String, Object> emptyMap());
    stub(snapshot.getKey()).toReturn(key);
    return snapshot;
  }

  public static DataSnapshot getTaskSnapshot(String key, Map<String, Object> value) {
    DataSnapshot snapshot = mock(DataSnapshot.class);
    stub(snapshot.getRef()).toReturn(mock(Firebase.class));
    stub(snapshot.getValue(Map.class)).toReturn(value);
    stub(snapshot.getKey()).toReturn(key);
    return snapshot;
  }

  public static QueueTask getBasicQueueTaskWithKey(String key) {
    QueueTask queueTask = mock(QueueTask.class);
    stub(queueTask.getTaskKey()).toReturn(key);
    return queueTask;
  }
}
