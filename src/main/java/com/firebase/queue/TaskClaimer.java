package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.MutableData;
import com.firebase.client.ServerValue;
import com.firebase.client.Transaction;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/*package*/ class TaskClaimer {
  private static final String[] SANITIZE_KEYS = new String[] { Task.STATE_KEY, Task.STATE_CHANGED_KEY, Task.OWNER_KEY, Task.ERROR_DETAILS_KEY };

  private final String ownerId;
  private final Firebase taskRef;
  private final TaskSpec taskSpec;
  private final TaskReset taskReset;
  private final boolean sanitize;

  private final CountDownLatch taskLatch;

  private volatile TaskGenerator taskGenerator;

  private volatile boolean interrupted;

  private boolean claimed;
  private final Object claimLock = new Object();

  private boolean startedClaiming;

  private long retries;

  public static class TaskGenerator {
    private Firebase taskRef;
    private Map<String, Object> taskData;

    public TaskGenerator(@NotNull Firebase taskRef, @NotNull Map<String, Object> taskData) {
      this.taskRef = taskRef;
      this.taskData = taskData;
    }

    public Task generateTask(@NotNull String ownerId, @NotNull TaskSpec taskSpec, @NotNull TaskReset taskReset,
                             @NotNull ValidityChecker validityChecker, @NotNull Queue.Options options) {
      return new Task(taskRef, ownerId, taskData, taskSpec, taskReset, validityChecker, options.suppressStack);
    }
  }

  public TaskClaimer(@NotNull String ownerId, @NotNull Firebase taskRef, @NotNull TaskSpec taskSpec, @NotNull TaskReset taskReset, boolean sanitize) {
    this.ownerId = ownerId;
    this.taskRef = taskRef;
    this.taskSpec = taskSpec;
    this.taskReset = taskReset;
    this.sanitize = sanitize;

    taskLatch = new CountDownLatch(1);
  }

  public TaskGenerator claimTask() {
    if(startedClaiming) {
      throw new IllegalStateException("Cannot claim a task more than once");
    }

    Log.log("Attempting to claim task " + taskRef.getKey() + " on " + ownerId);

    startedClaiming = true;

    return claimTaskInternal();
  }

  private TaskGenerator claimTaskInternal() {
    taskRef.runTransaction(new Transaction.Handler() {
      private boolean malformed;

      @Override
      public Transaction.Result doTransaction(MutableData taskSnpashot) {
        if(interrupted) {
          Log.debug("Claiming task " + taskRef.getKey() + " on " + ownerId + " was interrupted before we started the transaction");
          return Transaction.abort();
        }

        // if this task no longer exists
        if(taskSnpashot.getValue() == null) {
          Log.debug("Tried claiming task " + taskRef.getKey() + " on " + ownerId + " after someone else removed it");
          return Transaction.success(taskSnpashot);
        }

        // if the task is not in a format that we can understand
        if(!(taskSnpashot.getValue() instanceof Map)) {
          Log.debug("Tried claiming task " + taskRef.getKey() + " on " + ownerId + " but it was malformed (" + taskSnpashot.getValue() + ")", Log.Level.WARN);

          malformed = true;
          String error = "Task was malformed";

          Map<String, Object> errorDetails = new HashMap<String, Object>(2);
          errorDetails.put(Task.ERROR_KEY, error);
          errorDetails.put(Task.ORIGINAL_TASK_KEY, taskSnpashot.getValue());

          Map<String, Object> errorMap = new HashMap<String, Object>(3);
          errorMap.put(Task.STATE_KEY, taskSpec.getErrorState());
          errorMap.put(Task.STATE_CHANGED_KEY, ServerValue.TIMESTAMP);
          errorMap.put(Task.ERROR_DETAILS_KEY, errorDetails);

          taskSnpashot.setValue(errorMap);
          return Transaction.success(taskSnpashot);
        }

        @SuppressWarnings("unchecked") Map<String, Object> value = taskSnpashot.getValue(Map.class);
        String ourStartState = taskSpec.getStartState();
        Object taskState = value.get(Task.STATE_KEY);
        if(ourStartState == taskState || (ourStartState != null && ourStartState.equals(taskState))) {
          value.put(Task.STATE_KEY, taskSpec.getInProgressState());
          value.put(Task.STATE_CHANGED_KEY, ServerValue.TIMESTAMP);
          value.put(Task.OWNER_KEY, ownerId);
          taskSnpashot.setValue(value);
          return Transaction.success(taskSnpashot);
        }
        else {
          Log.debug("Tried claiming task " + taskRef.getKey() + " on " + ownerId + " but its _state (" + taskState + ") did not match our _start_state (" + ourStartState + ")");
          return Transaction.abort();
        }
      }

      @Override
      public void onComplete(FirebaseError error, boolean committed, DataSnapshot snapshot) {
        final String taskKey = snapshot.getKey();
        if(error != null) {
          if(interrupted) {
            Log.debug("Claiming task " + taskKey + " on " + ownerId + " was interrupted during a transaction that errored", error);
            taskLatch.countDown();
          }
          else if(++retries < Queue.MAX_TRANSACTION_RETRIES) {
            Log.debug("Received error while claiming task " + taskKey + " on " + ownerId + "...retrying", error);
            claimTaskInternal();
          }
          else {
            Log.debug("Can't claim task " + taskKey + " on " + ownerId + " - transaction errored too many times, no longer retrying", error);
            taskLatch.countDown();
          }
        }
        else if(committed && snapshot.exists()) { // we own the task
          if(interrupted) {
            // since we claimed this task, and we have to give it up because we were interrupted, we have to reset it so someone else can try claiming it
            Log.debug("Claiming task " + taskKey + " on " + ownerId + " was interrupted during the transaction");
            taskReset.reset(taskRef, ownerId, taskSpec.getInProgressState());
            taskLatch.countDown();
          }
          else if(malformed) {
            taskLatch.countDown();
          }
          else {
            Log.debug("Claimed task " + taskKey + " on " + ownerId);

            @SuppressWarnings("unchecked") Map<String, Object> value = snapshot.getValue(Map.class);
            if(sanitize) {
              for(String key : SANITIZE_KEYS) {
                value.remove(key);
              }
            }

            synchronized (claimLock) {
              if(!interrupted) {
                claimed = true;

                taskGenerator = new TaskGenerator(snapshot.getRef(), value);
              }

              taskLatch.countDown();
            }
          }
        }
        else {
          // we didn't get the task, so allow execution to continue and try to get another task
          taskLatch.countDown();
        }
      }
    });

    try {
      taskLatch.await();
    }
    catch (InterruptedException e) {
      synchronized (claimLock) {
        if(!claimed) {
          interrupted = true;
          Log.debug("Failed to claim task " + taskRef.getKey() + " on " + ownerId);
          Log.debug("Interrupted while trying to claim a task (" + taskRef.getKey() + ") for " + ownerId);
          Log.debug("Tried claiming task " + taskRef.getKey() + " on " + ownerId + " but we were interrupted");
          return null;
        }
      }
    }

    return taskGenerator;
  }

}
