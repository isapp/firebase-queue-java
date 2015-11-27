package com.firebase.queue;

import com.firebase.client.Firebase;
import org.jetbrains.annotations.NotNull;

/*package*/ class QueueTask implements Runnable {
  /*package*/ static final String PRE_RUN_ID = "<id not set yet>";
  private String id;

  @NotNull private final Firebase taskRef;
  @NotNull private final TaskSpec taskSpec;
  @NotNull private final TaskReset taskReset;
  @NotNull private final Queue.Options options;

  private Thread executingThread;

  private volatile boolean claimed;
  private volatile boolean cancelled;
  private volatile boolean done;

  public QueueTask(@NotNull Firebase taskRef, @NotNull TaskSpec taskSpec, @NotNull TaskReset taskReset, @NotNull Queue.Options options) {
    this.taskRef = taskRef;
    this.taskSpec = taskSpec;
    this.taskReset = taskReset;
    this.options = options;
  }

  public String getId() {
    return id;
  }

  public String getTaskKey() {
    return taskRef.getKey();
  }

  public boolean isClaimed() {
    return claimed;
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public boolean isDone() {
    return done;
  }

  public void cancel() {
    String id = this.id == null ? PRE_RUN_ID : this.id;

    if(cancelled || done) {
      Log.log("Not cancelling task (" + taskRef.getKey() + ") on " + id + " because it " + (cancelled ? "was already cancelled" : "is already done"));
    }
    else {
      cancelled = true;

      if(executingThread == null) {
        Log.log("Delaying cancelling task (" + taskRef.getKey() + ") on " + id + " because it hasn't started running yet");
      }
      else {
        executingThread.interrupt();
        Log.log("Cancelling task (" + taskRef.getKey() + ") on " + id);
      }
    }
  }

  @Override
  public void run() {
    id = Thread.currentThread().getName() + ":" + UuidUtils.getUUID();

    if(cancelled) {
      Log.log("Can't run task (" + taskRef.getKey() + ") on " + id +  " because it has previously been cancelled");
      return;
    }

    executingThread = Thread.currentThread();

    Log.log("Started claiming task (" + taskRef.getKey() + ") on " + id);

    TaskClaimer.TaskGenerator taskGenerator = getTaskClaimer(id, taskRef, taskSpec, taskReset, options).claimTask();
    if(taskGenerator == null) {
      Log.log("Couldn't claim task (" + taskRef.getKey() + ") on " + id);
      done = true;
      return;
    }

    Log.log("Claimed task (" + taskRef.getKey() + ") on " + id);

    // it is possible that we got cancelled while claiming a task and the TaskClaimer didn't pick that up
    if(cancelled) {
      Log.log("Can't process task (" + taskRef.getKey() + ") on " + id + " because it was cancelled while we were claiming it");
      done = true;
      return;
    }

    claimed = true;

    Log.log("Started processing task (" + taskRef.getKey() + ") on " + id);

    ValidityChecker validityChecker = getValidityChecker(id);

    Task task = taskGenerator.generateTask(id, taskSpec, taskReset, validityChecker, options);
    task.process(options.taskProcessor);

    done = true;

    validityChecker.destroy();

    Log.log("Finished processing task (" + taskRef.getKey() + ") on " + id);
  }

  /*package*/ TaskClaimer getTaskClaimer(String id, Firebase taskRef, TaskSpec taskSpec, TaskReset taskReset, Queue.Options options) {
    return new TaskClaimer(id, taskRef, taskSpec, taskReset, options.sanitize);
  }

  /*package*/ ValidityChecker getValidityChecker(String id) {
    return new ValidityChecker(Thread.currentThread(), id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    QueueTask queueTask = (QueueTask) o;

    if (id != null ? !id.equals(queueTask.id) : queueTask.id != null) return false;
    return taskRef.equals(queueTask.taskRef);

  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + taskRef.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "QueueTask{" +
            "id='" + id + "', " +
            "task='" + taskRef.getKey() + '\'' +
            '}';
  }
}
