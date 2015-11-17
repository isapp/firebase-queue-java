package com.firebase.queue;

import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.Query;
import com.firebase.client.ValueEventListener;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Queue {
  static boolean slept;
  static boolean aborted;
  public static void main(final String[] args) {
    final Firebase fb = new Firebase("https://fb-queue-test-2.firebaseio.com/");

    fb.removeValue(new Firebase.CompletionListener() {
      @Override
      public void onComplete(FirebaseError firebaseError, Firebase firebase) {
        Map<String, Object> queueVals = new HashMap<String, Object>() {{
          put("1", new HashMap<String, String>() {{
            put("val", "some string");
          }});
          put("2", new HashMap<String, String>() {{
            put("val", "some other string");
          }});
          put("3", new HashMap<String, String>() {{
            put("val", "hello");
          }});
          put("4", new HashMap<String, String>() {{
            put("val", "wassup");
          }});
          put("5", new HashMap<String, String>() {{
            put("val", "last but not least");
          }});
          put("5", new ArrayList<String>() {{
            add("malformed");
          }});
        }};
        fb.child("queue/tasks/").setValue(queueVals, new Firebase.CompletionListener() {
          @Override
          public void onComplete(FirebaseError firebaseError, Firebase firebase) {
            Queue q = new Builder(fb.child("queue"), new TaskProcessor() {
              @Override
              public void process(final Task task) throws InterruptedException {
                String val = task.getData().get("val").toString();
                Log.log(val, Log.Level.ERROR);

                if("wassup".equals(val)) {
                  task.reject("I don't like this word");
                }
                else if("hello".equals(val)) {
                  if(!slept) {
                    slept = true;
                    try {
                      Log.log("GOING TO SLEEP", Log.Level.ERROR);
                      Thread.sleep(30000);
                    } catch (InterruptedException e) {
                      throw e;
                    }
                  }
                  else if(!aborted) {
                    aborted = true;
                    task.abort();
                  }
                  else {
                    task.resolve();
                  }
                }
                else {
                  task.resolve();
                }
              }
            })
                .numWorkers(4)
                .build();
          }
        });
      }
    });

    try {
      Thread.sleep(Long.MAX_VALUE);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static final long MAX_TRANSACTION_RETRIES = 10;

  private static final String TASK_CHILD = "tasks";
  private static final String SPEC_CHILD = "specs";

  private static final TaskSpec DEFAULT_TASK_SPEC = new TaskSpec();

  private final Options options;

  private final Map<String, QueueTask> executingTasks = new HashMap<String, QueueTask>();
  private final Map<String, Runnable> timeoutsInFlight = new HashMap<String, Runnable>();

  private final QueueExecutor.Factory executorFactory;
  private QueueExecutor executor;
  private ScheduledThreadPoolExecutor timeoutExecutor;

  private Query newTaskQuery;
  private final ChildEventListener newTaskListener = new ChildEventAdapter() {
    @Override
    public void onChildAdded(final DataSnapshot taskSnapshot, String previousChildKey) {
      onNewTask(taskSnapshot);
    }

    @Override
    public void onChildChanged(DataSnapshot taskSnapshot, String previousChildKey) {
      onNewTask(taskSnapshot);
    }

    private void onNewTask(DataSnapshot taskSnapshot) {
      if(shutdown.get()) {
        return;
      }

      Log.log("Received new task - " + taskSnapshot);

      QueueTask task = new QueueTask(taskSnapshot.getRef(), taskSpec, taskReset, options);
      executor.execute(task);
    }

    @Override
    public void onCancelled(FirebaseError error) {
      Log.log("There was an error listening for children with a " + Task.STATE_KEY + " of " + taskSpec.getStartState(), error);
    }
  };

  private Query timeoutQuery;
  private final ChildEventListener timeoutListener = new ChildEventAdapter() {
    @Override
    public void onChildAdded(DataSnapshot snapshot, String previousChildKey) {
      setTimeout(snapshot);
    }

    @Override
    public void onChildChanged(DataSnapshot snapshot, String previousChildKey) {
      setTimeout(snapshot);
    }

    @Override
    public void onChildRemoved(DataSnapshot snapshot) {
      Runnable timeout = timeoutsInFlight.remove(snapshot.getKey());
      if(timeout != null) {
        timeoutExecutor.remove(timeout);
        Log.log("Cancelling timeout for " + snapshot);
      }
    }

    private void setTimeout(final DataSnapshot snapshot) {
      if(shutdown.get()) {
        return;
      }

      long timeoutDelay = getTimeoutDelay(snapshot);

      Runnable timeout = new Runnable() {
        @Override
        public void run() {
          if(timeoutsInFlight.remove(snapshot.getKey()) == null) {
            return;
          }

          QueueTask runningTask = executingTasks.remove(snapshot.getKey());
          if(runningTask != null) {
            Log.log("Task " + runningTask.getTaskKey() + " has timedout while running");
            runningTask.cancel();
          }
          else {
            Log.log("Task " + snapshot.getKey() + " has timedout");
          }

          taskReset.reset(snapshot.getRef(), taskSpec.getInProgressState());
        }
      };

      if(timeoutsInFlight.containsKey(snapshot.getKey())) {
        timeoutExecutor.remove(timeoutsInFlight.get(snapshot.getKey()));
        Log.log("Received updated task to monitor for timeouts - " + snapshot + " (timeout in " + timeoutDelay + " ms)");
      }
      else {
        Log.log("Received new task to monitor for timeouts - " + snapshot + " (timeout in " + timeoutDelay + " ms)");
      }

      timeoutExecutor.schedule(timeout, timeoutDelay, TimeUnit.MILLISECONDS);
      timeoutsInFlight.put(snapshot.getKey(), timeout);
    }

    private long getTimeoutDelay(DataSnapshot snapshot) {
      @SuppressWarnings("unchecked") Map<String, Object> value = snapshot.getValue(Map.class);
      Object timeStartedVal = value.get(Task.STATE_CHANGED_KEY);
      if(timeStartedVal instanceof Long) {
        long timeStarted = (Long) timeStartedVal;
        long timeElapsedSinceStart = (System.currentTimeMillis() - timeStarted);
        long timeout = taskSpec.getTimeout() - (timeElapsedSinceStart < 0 ? 0 : timeElapsedSinceStart);
        return timeout < 0 ? 0 : timeout;
      }
      else {
        return 0;
      }
    }

    @Override
    public void onCancelled(FirebaseError error) {
      Log.log("There was an error listening for timeouts with a " + Task.STATE_KEY + " of " + taskSpec.getInProgressState(), error);
    }
  };

  private final Firebase taskRef;

  private final TaskReset taskReset;

  private TaskSpec taskSpec;
  private final Firebase specRef;
  private final ValueEventListener specChangeListener = new ValueEventListener() {
    @Override
    public void onDataChange(DataSnapshot specSnapshot) {
      if(shutdown.get()) {
        return;
      }

      taskSpec = new TaskSpec(specSnapshot);
      if(taskSpec.validate()) {
        Log.log("Got a new spec - " + taskSpec);
        onNewSpec();
      }
      else {
        Log.log("Got a new spec, but it was not valid - " + taskSpec, Log.Level.WARN);
        onInvalidSpec();
        taskSpec = null;
      }
    }

    @Override
    public void onCancelled(FirebaseError error) {
      Log.log("There was an error listening for value events on " + SPEC_CHILD, error);
    }
  };

  private final TaskStateListener taskStateListener = new TaskStateListener() {
    @Override
    public void onTaskStart(Thread thread, QueueTask task) {
      executingTasks.put(task.getTaskKey(), task);
    }

    @Override
    public void onTaskFinished(QueueTask task, Throwable error) {
      executingTasks.remove(task.getTaskKey());
    }
  };

  private AtomicBoolean shutdown = new AtomicBoolean(false);

  private Queue(Builder builder) {
    this.options = new Options(builder);

    Firebase queueRef = builder.queueRef;

    taskRef = queueRef.child(TASK_CHILD);

    executorFactory = builder.executorFactory;

    taskReset = new TaskReset();

    if(options.specId == null) {
      specRef = null;
      taskSpec = DEFAULT_TASK_SPEC;
      onNewSpec();
    }
    else {
      specRef = queueRef.child(SPEC_CHILD);
      specRef.child(options.specId).addValueEventListener(specChangeListener);
    }
  }

  public void shutdown() {
    if(!shutdown.getAndSet(true)) {
      specRef.removeEventListener(specChangeListener);
      stopListeningForNewTasks();
      shutdownExecutors();
    }
  }

  private void onNewSpec() {
    taskReset.onNewTaskSpec(taskSpec);

    stopListeningForNewTasks();

    shutdownExecutors();

    startExecutors();

    startListeningForNewTasks();
  }

  private void onInvalidSpec() {
    stopListeningForNewTasks();

    shutdownExecutors();
  }

  private void startExecutors() {
    if(shutdown.get()) {
      return;
    }

    executor = executorFactory.get();
    executor.setTaskStateListener(taskStateListener);
    timeoutExecutor = new ScheduledThreadPoolExecutor(1);
  }

  /**
   * shutting down the executors will implicitly cancel all of their running tasks and not run any pending tasks
   */
  private void shutdownExecutors() {
    if(executor != null) {
      executor.shutdownNow();
      executor.setTaskStateListener(null);
      executor = null;

      executingTasks.clear();
    }

    if(timeoutExecutor != null) {
      timeoutExecutor.shutdownNow();
      timeoutExecutor = null;

      timeoutsInFlight.clear();
    }
  }

  private void startListeningForNewTasks() {
    if(shutdown.get()) {
      return;
    }

    newTaskQuery = taskRef.orderByChild(Task.STATE_KEY).equalTo(taskSpec.getStartState()).limitToFirst(1);
    newTaskQuery.addChildEventListener(newTaskListener);

    timeoutQuery = taskRef.orderByChild(Task.STATE_KEY).equalTo(taskSpec.getInProgressState());
    timeoutQuery.addChildEventListener(timeoutListener);
  }

  private void stopListeningForNewTasks() {
    if(newTaskQuery != null && newTaskListener != null) {
      newTaskQuery.removeEventListener(newTaskListener);
    }

    if(timeoutQuery != null && timeoutListener != null) {
      timeoutQuery.removeEventListener(timeoutListener);
    }
  }

  public static class Builder {
    private static final String DEFAULT_SPEC_ID = null;
    private static final int DEFAULT_NUM_WORKERS = 1;
    private static final int UNINITIALIZED_NUM_WORKERS = 1;
    private static final boolean DEFAULT_SANITIZE = true;
    private static final boolean DEFAULT_SUPPRESS_STACK = false;

    private boolean built;

    private Firebase queueRef;
    private TaskProcessor taskProcessor;

    private String specId = DEFAULT_SPEC_ID;
    private int numWorkers = UNINITIALIZED_NUM_WORKERS;
    private boolean sanitize = DEFAULT_SANITIZE;
    private boolean suppressStack = DEFAULT_SUPPRESS_STACK;

    private QueueExecutor.Factory executorFactory;

    public Builder(@NotNull Firebase queueRef, @NotNull TaskProcessor taskProcessor) {
      this.queueRef = queueRef;
      this.taskProcessor = taskProcessor;
    }

    public Builder specId(String specId) {
      this.specId = specId;
      return this;
    }

    /**
     * @throws IllegalArgumentException if {@link Builder#executorFactory(QueueExecutor.Factory)} was called with a non-{@code null} value,
     * or if {@code numWorkers} is < 1
     */
    public Builder numWorkers(int numWorkers) {
      if(executorFactory != null) {
        throw new IllegalArgumentException("Cannot set numWorkers if executorFactory has been set to a non-null value");
      }
      else if(numWorkers < 1) {
        throw new IllegalArgumentException("numWorkers must be greater than 0");
      }

      this.numWorkers = numWorkers;
      return this;
    }

    public Builder sanitize(boolean sanitize) {
      this.sanitize = sanitize;
      return this;
    }

    public Builder suppressStack(boolean suppressStack) {
      this.suppressStack = suppressStack;
      return this;
    }

    /**
     * @throws IllegalArgumentException if {@link Builder#numWorkers(int)} was called
     */
    public Builder executorFactory(QueueExecutor.Factory executorFactory) {
      if(numWorkers != UNINITIALIZED_NUM_WORKERS) {
        throw new IllegalArgumentException("Cannot set executorFactory if numWorkers has been set");
      }

      this.executorFactory = executorFactory;
      return this;
    }

    public Queue build() {
      if(built) {
        throw new IllegalStateException("Cannot call build twice");
      }
      built = true;

      if(numWorkers == UNINITIALIZED_NUM_WORKERS) {
        numWorkers = DEFAULT_NUM_WORKERS;
      }
      if(executorFactory == null) {
        executorFactory = new QueueExecutor.Factory() {
          @Override
          public QueueExecutor get() {
            QueueExecutor executor = new QueueExecutor(numWorkers);
            executor.prestartCoreThread();
            return executor;
          }
        };
      }

      return new Queue(this);
    }
  }

  /*package*/ static class Options {
    public final String specId;
    public final int numWorkers;
    public final boolean sanitize;
    public final boolean suppressStack;
    public final TaskProcessor taskProcessor;

    public Options(Builder builder) {
      this.specId = builder.specId;
      this.numWorkers = builder.numWorkers;
      this.sanitize = builder.sanitize;
      this.suppressStack = builder.suppressStack;
      this.taskProcessor = builder.taskProcessor;
    }
  }

  public interface TaskStateListener {
    void onTaskStart(Thread thread, QueueTask task);
    void onTaskFinished(QueueTask task, Throwable error);
  }
}
