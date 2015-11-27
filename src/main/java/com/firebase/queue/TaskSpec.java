package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/*package*/ class TaskSpec {
  /*package*/ static final String START_STATE = "start_state";
  /*package*/ static final String IN_PROGRESS_STATE = "in_progress_state";
  /*package*/ static final String FINISHED_STATE = "finished_state";
  /*package*/ static final String ERROR_STATE = "error_state";
  /*package*/ static final String TIMEOUT = "timeout";
  /*package*/ static final String RETRIES = "retries";

  /*package*/ static final String DEFAULT_IN_PROGRESS_STATE = "in_progress";
  /*package*/ static final String DEFAULT_ERROR_STATE = "error";
  /*package*/ static final long DEFAULT_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  /*package*/ static final long DEFAULT_RETRIES = 0;

  private final String startState;
  @NotNull private final String inProgressState;
  private final String finishedState;
  private final String errorState;
  private final long timeout;
  private final long retries;

  public TaskSpec() {
    this.startState = null;
    this.inProgressState = DEFAULT_IN_PROGRESS_STATE;
    this.finishedState = null;
    this.errorState = DEFAULT_ERROR_STATE;
    this.timeout = DEFAULT_TIMEOUT;
    this.retries = DEFAULT_RETRIES;
  }

  public TaskSpec(DataSnapshot specSnapshot) {
    Object startStateVal = specSnapshot.child(START_STATE).getValue();
    this.startState = startStateVal instanceof String ? ((String) startStateVal) : null;

    Object inProgressStateVal = specSnapshot.child(IN_PROGRESS_STATE).getValue();
    this.inProgressState = inProgressStateVal instanceof String ? ((String) inProgressStateVal) : DEFAULT_IN_PROGRESS_STATE;

    Object finishedStateVal = specSnapshot.child(FINISHED_STATE).getValue();
    this.finishedState = finishedStateVal instanceof String ? ((String) finishedStateVal) : null;

    Object errorStateVal = specSnapshot.child(ERROR_STATE).getValue();
    this.errorState = errorStateVal instanceof String ? ((String) errorStateVal) : DEFAULT_ERROR_STATE;

    this.timeout = firebaseValToLong(specSnapshot.child(TIMEOUT).getValue(), DEFAULT_TIMEOUT);

    this.retries = firebaseValToLong(specSnapshot.child(RETRIES).getValue(), DEFAULT_RETRIES);
  }

  public String getStartState() {
    return startState;
  }

  @NotNull
  public String getInProgressState() {
    return inProgressState;
  }

  public String getFinishedState() {
    return finishedState;
  }

  public String getErrorState() {
    return errorState;
  }

  public long getTimeout() {
    return timeout;
  }

  public long getRetries() {
    return retries;
  }

  public boolean validate() {
    if(startState != null && startState.equals(inProgressState)) {
      return false;
    }

    if(finishedState != null && (finishedState.equals(startState) || finishedState.equals(inProgressState))) {
      return false;
    }

    if(errorState != null && errorState.equals(inProgressState)) {
      return false;
    }

    if(timeout <= 0) {
      return false;
    }

    if(retries < 0) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "TaskSpec{" +
        "startState='" + startState + '\'' +
        ", inProgressState='" + inProgressState + '\'' +
        ", finishedState='" + finishedState + '\'' +
        ", errorState='" + errorState + '\'' +
        ", timeout=" + timeout +
        ", retries=" + retries +
        '}';
  }

  private static long firebaseValToLong(Object o, long defaultVal) {
    if(o instanceof Long) {
      return (Long) o;
    }
    else if(o instanceof Integer) {
      return ((Integer) o).longValue();
    }
    else if(o instanceof Float) {
      return ((Float) o).longValue();
    }
    else if(o instanceof Double) {
      return ((Double) o).longValue();
    }
    else {
      return defaultVal;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskSpec taskSpec = (TaskSpec) o;

    if (timeout != taskSpec.timeout) return false;
    if (retries != taskSpec.retries) return false;
    if (startState != null ? !startState.equals(taskSpec.startState) : taskSpec.startState != null) return false;
    if (!inProgressState.equals(taskSpec.inProgressState)) return false;
    if (finishedState != null ? !finishedState.equals(taskSpec.finishedState) : taskSpec.finishedState != null)
      return false;
    return !(errorState != null ? !errorState.equals(taskSpec.errorState) : taskSpec.errorState != null);

  }

  @Override
  public int hashCode() {
    int result = startState != null ? startState.hashCode() : 0;
    result = 31 * result + inProgressState.hashCode();
    result = 31 * result + (finishedState != null ? finishedState.hashCode() : 0);
    result = 31 * result + (errorState != null ? errorState.hashCode() : 0);
    result = 31 * result + (int) (timeout ^ (timeout >>> 32));
    result = 31 * result + (int) (retries ^ (retries >>> 32));
    return result;
  }
}
