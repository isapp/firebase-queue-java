package com.firebase.queue;

import com.firebase.client.DataSnapshot;

import java.util.concurrent.TimeUnit;

/*package*/ class TaskSpec {
  private static final String START_STATE = "start_state";
  private static final String IN_PROGRESS_STATE = "in_progress_state";
  private static final String FINISHED_STATE = "finished_state";
  private static final String ERROR_STATE = "error_state";
  private static final String TIMEOUT = "timeout";
  private static final String RETRIES = "retries";

  private static final String DEFAULT_IN_PROGRESS_STATE = "in_progress";
  private static final String DEFAULT_ERROR_STATE = "error";
  private static final long DEFAULT_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  private static final long DEFAULT_RETRIES = 0;

  private String startState;
  private String inProgressState;
  private String finishedState;
  private String errorState;
  private long timeout;
  private long retries;

  public TaskSpec() {
    this.inProgressState = DEFAULT_IN_PROGRESS_STATE;
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

    this.timeout = firebaseValToLong(specSnapshot.child(TIMEOUT), DEFAULT_TIMEOUT);

    this.retries = firebaseValToLong(specSnapshot.child(RETRIES), DEFAULT_RETRIES);
  }

  public String getStartState() {
    return startState;
  }

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
}
