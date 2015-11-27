package com.firebase.queue;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.firebase.queue.TestUtils.getTaskSpecSnapshot;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class TaskSpecTest {
  private static final String START_STATE = "my_start_state";
  private static final String IN_PROGRESS_STATE = "my_in_progress_state";
  private static final String FINISHED_STATE = "my_finished_state";
  private static final String ERROR_STATE = "my_error_state";
  private static final long TIMEOUT = Long.MAX_VALUE;
  private static final long RETRIES = Long.MAX_VALUE;

  @Test
  public void creatingADefaultTaskSpec_usesTheDefaultValues() {
    TaskSpec subject = new TaskSpec();
    assertThat(subject.getStartState()).isNull();
    assertThat(subject.getInProgressState()).isEqualTo(TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    assertThat(subject.getFinishedState()).isNull();
    assertThat(subject.getErrorState()).isEqualTo(TaskSpec.DEFAULT_ERROR_STATE);
    assertThat(subject.getTimeout()).isEqualTo(TaskSpec.DEFAULT_TIMEOUT);
    assertThat(subject.getRetries()).isEqualTo(TaskSpec.DEFAULT_RETRIES);
  }

  @Test
  public void creatingATaskSpec_withAnEmptyDataSnapshot_usesTheDefaultValues() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(null, null, null, null, null, null));
    assertThat(subject.getStartState()).isNull();
    assertThat(subject.getInProgressState()).isEqualTo(TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    assertThat(subject.getFinishedState()).isNull();
    assertThat(subject.getErrorState()).isEqualTo(TaskSpec.DEFAULT_ERROR_STATE);
    assertThat(subject.getTimeout()).isEqualTo(TaskSpec.DEFAULT_TIMEOUT);
    assertThat(subject.getRetries()).isEqualTo(TaskSpec.DEFAULT_RETRIES);
  }

  @Test
  public void creatingATaskSpec_withADataSnapshotThatHasTheWrongTypes_usesTheDefaultValues() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(1, 1, 1, 1, "", ""));
    assertThat(subject.getStartState()).isNull();
    assertThat(subject.getInProgressState()).isEqualTo(TaskSpec.DEFAULT_IN_PROGRESS_STATE);
    assertThat(subject.getFinishedState()).isNull();
    assertThat(subject.getErrorState()).isEqualTo(TaskSpec.DEFAULT_ERROR_STATE);
    assertThat(subject.getTimeout()).isEqualTo(TaskSpec.DEFAULT_TIMEOUT);
    assertThat(subject.getRetries()).isEqualTo(TaskSpec.DEFAULT_RETRIES);
  }

  @Test
  public void creatingATaskSpec_withADataSnapshot_usesTheValuesFromTheDataSnapshot() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.getStartState()).isEqualTo(START_STATE);
    assertThat(subject.getInProgressState()).isEqualTo(IN_PROGRESS_STATE);
    assertThat(subject.getFinishedState()).isEqualTo(FINISHED_STATE);
    assertThat(subject.getErrorState()).isEqualTo(ERROR_STATE);
    assertThat(subject.getTimeout()).isEqualTo(TIMEOUT);
    assertThat(subject.getRetries()).isEqualTo(RETRIES);
  }

  @Test
  public void creatingATaskSpec_withADataSnapshotThatHasAnIntegerForALongValue_usesThatValueConvertedToALong() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, 1));
    assertThat(subject.getRetries()).isEqualTo(1L);
  }

  @Test
  public void creatingATaskSpec_withADataSnapshotThatHasAFloatForALongValue_usesThatValueConvertedToALong() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, 1.2F));
    assertThat(subject.getRetries()).isEqualTo(1L);
  }

  @Test
  public void creatingATaskSpec_withADataSnapshotThatHasADoubleForALongValue_usesThatValueConvertedToALong() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, 1.2D));
    assertThat(subject.getRetries()).isEqualTo(1L);
  }

  @Test
  public void ifStartStateEqualsInProgressState_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, START_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifFinishedStateEqualsStartState_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, START_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifFinishedStateEqualsInProgressState_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, IN_PROGRESS_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifErrorStateEqualsInProgressState_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, IN_PROGRESS_STATE, TIMEOUT, RETRIES));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifTimeoutIsLessThan1_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, 0L, RETRIES));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifRetriesIsLessThan0_validateReturnsFalse() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, -1L));
    assertThat(subject.validate()).isFalse();
  }

  @Test
  public void ifAllValuesAreValid_validateReturnsTrue() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.validate()).isTrue();
  }

  @Test
  public void equalsAndHashCode_areImplemented() {
    EqualsVerifier.forClass(TaskSpec.class)
        .usingGetClass()
        .verify();
  }

  @Test
  public void toString_isImplemented() {
    TaskSpec subject = new TaskSpec(getTaskSpecSnapshot(START_STATE, IN_PROGRESS_STATE, FINISHED_STATE, ERROR_STATE, TIMEOUT, RETRIES));
    assertThat(subject.toString()).isEqualTo("TaskSpec{" +
        "startState='" + START_STATE + '\'' +
        ", inProgressState='" + IN_PROGRESS_STATE + '\'' +
        ", finishedState='" + FINISHED_STATE + '\'' +
        ", errorState='" + ERROR_STATE + '\'' +
        ", timeout=" + TIMEOUT +
        ", retries=" + RETRIES +
        '}');
  }
}
