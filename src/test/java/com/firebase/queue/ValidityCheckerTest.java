package com.firebase.queue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class ValidityCheckerTest {
  private static final String TASK_ID = "task";
  private static final String OTHER_TASK_ID = "other_task_id";

  private ValidityChecker subject;

  @Before
  public void setUp() throws Exception {
    subject = new ValidityChecker(Thread.currentThread(), TASK_ID);
  }

  @Test
  public void checkingIfValidFromTheInstantiatingThreadWithTheOriginalTaskId_returnsTrue() {
    assertThat(subject.isValid(Thread.currentThread(), TASK_ID)).isTrue();
  }

  @Test
  public void checkingIfValidFromTheInstantiatingThreadWithADifferentTaskId_returnsFalse() {
    assertThat(subject.isValid(Thread.currentThread(), OTHER_TASK_ID)).isFalse();
  }

  @Test
  public void checkingIfValidFromADifferentThreadWithTheOriginalTaskId_returnsFalse() {
    assertThat(subject.isValid(new Thread(), TASK_ID)).isFalse();
  }

  @Test
  public void checkingIfValidFromADifferentThreadWithADifferentTaskId_returnsFalse() {
    assertThat(subject.isValid(new Thread(), OTHER_TASK_ID)).isFalse();
  }

  @Test
  public void checkingIfValidFromTheInstantiatingThreadWithTheOriginalTaskId_afterCallingDestroy_returnsFalse() {
    subject.destroy();
    assertThat(subject.isValid(Thread.currentThread(), TASK_ID)).isFalse();
  }
}
