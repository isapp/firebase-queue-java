package com.firebase.queue;

import com.firebase.client.Firebase;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
    UuidUtils.class
)
public class QueueTaskTest {
  private static final String TASK_ID = "thread:uuid";
  private static final String TASK_KEY = "task_key";

  private Log.Logger logger;

  private Firebase taskRef;
  private TaskSpec taskSpec;
  private TaskReset taskReset;
  private Queue.Options options;

  private TaskClaimer taskClaimer;
  private ValidityChecker validityChecker;

  private QueueTask subject;

  @Before
  public void setUp() throws Exception {
    // clear the interrupt flag if it is set
    if(Thread.currentThread().isInterrupted()) {
      try {Thread.sleep(1);} catch(InterruptedException ignore) {}
    }

    mockStatic(UuidUtils.class);
    when(UuidUtils.getUUID()).thenReturn("uuid");
    Thread.currentThread().setName("thread");

    logger = mock(Log.Logger.class);
    Log.setLogger(logger);

    taskRef = mock(Firebase.class);
    stub(taskRef.getKey()).toReturn(TASK_KEY);

    taskSpec = mock(TaskSpec.class);
    taskReset = mock(TaskReset.class);
    options = new Queue.Options(mock(Queue.Builder.class));

    taskClaimer = mock(TaskClaimer.class);
    validityChecker = mock(ValidityChecker.class);

    subject = spy(new QueueTask(taskRef, taskSpec, taskReset, options));

    doReturn(taskClaimer)
        .when(subject).getTaskClaimer(TASK_ID, taskRef, taskSpec, taskReset, options);

    doReturn(validityChecker)
        .when(subject).getValidityChecker(TASK_ID);
  }

  @Test
  public void getTaskKey_returnsTheFirebaseRefKey() {
    assertThat(subject.getTaskKey()).isEqualTo(TASK_KEY);
  }

  @Test
  public void cancel_beforeRun_calledTwice_writesALog() {
    subject.cancel();
    subject.cancel();

    verify(logger).log("Not cancelling task (" + taskRef.getKey() + ") on " + QueueTask.PRE_RUN_ID + " because it was already cancelled", Log.Level.INFO);
  }

  @Test
  public void cancel_beforeRun_calledTwice_keepsTheTaskCancelled() {
    assertThat(subject.isCancelled()).isFalse();
    subject.cancel();
    assertThat(subject.isCancelled()).isTrue();
    subject.cancel();
    assertThat(subject.isCancelled()).isTrue();
  }

  @Test
  public void cancel_afterSuccessfulRun_writesALog() {
    doSuccessfulRun();

    reset(logger);

    subject.cancel();

    verify(logger).log("Not cancelling task (" + taskRef.getKey() + ") on " + TASK_ID + " because it is already done", Log.Level.INFO);
  }

  @Test
  public void cancel_afterSuccessfulRun_writesALog_doesNotCancelTheTask() {
    assertThat(subject.isCancelled()).isFalse();
    assertThat(subject.isDone()).isFalse();
    doSuccessfulRun();
    assertThat(subject.isCancelled()).isFalse();
    assertThat(subject.isDone()).isTrue();
    subject.cancel();
    assertThat(subject.isCancelled()).isFalse();
    assertThat(subject.isDone()).isTrue();
  }

  @Test
  public void cancel_beforeRun_causesIsCancelledToReturnTrue() {
    subject.cancel();
    assertThat(subject.isCancelled()).isTrue();
  }

  @Test
  public void cancel_beforeRun_writesALog() {
    subject.cancel();
    verify(logger).log("Delaying cancelling task (" + taskRef.getKey() + ") on " + QueueTask.PRE_RUN_ID + " because it hasn't started running yet", Log.Level.INFO);
  }

  @Test
  public void cancel_beforeRun_doesNotInterruptTheExecutingThread() {
    subject.cancel();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test()
  public void cancel_duringRun_interruptsTheExecutingThread() {
    final Thread executingThread = mock(Thread.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        stub(executingThread.isInterrupted()).toReturn(true);
        return null;
      }
    }).when(executingThread).interrupt();
    Whitebox.setInternalState(subject, "executingThread", executingThread);
    Whitebox.setInternalState(subject, "id", TASK_ID);
    subject.cancel();

    assertThat(executingThread.isInterrupted()).isTrue();
  }

  @Test()
  public void cancel_duringRun_writesALog() {
    Whitebox.setInternalState(subject, "executingThread", mock(Thread.class));
    Whitebox.setInternalState(subject, "id", TASK_ID);
    subject.cancel();

    verify(logger).log("Cancelling task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void beforeRun_getIdReturnsNull() {
    assertThat(subject.getId()).isNull();
  }

  @Test
  public void afterRun_getIdReturnsTheId() {
    subject.run();
    assertThat(subject.getId()).isEqualTo(TASK_ID);
  }

  @Test
  public void callingRun_ifCancelled_writesALog() {
    subject.cancel();
    subject.run();

    verify(logger).log("Can't run task (" + taskRef.getKey() + ") on " + TASK_ID +  " because it has previously been cancelled", Log.Level.INFO);
  }

  @Test
  public void callingRun_ifCancelled_doesNotInteractWithTheTaskClaimer() {
    subject.cancel();
    subject.run();

    verifyZeroInteractions(taskClaimer);
  }

  @Test
  public void callingRun_writesALog() {
    subject.run();

    verify(logger).log("Started claiming task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void callingRun_triesToClaimATask() {
    subject.run();

    verify(taskClaimer).claimTask();
  }

  @Test
  public void callingRun_butCouldNotClaimATask_writesALog() {
    stub(taskClaimer.claimTask()).toReturn(null);

    subject.run();

    verify(logger).log("Couldn't claim task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void callingRun_butCouldNotClaimATask_setsDoneToTrue() {
    stub(taskClaimer.claimTask()).toReturn(null);

    subject.run();

    assertThat(subject.isDone()).isTrue();
  }

  @Test
  public void callingRun_butCouldNotClaimATask_doesNotSetClaimedToTrue() {
    stub(taskClaimer.claimTask()).toReturn(null);

    subject.run();

    assertThat(subject.isClaimed()).isFalse();
  }

  @Test
  public void callingRun_andClaimedATask_writesALog() {
    doSuccessfulRun();

    verify(logger).log("Claimed task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void callingRun_andClaimedATask_butWasCancelled_writesALog() {
    doAnswer(new Answer<TaskClaimer.TaskGenerator>() {
      @Override
      public TaskClaimer.TaskGenerator answer(InvocationOnMock invocation) throws Throwable {
        subject.cancel();
        return mock(TaskClaimer.TaskGenerator.class);
      }
    }).when(taskClaimer).claimTask();

    subject.run();

    verify(logger).log("Can't process task (" + taskRef.getKey() + ") on " + TASK_ID + " because it was cancelled while we were claiming it", Log.Level.INFO);
  }

  @Test
  public void callingRun_andClaimedATask_butWasCancelled_doesNotSetClaimedToTrue() {
    doAnswer(new Answer<TaskClaimer.TaskGenerator>() {
      @Override
      public TaskClaimer.TaskGenerator answer(InvocationOnMock invocation) throws Throwable {
        subject.cancel();
        return mock(TaskClaimer.TaskGenerator.class);
      }
    }).when(taskClaimer).claimTask();

    subject.run();

    assertThat(subject.isClaimed()).isFalse();
  }

  @Test
  public void callingRun_andClaimedATask_butWasCancelled_setsDoneToTrue() {
    doAnswer(new Answer<TaskClaimer.TaskGenerator>() {
      @Override
      public TaskClaimer.TaskGenerator answer(InvocationOnMock invocation) throws Throwable {
        subject.cancel();
        return mock(TaskClaimer.TaskGenerator.class);
      }
    }).when(taskClaimer).claimTask();

    subject.run();

    assertThat(subject.isDone()).isTrue();
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_setsClaimedToTrue() {
    doSuccessfulRun();

    assertThat(subject.isClaimed()).isTrue();
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_writesAStartedLog() {
    doSuccessfulRun();

    verify(logger).log("Started processing task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_generatesATask() {
    TaskClaimer.TaskGenerator taskGenerator = mock(TaskClaimer.TaskGenerator.class);
    stub(taskGenerator.generateTask(TASK_ID, taskSpec, taskReset, validityChecker, options)).toReturn(mock(Task.class));
    stub(taskClaimer.claimTask()).toReturn(taskGenerator);
    subject.run();

    verify(taskGenerator).generateTask(TASK_ID, taskSpec, taskReset, validityChecker, options);
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_processesTheTask() {
    TaskClaimer.TaskGenerator taskGenerator = mock(TaskClaimer.TaskGenerator.class);
    Task task = mock(Task.class);
    stub(taskGenerator.generateTask(TASK_ID, taskSpec, taskReset, validityChecker, options)).toReturn(task);
    stub(taskClaimer.claimTask()).toReturn(taskGenerator);

    TaskProcessor taskProcessor = mock(TaskProcessor.class);
    Whitebox.setInternalState(options, "taskProcessor", taskProcessor);

    subject.run();

    verify(task).process(taskProcessor);
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_setsDoneToTrue() {
    doSuccessfulRun();

    assertThat(subject.isDone()).isTrue();
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_destroysTheValidityChecker() {
    doSuccessfulRun();

    verify(validityChecker).destroy();
  }

  @Test
  public void callingRun_andClaimedATask_andWasNotCancelled_writesAFinishedLog() {
    doSuccessfulRun();

    verify(logger).log("Finished processing task (" + taskRef.getKey() + ") on " + TASK_ID, Log.Level.INFO);
  }

  @Test
  public void equalsAndHashCode_areImplemented() {
    Firebase taskRefRed = mock(Firebase.class);
    stub(taskRefRed.getKey()).toReturn("red");
    Firebase taskRefBlack = mock(Firebase.class);
    stub(taskRefBlack.getKey()).toReturn("black");

    Thread threadRed = new Thread();
    threadRed.setName("red");
    Thread threadBlack = new Thread();
    threadBlack.setName("black");
    EqualsVerifier.forClass(QueueTask.class)
        .usingGetClass()
        .withPrefabValues(Firebase.class, taskRefRed, taskRefBlack)
        .withPrefabValues(Thread.class, threadRed, threadBlack)
        .suppress(Warning.NONFINAL_FIELDS, Warning.REFERENCE_EQUALITY) // library acting weird...not sure why it think we use == for equality
        .verify();
  }

  @Test
  public void toString_isImplemented() {
    assertThat(subject.toString()).isEqualTo(
        "QueueTask{" +
        "id='" + null + "', " +
        "task='" + taskRef.getKey() + '\'' +
        '}');
  }

  private void doSuccessfulRun() {
    TaskClaimer.TaskGenerator taskGenerator = mock(TaskClaimer.TaskGenerator.class);
    stub(taskGenerator.generateTask(TASK_ID, taskSpec, taskReset, validityChecker, options)).toReturn(mock(Task.class));
    stub(taskClaimer.claimTask()).toReturn(taskGenerator);
    subject.run();
  }
}
