package com.firebase.queue;

import com.firebase.client.Firebase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.Whitebox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class QueueBuilderTest {
  private QueueHelper queueHelper;

  @Before
  public void setUp() throws Exception {
    queueHelper = new QueueHelper(new FirebaseMock());
  }

  @SuppressWarnings("ConstantConditions")
  @Test(expected = Exception.class)
  public void creatingAQueueBuilder_withANullFirebase_throwsAnException() {
    new Queue.Builder(null, mock(TaskProcessor.class));
  }

  @SuppressWarnings("ConstantConditions")
  @Test(expected = Exception.class)
  public void creatingAQueueBuilder_withANullTaskProcessor_throwsAnException() {
    new Queue.Builder(mock(Firebase.class), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void creatingAQueueBuilder_andPassingANumberLessThan1ToNumWorkers_throwsAnException() {
    queueHelper.getNewBuilder().numWorkers(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void creatingAQueueBuilder_andSettingAnExecutorFactory_andThenSettingNumWorkers_throwsAnException() {
    queueHelper.getNewBuilder().executorFactory(mock(QueueExecutor.Factory.class)).numWorkers(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void creatingAQueueBuilder_andSettingNumWorkers_andThenSettingAnExecutorFactory_throwsAnException() {
    queueHelper.getNewBuilder().numWorkers(1).executorFactory(mock(QueueExecutor.Factory.class));
  }

  @Test
  public void creatingAQueueBuilder_andNotSettingAnExecutorFactory_usesADefaultExecutorFactory() {
    Queue queue = queueHelper.getNewBuilder().build();
    Object executor = Whitebox.getInternalState(queue, "executorFactory");
    assertThat(executor).isNotNull();
  }

  @Test(expected = IllegalStateException.class)
  public void callingBuildOnAQueueBuilderMoreThanOnce_throwsAnException() {
    Queue.Builder queueBuilder = queueHelper.getNewBuilder();
    queueBuilder.build();
    queueBuilder.build();
  }
}
