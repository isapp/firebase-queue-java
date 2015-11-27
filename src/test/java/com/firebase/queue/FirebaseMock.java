package com.firebase.queue;

import com.firebase.client.ChildEventListener;
import com.firebase.client.Firebase;
import com.firebase.client.Query;
import com.firebase.client.ValueEventListener;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.when;

public final class FirebaseMock {
  private final Firebase root;
  private final Firebase tasks;
  private final Firebase spec;
  private final Firebase specIdRef;

  private final Query tasksQuery;
  private final Query timeoutQuery;

  private ChildEventListener tasksChildEventListener;
  private ChildEventListener timeoutChildEventListener;

  private ValueEventListener specIdValueEventListener;

  public FirebaseMock() {
    root = mock(Firebase.class);
    tasks = mock(Firebase.class);
    spec = mock(Firebase.class);
    specIdRef = mock(Firebase.class);

    tasksQuery = mock(Query.class);
    timeoutQuery = mock(Query.class);

    specIdValueEventListener = mock(ValueEventListener.class);

    stub(root.child(Queue.TASK_CHILD)).toReturn(tasks);
    setupQueryChildEventListenerAnswers();

    stub(root.child(Queue.SPEC_CHILD)).toReturn(spec);
    stub(spec.child(anyString())).toReturn(specIdRef);
    setupSpecIdValueEventListenerAnswers();
  }

  public Firebase getRoot() {
    return root;
  }

  public Firebase getTasks() {
    return tasks;
  }

  public Firebase getSpec() {
    return spec;
  }

  public Firebase getSpecIdRef() {
    return specIdRef;
  }

  public Query getTasksQuery() {
    return tasksQuery;
  }

  public Query getTimeoutQuery() {
    return timeoutQuery;
  }

  public ChildEventListener getTasksChildEventListener() {
    return tasksChildEventListener;
  }

  public ChildEventListener getTimeoutChildEventListener() {
    return timeoutChildEventListener;
  }

  public ValueEventListener getSpecIdValueEventListener() {
    return specIdValueEventListener;
  }

  public void restubQueries(TaskSpec taskSpec) {
    Query startStateQuery = mock(Query.class);
    stub(tasks.orderByChild(Task.STATE_KEY)).toReturn(startStateQuery);

    Query limitQuery = mock(Query.class);
    stub(startStateQuery.equalTo(taskSpec.getStartState())).toReturn(limitQuery);
    stub(limitQuery.limitToFirst(1)).toReturn(tasksQuery);

    stub(startStateQuery.equalTo(taskSpec.getInProgressState())).toReturn(timeoutQuery);
  }

  @SuppressWarnings("Duplicates")
  private void setupQueryChildEventListenerAnswers() {
    when(tasksQuery.addChildEventListener(Matchers.<ChildEventListener>any())).then(new Answer<ChildEventListener>() {
      @Override
      public ChildEventListener answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        tasksChildEventListener = (ChildEventListener) args[0];
        return tasksChildEventListener;
      }
    });

    when(timeoutQuery.addChildEventListener(Matchers.<ChildEventListener>any())).then(new Answer<ChildEventListener>() {
      @Override
      public ChildEventListener answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        timeoutChildEventListener = (ChildEventListener) args[0];
        return timeoutChildEventListener;
      }
    });

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ChildEventListener childEventListener = (ChildEventListener) args[0];
        if(childEventListener == tasksChildEventListener) {
          tasksChildEventListener = null;
        }
        return null;
      }
    }).when(tasksQuery).removeEventListener(Matchers.<ChildEventListener>any());

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ChildEventListener childEventListener = (ChildEventListener) args[0];
        if(childEventListener == timeoutChildEventListener) {
          timeoutChildEventListener = null;
        }
        return null;
      }
    }).when(timeoutQuery).removeEventListener(Matchers.<ChildEventListener>any());
  }

  private void setupSpecIdValueEventListenerAnswers() {
    when(specIdRef.addValueEventListener(Matchers.<ValueEventListener>any())).then(new Answer<ValueEventListener>() {
      @Override
      public ValueEventListener answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        specIdValueEventListener = (ValueEventListener) args[0];
        return specIdValueEventListener;
      }
    });

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ValueEventListener valueEventListener = (ValueEventListener) args[0];
        if(valueEventListener == specIdValueEventListener) {
          specIdValueEventListener = null;
        }
        return null;
      }
    }).when(specIdRef).removeEventListener(Matchers.<ValueEventListener>any());
  }
}
