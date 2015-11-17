# firebase-queue-java

A fault-tolerant, multi-worker, multi-stage job pipeline built on the Firebase realtime database.


## Purpose of a Queue

Queues can be used in your Firebase app to organize workers or perform background work like generating thumbnails of images, filtering message contents and censoring data, or fanning data out to multiple locations in your Firebase database. First, let's define a few terms we'll use when talking about a queue:
  - `task` - a unit of work that a queue worker can process
  - `spec` - a definition of an operation that the queue will perform on matching tasks
  - `job` - one of more `spec`'s that specify a series of ordered operations to be performed
  - `worker` - an individual process that picks up tasks with a certain spec and processes them

Let's take a look at a simple example to see how this works. Imagine you wanted to build a chat application that does two things:
  1. Sanitize chat message input
  2. Fan data out to multiple rooms and users

Since chat message sanitization can't happen purely on the client side, as that would allow a malicious client to circumvent client side restrictions, you'll have to run this process on a trusted server process.

Using Firebase Queue, you can create specs for each of these tasks, and then use workers to process the individual tasks to complete the job. We'll explore the queue, adding tasks, assigning workers, and creating custom specs to create full jobs, then [revisit the example](#message-sanitization-revisited) above.

## The Queue in Your Firebase Database

The queue relies on having a Firebase database reference to coordinate workers e.g. `https://<your-firebase>.firebaseio.com/queue`. This queue can be stored at any path in your Firebase database, and you can have multiple queues as well. The queue will respond to tasks pushed onto the `tasks` subtree and optionally read specifications from a `specs` subtree.
```
queue
  - specs
  - tasks
```


## Queue Workers

The basic unit of the queue is the queue worker: the process that claims a task, performs the appropriate processing on the data, and either returns the transformed data, or an appropriate error.

You can start a worker process by passing in a Firebase database  [`ref`](https://www.firebase.com/docs/web/guide/understanding-data.html#section-creating-references) along with a `TaskProcessor` ([described below](#the-task-processor)), as follows:

```java
// MyQueueWorker.java

Firebase ref = new Firebase("https://<your-firebase>.firebaseio.com/queue");

Queue queue = new Queue.Builder(ref, new TaskProcessor() {
    @Override
    public void process(Task task) throws InterruptedException {
        System.out.println(task.getData());
        
        try {
            Thread.sleep(2500);
        }
        catch(InterruptedException e) {
             throw e;
        }
        
        task.resolve(new Task.Listener() {
            @Override
            public void onSuccess() {
                System.out.println("resolved successfully");
            }
            
            @Override
            public void onFailure(@NotNull String error, boolean canRetry) {
                System.out.println("did not resolve successfully " + error);
                
                if(canRetry) {
                    task.resolve(this);
                }
            }
        });
    }
}).build();
```

Multiple queue workers can be initialized on multiple machines and Firebase-Queue will ensure that only one worker is processing a single queue task at a time.


#### Queue Builder Options (Optional)

`Queue.Builder` can take optional options to specify:
  - `specId` - specifies the spec type for this worker. This is important when creating multiple specs. Defaults to `null` which uses the default spec
  - `numWorkers` - specifies the number of worker threads that will be kept in a thread pool. Defaults to 1 worker
  - `sanitize` - specifies whether the `data` object passed to the processing function is sanitized of internal keys reserved for use by the queue. Defaults to `true`
  - `suppressStack` - specifies whether the queue will suppress error stack traces from being placed in the `_error_details` of the task if it's rejected with a `Throwable`

```java
Queue queue = new Queue.Builder(ref, taskProcessor)
                                    .specId("spec_1")
                                    .numWorkers(5)
                                    .sanitize(false)
                                    .suppressStack(true)
                                    .build();
```

##### Custom Executors (Optional)

By default, a fixed size thread pool will be used to process tasks, having `numWorkers` threads in it.
If you want more control over the the underlying thread pool, you can pass a `QueueExecutor.Factory` to a Queue Builder using `executorFactory`.
`numWorkers` and `executorFactory` cannot both be called on one `Queue.Builder` (doing so will result in an `Exception` being thrown).

```java
Queue queue = new Queue.Builder(ref, taskProcessor)
                        .specId("spec_1")
                        .executorFactory(new QueueExecutor.Factory() {
                            @Override
                            public QueueExecutor get() {
                                QueueExecutor executor = new QueueExecutor(5, 10, new LinkedBlockingQueue<Runnable>());
                                executor.prestartAllCoreThreads();
                                executor.allowCoreThreadTimeOut(true);
                                executor.setKeepAliveTime(10, TimeUnit.MINUTES);
                                return executor;
                            }
                        })
                        .sanitize(false)
                        .suppressStack(true)
                        .build();
```


## Pushing Tasks Onto the Queue

Using any Firebase client or the REST API, push an object with some data to the `tasks` subtree of your queue. Queue workers listening on that subtree will automatically pick up and process the new task.

```shell
# Using curl in shell
curl -X POST -d '{"foo": "bar"}' https://<your-firebase>.firebaseio.com/queue/tasks.json
```
or
```js
// Firebase Javascript Client
var Firebase = require('firebase');

var ref = new Firebase('https://<your-firebase>.firebaseio.com/queue/tasks');
ref.push({'foo': 'bar'});
```
or
```java
// Firebase Java Client
Map<String, Object> data = new HashMap<String, Object>() {{
    put("foo", "bar");
}};

Firebase ref = new Firebase("https://<your-firebase>.firebaseio.com/queue/tasks");
ref.push().setValue(data);
```

### Starting Tasks in Specific States (Optional)

When using a custom spec, you can pass a `_state` key in with your object, which will allow a custom spec's worker(s) to pick up your task at a specific spec, rather than starting with the starting spec.

```js
{
  "foo": "bar",
  "boo": "baz",
  "_state": "spec_n_start"
}
```


## The Task Processor

The `TaskProcessor` provides a callback that will be invoked when a task is ready to be processed. As this callback defines the work that the worker must do, it is a required parameter. It is provided with a `Task` which allows for completing tasks successfully, with error conditions, or abandoning the task using the following methods:

#### `getData()`

A `Map<String, Object>` containing the claimed task's data, which can contain any keys and values with the exception of several reserved keys, which are used for tracking worker progress.

The reserved keys are:
 - `_state` - The current state of the task. Will always be the task's `in_progress_state` when passed to the processing function.
 - `_state_changed` - The timestamp that the task changed into its current state. This will always be the server time when the processing function was called.
 - `_owner` - A unique ID for the worker and task number combination to ensure only one worker is responsible for the task at any time.
 - `_error_details` - An object containing the error details from a previous task execution. If present, it may contain a `previous_state` string (or `null` if there was no previous state, in the case of malformed input) capturing the state the task was in when it errored, an `error` string from the `reject()` callback of the previous task, and an `attempts` field containing the number of retries attempted before failing a task. If the `suppressStack` queue option is not set to `true`, there may also be a `error_stack` field containing a stack trace of any `Throwable` passed into the `reject()` function.

 By default the data is sanitized of these keys, but you can disable this behavior by calling `.sanitize(false)` on the [`QueueBuilder`](#queue-builder-options-optional).

#### `resolve()`

A method for reporting that the current task has been completed and the worker is ready to process another task. If the current task specification has a `finished_state`, a `Map` passed into the `resolve()` function will be written to the `tasks` location and will be available to the next task if the tasks are chained.

#### `reject()`

A method for reporting that the current task failed and the worker is ready to process another task. Once this is called, the task will go into the `error_state` for the job with an additional `_error_details` object containing a `previous_state` key referencing this task's `in_progress_state`. If a string is passed into the `reject()` function, the `_error_details` will also contain an `error` key containing that string. If a `Throwable` is passed into the `reject()` function, the `error` key will contain the `Throwable.getMessage()`, and if `suppressStack` option has not been specified the `error_stack` key will contain the `Throwable`'s stack trace. Note that if retries are enabled and there are remaining attempts, the task will be restarted in it's spec's `start_state`.

#### `abort()`

A method for reporting that the current task is being abandoned and the worker is ready to process another task. Once this is called, the task's `_state` will be set to the `start_state`, `_state_changed` will be set to the current server time, `_owner` will be set to `null`, and `_error_details` will be set to `null`. This task will then be eligible to be picked up by any worker (including the one that aborted it).

#### `isCancelled`

A method for reporting whether the current task has been cancelled. A task becomes cancelled when:

  - It times out
  - The queue was shutdown
  - `resolve`, `reject`, or `abort` fails in a way that can't be retried
  
#### `isFinished`

A method for reporting whether the current task has been finished. A task becomes finished when:

  - The task has been successfully resolved, rejected, or aborted
  - The task is cancelled
  
### `Task.Listener`

A `Task.Listener` can be supplied to any one of `resolve`, `reject`, or `abort`. If `onSuccess` is invoked, the action completed successfully. If `onFailure` is invoked, a human readable error will be present in the `error` parameter. The `canRetry` parameter indicates whether the action could be retried. Typically, if it is `false` `TaskProcessor.process` should be exited as soon as possible.

### `InterruptedException`

If an `InterruptedException` is raised at any point during `TaskProcessor.process`'s execution, it should be bubbled up until it is thrown from `TaskProcessor.process`. No `InterruptedException` should ever be swallowed in this context.

## Queue Security

Securing your queue is an important step in securely processing events that come in. Below is a sample set of security rules that can be tailored to your particular use case.

In this example, there are three categories of users, represented using fields of a [custom token](https://www.firebase.com/docs/rest/guide/user-auth.html):
- `auth.canAddTasks`: Users who can add tasks to the queue (could be an authenticated client or a secure server)
- `auth.canProcessTasks`: Users who can process tasks (usually on a secure server)
- `auth.canAddSpecs`: Users who can create and view task specifications (usually on a secure server)

These don't have to use a custom token, for instance you could use `auth != null` in place of `auth.canAddTasks` if application's users can write directly to the queue. Similarly, `auth.canProcessTasks` and `auth.canAddSpecs` could be `auth.admin === true` if a single trusted server process was used to perform all queue functions.

```js
{
  "rules": {
    "queue": {
      "tasks": {
        ".read": "auth.canProcessTasks",
        ".write": "auth.canAddTasks || auth.canProcessTasks",
        ".indexOn": "_state",
        "$taskId": {
          ".validate": "newData.hasChildren(['property_1', ..., 'property_n'])
                        || (auth.canProcessTasks
                        && newData.hasChildren(['_state', '_state_changed', '_progress']))",
          "_state": {
            ".validate": "newData.isString()"
          },
          "_state_changed": {
            ".validate": "newData.isNumber() && (newData.val() === now
                          || data.val() === newData.val())"
          },
          "_owner": {
            ".validate": "newData.isString()"
          },
          "_progress": {
            ".validate": "newData.isNumber()
                          && newData.val() >= 0
                          && newData.val() <= 100"
          },
          "_error_details": {
              "error": {
                ".validate": "newData.isString()"
              },
              "error_stack": {
                ".validate": "newData.isString()"
              },
              "previous_state": {
                ".validate": "newData.isString()"
              },
              "original_task": {
                /* This space intentionally left blank, for malformed tasks */
              },
              "attempts": {
                ".validate": "newData.isNumber() && newData.val() > 0"
              },
              "$other": {
                ".validate": false
              }
          },
          "property_1": {
            ".validate": "/* Insert custom data validation code here */"
          },
          ...
          "property_n": {
            ".validate": "/* Insert custom data validation code here */"
          }
        }
      },
      "specs" : {
        ".read": "auth.canAddSpecs || auth.canProcessTasks",
        ".write": "auth.canAddSpecs",
        "$specId": {
          ".validate": "newData.hasChild('in_progress_state')",
          "start_state": {
            ".validate": "newData.isString()"
          },
          "in_progress_state": {
            ".validate": "newData.isString()"
          },
          "finished_state": {
            ".validate": "newData.isString()"
          },
          "error_state": {
            ".validate": "newData.isString()"
          },
          "timeout": {
            ".validate": "newData.isNumber() && newData.val() > 0"
          },
          "$other": {
            ".validate": false
          }
        }
      }
    }
  }
}
```

## Defining Specs (Optional)

#### Default Spec

A default spec configuration is assumed if no specs are specified in the `specs` subtree of the queue. The default spec has the following characteristics:

```js
{
  "default_spec": {
    "start_state": null,
    "in_progress_state": "in_progress",
    "finished_state": null,
    "error_state": "error",
    "timeout": 300000, // 5 minutes
    "retries": 0 // don't retry
  }
}
```

- `start_state` - The default spec has no `start_state`, which means any task pushed into the `tasks` subtree without a `_state` key will be picked up by default spec workers. If `start_state` is specified, only tasks with that `_state` may be claimed by the worker.
- `in_progress_state` - When a worker picks up a task and begins processing it, it will change the tasks's `_state` to the value of `in_progress_state`. This is the only required spec property, and it cannot equal the `start_state`, `finished_state`, or `error_state`.
- `finished_state` - The default spec has no `finished_state` so the worker will remove tasks from the queue upon successful completion. If `finished_state` is specified, then the task's `_state` value will be updated to the `finished_state` upon task completion. Setting this value to another spec's `start_state` is useful for chaining tasks together to create a job.
- `error_state` - If the task gets rejected the `_state` will be updated to this value and an additional key `_error_details` will be populated with the `previous_state` and an optional error message from the `reject()` callback. If this isn't specified, it defaults to "error". This can be useful for specifying different error states for different tasks, or chaining errors so that they can be logged.
- `timeout` - The default timeout is 5 minutes. When a task has been claimed by a worker but has not completed within `timeout` milliseconds, the queue will report that task as timed out, and reset that task to be claimable once again. If this is not specified, a task claimed by a worker could be orphaned and left in an unclaimable state if the worker processing it dies before the task is resolved or rejected.
- `retries` - The default spec doesn't retry failed tasks. When a task fails, if there are any remaining attempts, the queue will restart the task by setting the task's `_state` to its spec's `start_state`.

#### Creating Jobs using Custom Specs and Task Chaining

In order to use a job specification other than the default, the specification must be defined in the Firebase under the `specs` subtree. This allows us to coordinate job specification changes between workers and enforce expected behavior with Firebase security rules.

In this example, we're chaining three specs to make a job. New tasks pushed onto the queue without a `_state` key will be picked up by "spec_1" and go into the `spec_1_in_progress` state. Once "spec_1" completes and the task goes into the `spec_1_finished` state, "spec_2" takes over and puts it into the `spec_2_in_progress` state. Again, once "spec_2" completes and the task goes into the `spec_2_finished` state, "spec_3" takes over and puts it into the `spec_3_in_progress` state. Finally, "spec_3" removes it once complete. If, during any stage in the process there's an error, the task will end up in an "error" state.

```
queue
  - specs
```
```json
{
  "spec_1": {
    "in_progress_state": "spec_1_in_progress",
    "finished_state": "spec_1_finished",
    "timeout": 5000
  },
  "spec_2": {
    "start_state": "spec_1_finished",
    "in_progress_state": "spec_2_in_progress",
    "finished_state": "spec_2_finished",
    "timeout" : 20000
  },
  "spec_3": {
    "start_state": "spec_2_finished",
    "in_progress_state": "spec_3_in_progress",
    "timeout": 3000
  }
}
```

## Graceful Shutdown

Once initialized, a queue can be gracefully shutdown by calling its `shutdown()` method. This prevents workers from claiming new tasks, removes all Firebase listeners, and shuts down the thread pool. It will not wait for current tasks to be finished, but an `InterruptedException` will be raised for that task.

## Message Sanitization, Revisited

In our example at the beginning, you wanted to perform several actions on your chat system:
  1. Sanitize chat message input
  2. Fan data out to multiple rooms and users

Together, these two actions form a job, and you can use custom specs, as shown above, to define the flow of tasks in this job. When you start, your Firebase should look like this:

```
root
  - queue
    - specs
      - sanitize_message
        - in_progress_state: "sanitize_message_in_progress"
        - finished_state: "sanitize_message_finished"
      - fanout_message
        - start_state: "sanitize_message_finished"
        - in_progress_state: "fanout_message_in_progress"
        - error_state: "fanout_message_failed"
        - retries: 3
    - tasks
      /* null, no data */
```

Let's imagine that you have some front end that allows your users to write their name and a message, and send that to your queue as it's `data`. Let's assume your user writes something like the following:

```java
// ChatClient.java

Map<String, Object> chat = new HashMap<String, Object>() {{
    put("message", "Hello Firebase Queue Users");
    put("name", "Chris");
}};

Firebase tasksRef = new Firebase("https://<your-firebase>.firebaseio.com/queue/tasks");
tasksRef.push().setValue(chat);
```

Your Firebase database should now look like this:

```
root
  - queue
    - specs
      /* same as above */
    - tasks
      - $taskId
        - message: "Hello Firebase Queue Users!"
        - name: "Chris"
```

When your users push `data` like the above into the `tasks` subtree, tasks will initially start in the `sanitize_message` spec because the task has no `start_state`. The associated queue can be specified using the following processing function:

```java
// ChatMessageSanitization.java

Firebase ref = new Firebase("https://<your-firebase>.firebaseio.com");
Firebase queueRef = ref.child("queue");
Firebase messagesRef = ref.child("messages");

Queue sanitizeQueue = new Queue.Builder(queueRef, new TaskProcessor() {
    @Override
    public void process(Task task) {
        Map<String, Object> data = task.getData();
        data.put("message", sanitize((String) data.get("message")));
        task.resolve(data);
    }
})
.specId("sanitize_message")
.build();

...
```

The queue worker will take this task, begin to process it, and update the reserved keys of the task:

```
root
  - queue
    - specs
      /* same as above */
    - tasks
      - $taskId
        - _owner: $workerUid
        - _progress: 0
        - _state: "sanitize_message_in_progress"
        - _state_changed: 1431475215737
        - message: "Hello Firebase Queue Users!"
        - name: "Chris"
```

Once the message is sanitized, it will be resolved and both the reserved keys and the data will be updated in the task (imagine for a minute that queue is a blacklisted word):

```
root
  - queue
    - specs
      /* same as above */
    - tasks
      - $taskId
        - _owner: null
        - _progress: 100
        - _state: "sanitize_message_finished"
        - _state_changed: 1431475215918
        - message: "Hello Firebase ***** Users!"
        - name: "Chris"
```

Now, you want to fan the data out to the `messages` subtree of your Firebase database, using the spec, `fanout_message`, so you can set up a second processing function to find tasks whose `_state` is `sanitize_message_finished`:

```java
...

Queue fanoutQueue = new Queue(tasksRef, new TaskProcessor() {
    @Override
    public void process(final Task task) {
        messagesRef.push().setValue(message, new CompletionListener() {
            @Override
            public void onComplete(FirebaseError error, Firebase firebase) {
                if(error != null) {
                    task.reject(error);
                }
                else {
                    task.resolve(task.getData());
                }
            }
        });
    }
})
.specId("fanout_message")
.numWorkers(5)
.build();
```

Since there is no `finished_state` in the `fanout_message` spec, the task will be purged from the queue after the data is fanned out to the messages node. If the `push` fails for some reason, the task will fail and retry, a maximum of three times (as specified in our spec).

While this example is a little contrived since you could perform the sanitization and fanout in a single task, creating multiple specs for our tasks allows us to do things like add selective retries to certain tasks more likely to fail, put additional workers on more expensive tasks, or add expressive error states.

## Wrap Up

As you can see, Firebase Queue is a powerful tool that allows you to securely and robustly perform background work on your Firebase data, from sanitization to data fanout and more. We'd love to hear about how you're using Firebase-Queue in your project! Let us know on [Twitter](https://twitter.com/firebase), [Facebook](https://www.facebook.com/Firebase), or [G+](https://plus.google.com/115330003035930967645). If you have any questions, please direct them to our [Google Group](https://groups.google.com/forum/#!forum/firebase-talk) or [support@firebase.com](mailto:support@firebase.com).