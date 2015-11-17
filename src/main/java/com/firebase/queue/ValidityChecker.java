package com.firebase.queue;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;

/*package*/ class ValidityChecker {
  private static final HashMap<Long, String> threadIdToTaskIdMap = new HashMap<Long, String>();

  private final Long id;

  public ValidityChecker(@NotNull Thread thread, @NotNull String taskId) {
    this.id = thread.getId();

    threadIdToTaskIdMap.put(id, taskId);
  }

  public boolean isValid(@NotNull Thread processingThread, @NotNull String taskId) {
    return taskId.equals(threadIdToTaskIdMap.get(processingThread.getId()));
  }

  public void destroy() {
    threadIdToTaskIdMap.remove(id);
  }
}
