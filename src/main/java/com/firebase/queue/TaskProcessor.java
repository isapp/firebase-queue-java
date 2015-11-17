package com.firebase.queue;

public interface TaskProcessor {
  void process(Task task) throws InterruptedException;
}
