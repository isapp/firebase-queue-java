package com.firebase.queue;

import java.util.UUID;

public class UuidUtils {
  private UuidUtils() {}

  public static String getUUID() {
    return UUID.randomUUID().toString();
  }
}
