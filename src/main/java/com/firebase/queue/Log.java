package com.firebase.queue;

import com.firebase.client.FirebaseError;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

public class Log {
  public interface Logger {
    void log(String log, Level level);
    void debug(String log, Level level);
    void log(String log, Throwable error);
    void debug(String log, Throwable error);
    void log(String log, FirebaseError error);
    void debug(String log, FirebaseError error);
    boolean shouldLogDebug(Level level);
  }

  public static boolean DEBUG = true;

  public enum Level {
    INFO, WARN, ERROR
  }

  private Log() {}

  private static Logger instance = new DefaultLogger();

  public static void setLogger(@NotNull Logger log) {
    instance = log;
  }

  public static void stopLogging() {
    if(!(instance instanceof NoOpLogger)) {
      instance = new NoOpLogger();
    }
  }

  public static void startLogging() {
    if(instance instanceof NoOpLogger) {
      instance = new DefaultLogger();
    }
  }

  public static void log(String log) {
    instance.log(log, Level.INFO);
  }

  public static void debug(String log) {
    if(instance.shouldLogDebug(Level.INFO)) {
      instance.debug(log, Level.INFO);
    }
  }

  public static void log(String log, Level level) {
    instance.log(log, level);
  }

  public static void debug(String log, Level level) {
    if(instance.shouldLogDebug(level)) {
      instance.debug(log, level);
    }
  }

  public static void log(String log, Throwable error) {
    instance.log(log, error);
  }

  public static void debug(String log, Throwable error) {
    if(instance.shouldLogDebug(Level.ERROR)) {
      instance.debug(log, error);
    }
  }

  public static void log(String log, FirebaseError error) {
    instance.log(log, error);
  }

  public static void debug(String log, FirebaseError error) {
    if(instance.shouldLogDebug(Level.ERROR)) {
      instance.debug(log, error);
    }
  }

  private static class DefaultLogger implements Logger {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_WHITE = "\u001B[37m";

    private final Date date = new Date();

    @Override
    public void log(String log, Level level) {
      System.out.println(getAnsiColor(level) + getDateTimeStamp() + getLabel(level) + log + ANSI_RESET);
    }

    @Override
    public void debug(String log, Level level) {
      System.out.println(getAnsiColor(level) + getDateTimeStamp() + " [DEBUG] " + log + ANSI_RESET);
    }

    @Override
    public void log(String log, Throwable error) {
      System.err.println(ANSI_RED + getDateTimeStamp() + " [ERROR] " + log);
      error.printStackTrace(System.err);
      System.err.println(ANSI_RESET);
    }

    @Override
    public void debug(String log, Throwable error) {
      System.err.println(ANSI_RED + getDateTimeStamp() + " [DEBUG] " + log);
      error.printStackTrace(System.err);
      System.err.println(ANSI_RESET);
    }

    @Override
    public void log(String log, FirebaseError error) {
      System.err.println(ANSI_RED + getDateTimeStamp() + " [ERROR] " + log);
      System.err.println("\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails());
      System.err.println(ANSI_RESET);
    }

    @Override
    public void debug(String log, FirebaseError error) {
      System.err.println(ANSI_RED + getDateTimeStamp() + " [DEBUG] " + log);
      System.err.println("\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails());
      System.err.println(ANSI_RESET);
    }

    @Override
    public boolean shouldLogDebug(Level level) {
      return DEBUG;
    }

    private String getDateTimeStamp() {
      synchronized (date) {
        date.setTime(System.currentTimeMillis());
        return date.toString();
      }
    }

    private String getAnsiColor(Level level) {
      switch(level) {
        case INFO:
          return ANSI_WHITE;
        case WARN:
          return ANSI_YELLOW;
        case ERROR:
          return ANSI_RED;
        default:
          return ANSI_RESET;
      }
    }

    private String getLabel(Level level) {
      switch(level) {
        case INFO:
          return " [INFO] ";
        case WARN:
          return " [WARN] ";
        case ERROR:
          return " [ERROR] ";
      }

      return " ";
    }
  }

  private static class NoOpLogger implements Logger {
    @Override public void log(String log, Level level) {}
    @Override public void debug(String log, Level level) {}
    @Override public void log(String log, Throwable error) {}
    @Override public void debug(String log, Throwable error) {}
    @Override public void log(String log, FirebaseError error) {}
    @Override public void debug(String log, FirebaseError error) {}
    @Override public boolean shouldLogDebug(Level level) {return false;}
  }
}
