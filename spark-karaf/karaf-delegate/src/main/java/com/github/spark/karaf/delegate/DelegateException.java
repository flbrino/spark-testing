package com.github.spark.karaf.delegate;

/**
 * Created by ccaspanello on 2/8/18.
 */
public class DelegateException extends RuntimeException {
  public DelegateException( String message, Throwable throwable ) {
    super( message, throwable );
  }
}

