package com.github.spark.karaf.delegate;

/**
 * Created by ccaspanello on 2/8/18.
 */
public class SparkMain {

  public static void main(String[] args){

    String karafHome = "/Users/ccaspanello/Desktop/spark-testing/spark-karaf/spark-karaf-app/spark-karaf-assembly/target/assembly";

    args = new String[]{"-rt",""};
    KarafContainer karafContainer = new KarafContainer(karafHome);
    karafContainer.launch(args);

    karafContainer.awaitShutdown();
    System.exit(0);
  }

}
