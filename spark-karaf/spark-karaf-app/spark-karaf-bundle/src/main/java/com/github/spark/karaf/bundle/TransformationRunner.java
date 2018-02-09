package com.github.spark.karaf.bundle;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main Entry Point for Running Transformation
 * <p>
 * TODO Figure out why javax.annotations classes are throwing bundle wiring issues.
 * TODO Inject SparkContext so we can unit test and run in production.
 * TODO Limit running transformations on the executors; but still have Karaf instantiated.
 * <p>
 * Created by ccaspanello on 2/8/18.
 */
public class TransformationRunner {

  private static final Logger LOG = LoggerFactory.getLogger( TransformationRunner.class );
  private final BundleContext bundleContext;

  public TransformationRunner( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
  }

  //  @PostConstruct
  public void init() {
    LOG.info( "init()" );

    Dataset<Row> results = run( "", new HashMap<>() );
    results.show();

    shutdown();
  }

  //  @PreDestroy
  public void destroy() {
    LOG.info( "destroy()" );
  }

  private void shutdown() {
    try {
      bundleContext.getBundle( 0 ).stop();
    } catch ( BundleException e ) {
      LOG.error( "Unexpected error", e );
      throw new RuntimeException( e );
    }
  }

  private Dataset<Row> run( String transformationFile, Map<String, String> parameters ) {
    LOG.info( "Run some stuff" );

    SparkConf sparkConf = new SparkConf();
    sparkConf.set( "spark.driver.host", "localhost" );

    SparkContext sc = new SparkContext( "local[*]", "SparkTest", sparkConf );
    SparkSession ss = new SparkSession( sc );

    // Schema
    StructType schema = new StructType( new StructField[] {
      new StructField( "key", DataTypes.StringType, false, Metadata.empty() ),
      new StructField( "value", DataTypes.IntegerType, false, Metadata.empty() )
    } );

    // Data
    List<Row> myList = new ArrayList<>();
    for ( int i = 0; i < 25; i++ ) {
      String key = "key" + i;
      int value = i;
      myList.add( RowFactory.create( key, value ) );
    }

    Dataset<Row> result = ss.createDataFrame( myList, schema );

    // TODO Do some more complicated transformations to test executors

    return result;
  }
}
