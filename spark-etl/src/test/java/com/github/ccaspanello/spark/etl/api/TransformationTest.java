package com.github.ccaspanello.spark.etl.api;

import com.github.ccaspanello.spark.etl.AppContext;
import com.github.ccaspanello.spark.etl.GuiceExampleModule;
import com.github.ccaspanello.spark.etl.SampleTransformationGenerator;
import com.github.ccaspanello.spark.etl.gson.TransMetaConverter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertNotNull;

/**
 * Created by ccaspanello on 2/4/18.
 */
@Guice( modules = GuiceExampleModule.class )
public class TransformationTest {

  private static final Logger LOG = LoggerFactory.getLogger( TransformationTest.class );

  @Inject
  private AppContext appContext;

  private File output;

  @BeforeTest
  public void beforeTest() {
    try {
      output = File.createTempFile( "test-", "" );
      // Delete file since it is created automatically.
      output.delete();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unexpected error", e );
    }
  }

  @Test
  public void testFromFile() throws Exception {

    Map<String, String> parameters = new HashMap<>();
    parameters.put("outputDir", output.getAbsolutePath());

    File jsonFile = new File( this.getClass().getClassLoader().getResource( "sample.json" ).toURI() );
    TransMetaConverter transMetaConverter = new TransMetaConverter();
    TransMeta transMeta = transMetaConverter.fromJson( jsonFile );

    Transformation transformation = new Transformation( appContext, transMeta );
    transformation.injectParameters( parameters );
    transformation.execute();

    File[] files = new File(output,"sample").listFiles();
    assertNotNull( files );

    List<File> csvFiles = Arrays.stream( files )
      .filter( line -> line.getName().endsWith( ".csv" ) ).collect( Collectors.toList() );

    // TODO Work on assertion
    LOG.info( "OUTPUT - Files: {}", csvFiles.size() );
    LOG.info( "=========================" );
    for ( File file : csvFiles ) {
      List<String> lines = FileUtils.readLines( file );
      for ( String line : lines ) {
        LOG.info( "{}", line );
      }
    }
  }

  @Test
  public void testSimple() throws Exception {

    Map<String, String> parameters = new HashMap<>();
    parameters.put("outputDir", output.getAbsolutePath());

    TransMeta transMeta = SampleTransformationGenerator.simple();

    Transformation transformation = new Transformation( appContext, transMeta );
    transformation.injectParameters( parameters );
    transformation.execute();

    File[] files = new File(output,"simple").listFiles();
    assertNotNull( files );

    List<File> csvFiles = Arrays.stream( files )
      .filter( line -> line.getName().endsWith( ".csv" ) ).collect( Collectors.toList() );

    // TODO Work on assertion
    LOG.info( "OUTPUT - Files: {}", csvFiles.size() );
    LOG.info( "=========================" );
    for ( File file : csvFiles ) {
      List<String> lines = FileUtils.readLines( file );
      for ( String line : lines ) {
        LOG.info( "{}", line );
      }
    }
  }


  @DataProvider(name = "testData")
  public Object[][] testData() {

    SampleTransformationGenerator generator = new SampleTransformationGenerator();


    return new Object[][] {
      { "Simple", generator.simple() },
      //{ "Split", generator.split() },
      { "Combine", generator.combine() },
      //{ "Complex", generator.complex() }
    };
  }

  @Test(dataProvider = "testData")
  public void dataDrivenTest(String name, TransMeta transMeta) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("outputDir", output.getAbsolutePath());
    parameters.put("projectDir", this.getClass().getClassLoader().getResource( "" ).toString());

    Transformation transformation = new Transformation( appContext, transMeta );
    transformation.injectParameters( parameters );
    transformation.execute();
  }

}
