package com.github.ccaspanello.spark.etl.gson;

import com.github.ccaspanello.spark.etl.api.HopMeta;
import com.github.ccaspanello.spark.etl.api.TransMeta;
import com.github.ccaspanello.spark.etl.step.addsequence.AddSequenceMeta;
import com.github.ccaspanello.spark.etl.step.calculator.CalcFunction;
import com.github.ccaspanello.spark.etl.step.calculator.CalculatorMeta;
import com.github.ccaspanello.spark.etl.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.spark.etl.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.spark.etl.step.datagrid.Column;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGridMeta;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;

import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

/**
 * TransMetaConverter Test
 *
 * Created by ccaspanello on 1/29/18.
 */
public class TransMetaConverterTest {

  private static final Logger LOG = LoggerFactory.getLogger( TransMetaConverterTest.class );

  @Test
  public void test() {

    TransMetaConverter transConverter = new TransMetaConverter();

    TransMeta expected = testTransMeta();

    String json = transConverter.toJson( expected );
    TransMeta actual = transConverter.fromJson( json );

    assertReflectionEquals( actual, expected );

    LOG.info( "json: {}", json );
  }

  @Test
  public void test2(){
    TransMetaConverter transConverter = new TransMetaConverter();

    TransMeta expected = testTransMeta2();

    String json = transConverter.toJson( expected );
    TransMeta actual = transConverter.fromJson( json );

    assertReflectionEquals( actual, expected );

    LOG.info( "json: {}", json );

  }

  private TransMeta testTransMeta(){
    CsvInputMeta input = new CsvInputMeta( "Input" );
    CsvOutputMeta output = new CsvOutputMeta( "Output" );

    TransMeta transMeta = new TransMeta( "Name" );
    transMeta.getSteps().add( input );
    transMeta.getSteps().add( output );
    transMeta.getHops().add( new HopMeta( input.getName(), output.getName() ) );
    return transMeta;
  }

  private TransMeta testTransMeta2(){
    DataGridMeta dataGrid = new DataGridMeta( "Data Grid" );
    dataGrid.getColumns().add( new Column( "test_string", DataTypes.StringType ) );
    for ( int i = 0; i < 25; i++ ) {
      dataGrid.getData().add( Collections.singletonList( UUID.randomUUID().toString() ) );
    }

    AddSequenceMeta addSequence = new AddSequenceMeta( "Add Sequence" );
    addSequence.setColumnName( "test_id" );

    CalculatorMeta calculator = new CalculatorMeta( "Calculator" );
    calculator.setColumnName( "test_id_calc" );
    calculator.setCalcFunction( CalcFunction.ADD );
    calculator.setFieldA( "test_id" );
    calculator.setFieldB( "test_id" );

    CsvOutputMeta csvOutput = new CsvOutputMeta( "Results" );
    csvOutput.setFilename( "dummy.csv" );

    TransMeta transMeta = new TransMeta( "My Transformation" );
    transMeta.getSteps().add( dataGrid );
    transMeta.getSteps().add( addSequence );
    transMeta.getSteps().add( calculator );
    transMeta.getSteps().add( csvOutput );

    transMeta.getHops().add( new HopMeta( dataGrid.getName(), addSequence.getName() ) );
    transMeta.getHops().add( new HopMeta( addSequence.getName(), calculator.getName() ) );
    transMeta.getHops().add( new HopMeta( calculator.getName(), csvOutput.getName() ) );

    return transMeta;
  }
}
