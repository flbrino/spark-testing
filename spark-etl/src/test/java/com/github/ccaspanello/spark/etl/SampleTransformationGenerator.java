package com.github.ccaspanello.spark.etl;

import com.github.ccaspanello.spark.etl.api.HopMeta;
import com.github.ccaspanello.spark.etl.api.TransMeta;
import com.github.ccaspanello.spark.etl.step.addsequence.AddSequence;
import com.github.ccaspanello.spark.etl.step.addsequence.AddSequenceMeta;
import com.github.ccaspanello.spark.etl.step.calculator.CalcFunction;
import com.github.ccaspanello.spark.etl.step.calculator.CalculatorMeta;
import com.github.ccaspanello.spark.etl.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.spark.etl.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.spark.etl.step.datagrid.Column;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGridMeta;
import com.github.ccaspanello.spark.etl.step.join.JoinMeta;
import org.apache.spark.sql.types.DataTypes;

import java.util.Collections;

/**
 * Uses TransMeta objects to to generate sample transformation files
 * <p>
 * Created by ccaspanello on 1/30/18.
 */
public class SampleTransformationGenerator {

  /**
   * DataGrid -> AddSequence -> Calculator -> CsvOutput
   *
   * @return TransMeta
   */
  public static TransMeta simple() {

    DataGridMeta dataGrid = new DataGridMeta( "Data Grid" );
    dataGrid.getColumns().add( new Column( "test_string", DataTypes.StringType ) );
    for ( int i = 0; i < 25; i++ ) {
      dataGrid.getData().add( Collections.singletonList( "key" + i ) );
    }

    AddSequenceMeta addSequence = new AddSequenceMeta( "Add Sequence" );
    addSequence.setColumnName( "test_id" );

    CalculatorMeta calculator = new CalculatorMeta( "Calculator" );
    calculator.setColumnName( "test_id_calc" );
    calculator.setCalcFunction( CalcFunction.ADD );
    calculator.setFieldA( "test_id" );
    calculator.setFieldB( "test_id" );

    CsvOutputMeta csvOutput = new CsvOutputMeta( "Results" );
    csvOutput.setFilename( "${outputDir}/simple" );

    TransMeta transMeta = new TransMeta( "Simple Transformation" );
    transMeta.getSteps().add( dataGrid );
    transMeta.getSteps().add( addSequence );
    transMeta.getSteps().add( calculator );
    transMeta.getSteps().add( csvOutput );

    transMeta.getHops().add( new HopMeta( dataGrid.getName(), addSequence.getName() ) );
    transMeta.getHops().add( new HopMeta( addSequence.getName(), calculator.getName() ) );
    transMeta.getHops().add( new HopMeta( calculator.getName(), csvOutput.getName() ) );

    return transMeta;
  }

  /**
   * (DataGrid1 & DataGrid2) -> Lookup -> CsvOutput
   *
   * @return TransMeta
   */
  public TransMeta combine() {

    CsvInputMeta user = new CsvInputMeta( "CSV User" );
    user.setFilename( "${projectDir}/input/user.csv" );
    user.setHeader( true );

    CsvInputMeta userLog = new CsvInputMeta( "CSV User Log" );
    userLog.setFilename( "${projectDir}/input/userLog.csv" );
    userLog.setHeader( true );

    JoinMeta join = new JoinMeta( "Join" );
    join.setJoinColumn( "username" );

    CsvOutputMeta csvOutput = new CsvOutputMeta( "CSV Output" );
    csvOutput.setFilename( "${outputDir}/combine" );
    csvOutput.setHeader( true );

    TransMeta transMeta = new TransMeta( "Combine Transformation" );
    transMeta.getSteps().add( user );
    transMeta.getSteps().add( userLog );
    transMeta.getSteps().add( join );
    transMeta.getSteps().add( csvOutput );

    transMeta.getHops().add( new HopMeta( user, join ) );
    transMeta.getHops().add( new HopMeta( userLog, join ) );
    transMeta.getHops().add( new HopMeta( join, csvOutput ) );

    return transMeta;
  }

  /**
   * DataGrid -> Filter -> (CsvOutput1 & CsvOutput2)
   *
   * @return TransMeta
   */
  public TransMeta split() {
    return null;
  }

  /**
   * (DataGrid1 & DataGrid2) -> Lookup -> Filter -> (CsvOutput & CsvOutput2)
   *
   * @return TransMeta
   */
  public TransMeta complex() {
    return null;
  }
}
