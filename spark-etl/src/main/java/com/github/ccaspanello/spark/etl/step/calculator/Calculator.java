package com.github.ccaspanello.spark.etl.step.calculator;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculator Step Logic
 *
 * Created by ccaspanello on 1/29/18.
 */
public class Calculator extends BaseStep<CalculatorMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(Calculator.class);

  public Calculator( CalculatorMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    Dataset<Row> incoming = getIncoming().stream().findFirst().get().getData();
    switch ( getStepMeta().getCalcFunction() ) {
      case ADD:
        addFunction( incoming );
        break;
      default:
        throw new RuntimeException( "CalcFunction not supported." );
    }
  }

  private void addFunction( Dataset<Row> incoming ) {
    String colName = getStepMeta().getColumnName();
    String expression = getStepMeta().getFieldA() + "+" + getStepMeta().getFieldB();
    Dataset<Row> result = incoming.withColumn( colName, org.apache.spark.sql.functions.expr( expression ) );
    setData( result );
  }
}
