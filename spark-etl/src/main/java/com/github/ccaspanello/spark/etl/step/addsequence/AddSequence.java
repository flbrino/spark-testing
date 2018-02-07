package com.github.ccaspanello.spark.etl.step.addsequence;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Add Sequence Step
 *
 * Adds a sequence using Sparks built in function "monotonicallyIncreasingId"
 *
 * Created by ccaspanello on 1/29/18.
 */
public class AddSequence extends BaseStep<AddSequenceMeta> {

  public AddSequence( AddSequenceMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    String columnName = getStepMeta().getColumnName();
    Dataset<Row> incoming = getIncoming().stream().findFirst().get().getData();
    Dataset<Row> result = incoming.withColumn( columnName, org.apache.spark.sql.functions.monotonicallyIncreasingId() );
    setData(result);
  }
}
