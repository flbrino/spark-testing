package com.github.ccaspanello.spark.etl.step.join;

import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Iterator;

/**
 * Join Step
 * <p>
 * Joins 2 datasets together.
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class Join extends BaseStep<JoinMeta> {

  public Join( JoinMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    Iterator<Step> iterator = getIncoming().iterator();
    Dataset<Row> data1 = iterator.next().getData();
    Dataset<Row> data2 = iterator.next().getData();
    setData( data1.join( data2, getStepMeta().getJoinColumn() ) );
  }
}
