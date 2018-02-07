package com.github.ccaspanello.spark.etl.step.filter;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 2/4/18.
 */
public class Filter extends BaseStep<FilterMeta> {

  public Filter( FilterMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    // TODO Figure out how to do a split
    Dataset<Row> incoming = getIncoming().stream().findFirst().get().getData();
    setData( incoming );
  }
}
