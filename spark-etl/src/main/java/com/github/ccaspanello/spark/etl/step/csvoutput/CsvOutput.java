package com.github.ccaspanello.spark.etl.step.csvoutput;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV Output Step Logic
 *
 * Created by ccaspanello on 1/29/18.
 */
public class CsvOutput extends BaseStep<CsvOutputMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(CsvOutput.class);

  public CsvOutput(CsvOutputMeta meta) {
    super(meta);
  }

  @Override
  public void execute() {
    Dataset<Row> result = getIncoming().stream().findFirst().get().getData();
    //        result.write().text("output-text");
    //        result.write().csv(getStepMeta().getFilename());
    result.write().format("com.databricks.spark.csv").option("header", true).save(getStepMeta().getFilename());
    setData( result );
  }
}
