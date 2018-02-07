package com.github.ccaspanello.spark.etl.step.csvinput;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV Input Step Logic
 *
 * Created by ccaspanello on 1/29/18.
 */
public class CsvInput extends BaseStep<CsvInputMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(CsvInput.class);

  public CsvInput(CsvInputMeta meta) {
    super(meta);
  }

  @Override
  public void execute() {
    Dataset<Row> result = getSparkSession().read()
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(getStepMeta().getFilename());
    LOG.info("ROW COUNT for {}: {}", getStepMeta().getName() ,result.count());
    setData(result);
  }
}
