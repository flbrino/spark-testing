package com.github.ccaspanello.spark.etl;

import com.github.ccaspanello.spark.etl.StepRegistry;
import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.api.StepMeta;
import com.github.ccaspanello.spark.etl.step.addsequence.AddSequence;
import com.github.ccaspanello.spark.etl.step.addsequence.AddSequenceMeta;
import com.github.ccaspanello.spark.etl.step.calculator.Calculator;
import com.github.ccaspanello.spark.etl.step.calculator.CalculatorMeta;
import com.github.ccaspanello.spark.etl.step.csvinput.CsvInput;
import com.github.ccaspanello.spark.etl.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.spark.etl.step.csvoutput.CsvOutput;
import com.github.ccaspanello.spark.etl.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGrid;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGridMeta;
import com.github.ccaspanello.spark.etl.step.join.Join;
import com.github.ccaspanello.spark.etl.step.join.JoinMeta;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Step Registry Test
 *
 * Created by ccaspanello on 1/29/18.
 */
public class StepRegistryTest {

  @Test
  public static void test(){
    StepRegistry stepRegistry = new StepRegistry();
    stepRegistry.init();

    Map<Class<? extends StepMeta>, Class<? extends Step>> registry = stepRegistry.getRegistry();

    assertEquals(registry.size(), 6);
    assertEquals(registry.get(AddSequenceMeta.class), AddSequence.class);
    assertEquals(registry.get(CalculatorMeta.class), Calculator.class);
    assertEquals(registry.get(CsvInputMeta.class), CsvInput.class);
    assertEquals(registry.get(CsvOutputMeta.class), CsvOutput.class);
    assertEquals(registry.get(DataGridMeta.class), DataGrid.class);
    assertEquals(registry.get(JoinMeta.class), Join.class);
  }
}
