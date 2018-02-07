package com.github.ccaspanello.spark.etl.api;

import java.io.Serializable;

/**
 * Step Meta Interface
 *
 * Interface for classes that hold step meta data (model; no logic).
 *
 * Created by ccaspanello on 1/29/2018.
 */
public interface StepMeta extends Serializable {

    //<editor-fold desc="Getters & Setters">
    String getName();
    //</editor-fold>
}
