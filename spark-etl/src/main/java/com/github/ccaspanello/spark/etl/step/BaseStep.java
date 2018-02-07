package com.github.ccaspanello.spark.etl.step;

import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.api.StepMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Set;

/**
 * Common Base Step Logic
 *
 * Created by ccaspanello on 1/29/2018.
 */
public abstract class BaseStep<E extends StepMeta> implements Step {

    private SparkSession sparkSession;
    private E meta;
    private Set<Step> incoming;
    private Set<Step> outgoing;
    private Dataset<Row> data;

    public BaseStep(E meta){
        this.meta = meta;
    }

    //<editor-fold desc="Getters & Setters">
    @Override
    public void setIncoming(Set<Step> incoming) {
        this.incoming = incoming;
    }
    @Override
    public void setOutgoing(Set<Step> outgoing) {
        this.outgoing = outgoing;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public E getStepMeta(){
        return meta;
    }


    public Set<Step> getIncoming() {
        return incoming;
    }

    public Set<Step> getOutgoing() {
        return outgoing;
    }

    public Dataset<Row> getData() {
        return data;
    }

    public void setData(Dataset<Row> data) {
        this.data = data;
    }
    //</editor-fold>
}
