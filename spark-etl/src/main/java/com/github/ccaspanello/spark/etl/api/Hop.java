package com.github.ccaspanello.spark.etl.api;

import org.jgrapht.graph.DefaultEdge;

/**
 * Hop
 *
 * Wraps the DefaultEdge to define the to/from steps.  This wrapper will cast the source/target to Steps which by
 * default are not accessible.
 *
 * Created by ccaspanello on 1/29/18.
 */
public class Hop extends DefaultEdge {

    //<editor-fold desc="Getters & Setters">
    public Step incomingStep() {
        return (Step) getSource();
    }

    public Step outgoingStep() {
        return (Step) getTarget();
    }
    //</editor-fold>
}
