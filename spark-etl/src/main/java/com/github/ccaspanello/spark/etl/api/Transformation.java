package com.github.ccaspanello.spark.etl.api;

import com.github.ccaspanello.spark.etl.AppContext;
import com.github.ccaspanello.spark.etl.StepRegistry;
import com.github.ccaspanello.spark.etl.gson.TransMetaConverter;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.spark.sql.SparkSession;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transformation
 * <p>
 * Materialized version of TransMeta data; logical steps wired with hops.
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class Transformation implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger( Transformation.class );

  private final AppContext appContext;
  private final String name;

  private TransMeta transMeta;

  public Transformation( AppContext appContext, TransMeta transMeta ) {
    this.appContext = appContext;
    this.name = transMeta.getName();
    this.transMeta = transMeta;
  }

  /**
   * Updates TranMeta model with runtime parameters
   *
   * @param parameters
   */
  public void injectParameters( Map<String, String> parameters ) {
    TransMetaConverter transMetaConverter = new TransMetaConverter();
    String json = transMetaConverter.toJson( transMeta );

    StrSubstitutor sub = new StrSubstitutor(parameters);
    String newJson = sub.replace(json);
    TransMeta newTransMeta = transMetaConverter.fromJson( newJson );
    this.transMeta = newTransMeta;
  }

  /**
   * Execute transformation
   */
  public void execute() {
    SparkSession session = new SparkSession( appContext.getSparkContext() );
    List<Step> executionPlan = createExecutionPlan();
    LOG.warn( "RUNNING STEPS" );
    LOG.warn( "=============================" );
    for ( Step step : executionPlan ) {
      LOG.warn( "***** -> {}", step.getStepMeta().getName() );
      step.setSparkSession( session );
      step.execute();
    }
  }

  private List<Step> createExecutionPlan(){
    DirectedGraph<Step, Hop> graph = createGraph( transMeta, appContext.getStepRegistry() );
    LOG.warn( "STEP ORDER" );
    LOG.warn( "=============================" );
    List<Step> executionPlan = new ArrayList<>();
    TopologicalOrderIterator<Step, Hop> orderIterator = new TopologicalOrderIterator<>( graph );
    while ( orderIterator.hasNext() ) {
      Step step = orderIterator.next();
      LOG.warn( "Step -> {}", step.getStepMeta().getName() );
      Set<Hop> incoming = graph.incomingEdgesOf( step );
      Set<Hop> outgoing = graph.outgoingEdgesOf( step );

      LOG.warn( "   - Incoming: {}", incoming.size() );
      LOG.warn( "   - Outgoing: {}", outgoing.size() );

      Set<Step> incomingSteps = new HashSet<>();
      for ( Hop hop : incoming ) {
        incomingSteps.add( hop.incomingStep() );
      }

      Set<Step> outgoingSteps = new HashSet<>();
      for ( Hop hop : outgoing ) {
        outgoingSteps.add( hop.outgoingStep() );
      }

      incomingSteps.forEach( s -> LOG.warn( "  -> Incoming: {}", s.getStepMeta().getName() ) );
      outgoingSteps.forEach( s -> LOG.warn( "  -> Outgoing: {}", s.getStepMeta().getName() ) );

      step.setIncoming( incomingSteps );
      step.setOutgoing( outgoingSteps );

      executionPlan.add( step );
    }
    return executionPlan;
  }

  private DirectedGraph<Step, Hop> createGraph( TransMeta transMeta, StepRegistry stepRegistry ) {
    DirectedGraph<Step, Hop> graph = new DefaultDirectedGraph<>( Hop.class );

    // Create and Map Steps
    Map<String, Step> map = new HashMap<>();
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      map.put( stepMeta.getName(), stepRegistry.createStep( stepMeta ) );
    }

    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      graph.addVertex( map.get( stepMeta.getName() ) );
    }

    for ( HopMeta hopMeta : transMeta.getHops() ) {
      Step incoming = map.get( hopMeta.getIncoming() );
      Step outgoing = map.get( hopMeta.getOutgoing() );
      graph.addEdge( incoming, outgoing );
    }
    return graph;
  }

  //<editor-fold desc="Getters & Setters">
  public String getName() {
    return name;
  }
  //</editor-fold>

}
