package com.github.spark.karaf.delegate;

import org.apache.karaf.main.Main;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.startlevel.FrameworkStartLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by ccaspanello on 2/8/18.
 */
public class KarafContainer {

  private static final Logger LOG = LoggerFactory.getLogger( KarafContainer.class );

  private final String karafHome;
  private Main main;

  public KarafContainer( String karafHome ) {
    this.karafHome = karafHome;
  }

  public void launch( String[] args ) {
    try {
      String root = new File( karafHome ).getAbsolutePath();
      System.out.println( "Starting Karaf @ " + root );
      System.setProperty( "karaf.home", root );
      System.setProperty( "karaf.base", root );
      System.setProperty( "karaf.data", root + "/data" );
      System.setProperty( "karaf.etc", root + "/etc" );
      System.setProperty( "karaf.history", root + "/data/history.txt" );
      System.setProperty( "karaf.instances", root + "/instances" );
      System.setProperty( "karaf.startLocalConsole", "false" );
      System.setProperty( "karaf.startRemoteShell", "true" );
      System.setProperty( "karaf.lock", "false" );
      main = new Main( args );
      main.launch();
    } catch ( Exception e ) {
      throw new DelegateException( "Unexpected error starting Karaf container.", e );
    }
  }

  public void awaitShutdown() {
    try {
      main.awaitShutdown();
    } catch ( Exception e ) {
      throw new DelegateException( "Unexpected error waiting for Karaf container to shut down.", e );
    }
  }

  public <E> E service( Class<E> serviceType ) {
    BundleContext bundleContext = main.getFramework().getBundleContext();
    ServiceReference<E> serviceReference = bundleContext.getServiceReference( serviceType );
    return bundleContext.getService( serviceReference );
  }
}
