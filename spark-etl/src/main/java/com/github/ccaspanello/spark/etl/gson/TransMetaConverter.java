package com.github.ccaspanello.spark.etl.gson;

import com.github.ccaspanello.spark.etl.api.StepMeta;
import com.github.ccaspanello.spark.etl.api.TransMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.types.DataType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * TransMeta Converter
 * <p>
 * Coverts a TransMeta Object to and from JSON strings / files.
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class TransMetaConverter {

  private Gson gson;

  public TransMetaConverter() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter( DataType.class, new DataTypeAdapter() );
    builder.registerTypeAdapter( StepMeta.class, new StepMetaAdapter() );
    builder.setPrettyPrinting();
    this.gson = builder.create();
  }

  public String toJson( TransMeta transMeta ) {
    return gson.toJson( transMeta );
  }

  public TransMeta fromJson( String transMeta ) {
    return gson.fromJson( transMeta, TransMeta.class );
  }

  public void toJson( TransMeta transMeta, File file ) {
    try {
      FileUtils.writeStringToFile( file, toJson( transMeta ) );
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to write json file.", e );
    }
  }

  public TransMeta fromJson( File file ) {
    try {
      String json = FileUtils.readFileToString( file, Charset.forName("UTF-8") );
      return fromJson(json);
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to read json file.", e );
    }
  }

}
