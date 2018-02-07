package com.github.ccaspanello.spark.etl.gson;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.spark.sql.types.DataType;

import java.lang.reflect.Type;

/**
 * Converts Spark SQL DataType into JSON
 *
 * TODO Seems like we have JSON embedded in JSON; while it works, cleanup by converting JValue into JsonObject.
 *
 * Created by ccaspanello on 1/30/18.
 */
public class DataTypeAdapter implements JsonSerializer<DataType>, JsonDeserializer<DataType> {

  @Override
  public DataType deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context )
    throws JsonParseException {
    return DataType.fromJson( json.getAsString() );
  }

  @Override
  public JsonElement serialize( DataType src, Type typeOfSrc, JsonSerializationContext context ) {
    return new JsonPrimitive( src.json() );
  }
}
