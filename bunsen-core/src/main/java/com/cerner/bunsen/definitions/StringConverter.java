package com.cerner.bunsen.definitions;

import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class StringConverter<T> extends PrimitiveConverter<T> {

  private final T dataType;

  public StringConverter(T dataType) {

    this.dataType = dataType;
  }

  protected void toHapi(Object input, IPrimitiveType primitive) {
    primitive.setValueAsString((String) input);
  }

  protected final Object fromHapi(IPrimitiveType primitive) {
    return primitive.getValueAsString();
  }


  @Override
  public T getDataType() {

    return dataType;
  }


  @Override
  public String getElementType() {

    return "String";
  }
}


