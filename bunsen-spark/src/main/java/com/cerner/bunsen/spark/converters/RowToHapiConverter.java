package com.cerner.bunsen.spark.converters;

import org.hl7.fhir.instance.model.api.IBase;

public interface RowToHapiConverter {

  IBase toHapi(Object input);
}