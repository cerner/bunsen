package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseExtension;

public class LeafExtensionConverter<T> extends HapiConverter<T> {

  class LeafExensionFieldSetter implements HapiFieldSetter {

    private final HapiObjectConverter valuetoHapiConverter;

    private final BaseRuntimeElementCompositeDefinition elementDefinition;

    LeafExensionFieldSetter(BaseRuntimeElementCompositeDefinition elementDefinition,
        HapiObjectConverter valuetoHapiConverter) {

      this.elementDefinition = elementDefinition;
      this.valuetoHapiConverter = valuetoHapiConverter;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      IBase hapiObject = valuetoHapiConverter.toHapi(sparkObject);

      IBaseExtension extension = (IBaseExtension) elementDefinition.newInstance(extensionUrl);

      extension.setValue((IBaseDatatype) hapiObject);

      fieldToSet.getMutator().addValue(parentObject, extension);
    }
  }

  private final String extensionUrl;

  private final HapiConverter<T> valueConverter;

  public LeafExtensionConverter(String extensionUrl, HapiConverter valueConverter) {

    this.extensionUrl = extensionUrl;
    this.valueConverter = valueConverter;
  }

  @Override
  public Object fromHapi(Object input) {

    IBaseExtension extension = (IBaseExtension) input;

    return valueConverter.fromHapi(extension.getValue());
  }

  @Override
  public T getDataType() {
    return valueConverter.getDataType();
  }

  @Override
  public String extensionUrl() {
    return extensionUrl;
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    // Get the structure definition of the value.
    String fieldName = "value" + valueConverter.getElementType();

    RuntimeCompositeDatatypeDefinition definition =
        (RuntimeCompositeDatatypeDefinition) elementDefinitions[0];

    BaseRuntimeElementDefinition valueDefinition = definition.getChildByName(fieldName)
        .getChildByName(fieldName);

    HapiObjectConverter sparkToHapi = (HapiObjectConverter)
        valueConverter.toHapiConverter(valueDefinition);

    return new LeafExensionFieldSetter(definition,  sparkToHapi);
  }
}