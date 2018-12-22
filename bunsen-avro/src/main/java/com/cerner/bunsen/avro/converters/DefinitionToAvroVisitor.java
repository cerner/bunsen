package com.cerner.bunsen.avro.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.ChoiceConverter;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.HapiCompositeConverter;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.PrimitiveConverter;
import com.cerner.bunsen.definitions.StringConverter;
import com.cerner.bunsen.definitions.StructureField;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.hl7.fhir.instance.model.api.IBase;

public class DefinitionToAvroVisitor implements DefinitionVisitor<HapiConverter<Schema>> {

  private final FhirConversionSupport fhirSupport;


  private static final HapiConverter STRING_CONVERTER =
      new StringConverter(Schema.create(Type.STRING));

  private static final HapiConverter DATE_CONVERTER =
      new StringConverter(Schema.create(Type.STRING));

  private static final Schema BOOLEAN_SCHEMA = Schema.create(Type.BOOLEAN);

  private static final HapiConverter BOOLEAN_CONVERTER = new PrimitiveConverter<Schema>() {

    @Override
    public Schema getDataType() {
      return BOOLEAN_SCHEMA;
    }
  };

  private static final Schema INTEGER_SCHEMA = Schema.create(Type.INT);

  private static final HapiConverter INTEGER_CONVERTER = new PrimitiveConverter<Schema>() {

    @Override
    public Schema getDataType() {
      return INTEGER_SCHEMA;
    }
  };

  private static final HapiConverter ENUM_CONVERTER =
      new StringConverter(Schema.create(Type.STRING));

  private static final HapiConverter DECIMAL_CONVERTER =
      new StringConverter(Schema.create(Type.STRING));


  static final Map<String,HapiConverter> TYPE_TO_CONVERTER =
      ImmutableMap.<String,HapiConverter>builder()
          .put("id", STRING_CONVERTER)
          .put("boolean", BOOLEAN_CONVERTER)
          .put("code", ENUM_CONVERTER)
          .put("markdown", STRING_CONVERTER)
          .put("date", DATE_CONVERTER)
          .put("instant", DATE_CONVERTER)
          .put("datetime", DATE_CONVERTER)
          .put("dateTime", DATE_CONVERTER)
          .put("time", STRING_CONVERTER)
          .put("string", STRING_CONVERTER)
          .put("xhtml", STRING_CONVERTER)
          .put("decimal", DECIMAL_CONVERTER)
          .put("integer", INTEGER_CONVERTER)
          .put("unsignedInt", INTEGER_CONVERTER)
          .put("positiveInt", INTEGER_CONVERTER)
          .put("base64Binary", STRING_CONVERTER) // FIXME: convert to Base64
          .put("uri", STRING_CONVERTER)
          .build();

  private static class CompositeToAvroConverter extends HapiCompositeConverter<Schema> {

    CompositeToAvroConverter(String elementType,
        List<StructureField<HapiConverter<Schema>>> children,
        Schema structType,
        FhirConversionSupport fhirSupport) {
      this(elementType, children, structType, fhirSupport, null);
    }

    CompositeToAvroConverter(String elementType,
        List<StructureField<HapiConverter<Schema>>> children,
        Schema structType,
        FhirConversionSupport fhirSupport,
        String extensionUrl) {

      super(elementType, children, structType, fhirSupport, extensionUrl);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      return ((IndexedRecord) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {

      IndexedRecord record = new GenericData.Record(getDataType());

      for (int i = 0; i < children.length; ++i) {

        record.put(i, children[i]);
      }

      return record;
    }

    @Override
    protected boolean isMultiValued(Schema schema) {

      return schema.getType().equals(Schema.Type.ARRAY);
    }
  }

  private static class ChoiceToAvroConverter extends ChoiceConverter<Schema> {

    ChoiceToAvroConverter(Map<String,HapiConverter<Schema>> choiceTypes,
        Schema structType,
        FhirConversionSupport fhirSupport) {

      super(choiceTypes, structType, fhirSupport);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      return ((IndexedRecord) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {

      IndexedRecord record = new GenericData.Record(getDataType());

      for (int i = 0; i < children.length; ++i) {

        record.put(i, children[i]);
      }

      return record;
    }
  }

  private static class MultiValuedToAvroConverter extends HapiConverter<Schema> {

    private class MultiValuedtoHapiConverter implements HapiFieldSetter {

      private final BaseRuntimeElementDefinition elementDefinition;

      private final HapiObjectConverter elementToHapiConverter;

      MultiValuedtoHapiConverter(BaseRuntimeElementDefinition elementDefinition,
          HapiObjectConverter elementToHapiConverter) {
        this.elementDefinition = elementDefinition;
        this.elementToHapiConverter = elementToHapiConverter;
      }

      @Override
      public void setField(IBase parentObject,
          BaseRuntimeChildDefinition fieldToSet,
          Object element) {

        for (Object value: (Iterable) element) {

          Object hapiObject = elementToHapiConverter.toHapi(value);

          fieldToSet.getMutator().addValue(parentObject, (IBase) hapiObject);
        }
      }
    }

    HapiConverter<Schema> elementConverter;

    MultiValuedToAvroConverter(HapiConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    @Override
    public Object fromHapi(Object input) {

      List list = (List) input;

      return list.stream()
          .map(item -> elementConverter.fromHapi(item))
          .collect(Collectors.toList());
    }

    @Override
    public Schema getDataType() {

      return Schema.createArray(elementConverter.getDataType());
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      BaseRuntimeElementDefinition elementDefinition = elementDefinitions[0];

      HapiObjectConverter rowToHapiConverter = (HapiObjectConverter)
          elementConverter.toHapiConverter(elementDefinition);

      return new MultiValuedtoHapiConverter(elementDefinition, rowToHapiConverter);
    }
  }

  public DefinitionToAvroVisitor(FhirConversionSupport fhirSupport) {

    this.fhirSupport = fhirSupport;

  }

  @Override
  public HapiConverter<Schema> visitPrimitive(String elementName, String primitiveType) {

    return TYPE_TO_CONVERTER.get(primitiveType);
  }

  @Override
  public HapiConverter<Schema> visitComposite(String elementName, String elementType,
      List<StructureField<HapiConverter<Schema>>> children) {

    List<Field> fields = children.stream()
        .map((StructureField<HapiConverter<Schema>> field) -> {

          String doc = field.extensionUrl() != null
              ? "Extension field for " + field.extensionUrl()
              : "Field for FHIR property " + field.propertyName();

          return new Field(field.fieldName(),
              field.result().getDataType(),
              doc,
              (Object) null);

        }).collect(Collectors.toList());

    Schema schema = Schema.createRecord(elementType,
        "Structure for FHIR type " + elementType,
        "com.cerner.bunsen.avro",
        false, fields);


    return new CompositeToAvroConverter(elementType,
        children, schema, fhirSupport);
  }

  @Override
  public HapiConverter<Schema> visitReference(String elementName, List<String> referenceTypes,
      List<StructureField<HapiConverter<Schema>>> children) {
    return NoOpConverter.INSTANCE;
  }

  @Override
  public HapiConverter<Schema> visitParentExtension(String elementName, String extensionUrl,
      List<StructureField<HapiConverter<Schema>>> children) {
    return NoOpConverter.INSTANCE;
  }

  @Override
  public HapiConverter<Schema> visitLeafExtension(String elementName, String extensionUrl,
      HapiConverter<Schema> element) {
    return NoOpConverter.INSTANCE;
  }

  @Override
  public HapiConverter<Schema> visitMultiValued(String elementName,
      HapiConverter<Schema> arrayElement) {

    return new MultiValuedToAvroConverter(arrayElement);
  }

  @Override
  public HapiConverter<Schema> visitChoice(String elementName,
      Map<String, HapiConverter<Schema>> choiceTypes) {

    List<Field> fields = choiceTypes.entrySet().stream()
        .map(entry -> {

          // Ensure first character of the field is lower case.
          String fieldName = Character.toLowerCase(entry.getKey().charAt(0))
              + entry.getKey().substring(1);

          return new Field(fieldName,
              entry.getValue().getDataType(),
              "Choice field",
              (Object) null);

        })
        .collect(Collectors.toList());

    // TODO: Use path from root to define choice name?
    String recordName = choiceTypes.keySet().stream().collect(Collectors.joining());

    Schema schema = Schema.createRecord("Choice" + recordName,
        "Structure for FHIR choice type ",
        "com.cerner.bunsen.avro",
        false, fields);

    return new ChoiceToAvroConverter(choiceTypes,
        schema,
        fhirSupport);

  }
}
