package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.ChoiceConverter;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.HapiCompositeConverter;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiFieldSetter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.LeafExtensionConverter;
import com.cerner.bunsen.definitions.PrimitiveConverter;
import com.cerner.bunsen.definitions.StringConverter;
import com.cerner.bunsen.definitions.StructureField;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import scala.collection.JavaConversions;

/**
 * A visitor implementation to travers a FHIR resource definition and produce
 * a converter class that translates from FHIR to Spark and vice versa.
 */
public class DefinitionToSparkVisitor implements DefinitionVisitor<HapiConverter<DataType>> {

  private final FhirConversionSupport fhirSupport;

  public DefinitionToSparkVisitor(FhirConversionSupport fhirSupport) {
    this.fhirSupport = fhirSupport;
  }

  private static final DataType decimalType = DataTypes.createDecimalType(12, 4);

  private static class ChoiceToSparkConverter extends ChoiceConverter<DataType> {

    ChoiceToSparkConverter(Map<String,HapiConverter<DataType>> choiceTypes,
        StructType structType,
        FhirConversionSupport fhirSupport) {

      super(choiceTypes, structType, fhirSupport);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      return ((Row) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {
      return RowFactory.create(children);
    }
  }

  /**
   * Field setter that does nothing for synthetic or unsupported field types.
   */
  private static class NoOpFieldSetter implements HapiFieldSetter,
      HapiObjectConverter {

    @Override
    public void setField(IBase parentObject, BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

    }

    @Override
    public IBase toHapi(Object input) {
      return null;
    }

  }

  private static final HapiFieldSetter NOOP_FIELD_SETTER = new NoOpFieldSetter();


  private static class CompositeToSparkConverter extends HapiCompositeConverter<DataType> {

    CompositeToSparkConverter(String elementType,
        List<StructureField<HapiConverter<DataType>>> children,
        StructType structType,
        FhirConversionSupport fhirSupport) {
      this(elementType, children, structType, fhirSupport, null);
    }

    CompositeToSparkConverter(String elementType,
        List<StructureField<HapiConverter<DataType>>> children,
        StructType structType,
        FhirConversionSupport fhirSupport,
        String extensionUrl) {

      super(elementType, children, structType, fhirSupport, extensionUrl);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      return ((Row) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {
      return RowFactory.create(children);
    }

    @Override
    protected boolean isMultiValued(DataType schemaType) {
      return schemaType instanceof ArrayType;
    }
  }

  private static class MultiValuedToSparkConverter extends HapiConverter {

    private class MultiValuedtoHapiConverter implements HapiFieldSetter {

      private final BaseRuntimeElementDefinition elementDefinition;

      private final HapiObjectConverter rowToHapiConverter;

      MultiValuedtoHapiConverter(BaseRuntimeElementDefinition elementDefinition,
          HapiObjectConverter rowToHapiConverter) {
        this.elementDefinition = elementDefinition;
        this.rowToHapiConverter = rowToHapiConverter;
      }

      @Override
      public void setField(IBase parentObject,
          BaseRuntimeChildDefinition fieldToSet,
          Object sparkObject) {

        if (sparkObject instanceof Object[]) {

          for (Object rowObject: (Object[]) sparkObject) {

            Object hapiObject = rowToHapiConverter.toHapi(rowObject);

            fieldToSet.getMutator().addValue(parentObject, (IBase) hapiObject);
          }

        } else {

          Iterable iterable = JavaConversions
              .asJavaIterable((scala.collection.Iterable) sparkObject);

          for (Object rowObject: iterable) {

            Object hapiObject = rowToHapiConverter.toHapi(rowObject);

            fieldToSet.getMutator().addValue(parentObject, (IBase) hapiObject);
          }
        }
      }
    }

    HapiConverter<DataType> elementConverter;

    MultiValuedToSparkConverter(HapiConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    @Override
    public Object fromHapi(Object input) {

      List list = (List) input;

      return list.stream()
          .map(item -> elementConverter.fromHapi(item))
          .toArray();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.createArrayType(elementConverter.getDataType());
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      BaseRuntimeElementDefinition elementDefinition = elementDefinitions[0];

      HapiObjectConverter rowToHapiConverter = (HapiObjectConverter)
          elementConverter.toHapiConverter(elementDefinition);

      return new MultiValuedtoHapiConverter(elementDefinition, rowToHapiConverter);
    }
  }

  private static HapiConverter DECIMAL_CONVERTER = new PrimitiveConverter() {

    protected void toHapi(Object input, IPrimitiveType primitive) {

      primitive.setValueAsString(((BigDecimal) input).toPlainString());
    }

    @Override
    public Object getDataType() {
      return decimalType;
    }
  };

  private static final HapiConverter STRING_CONVERTER = new StringConverter(DataTypes.StringType);


  private static final HapiConverter ENUM_CONVERTER = new StringConverter(DataTypes.StringType);

  /**
   * Converter that returns the relative value of a URI type.
   */
  private static class RelativeValueConverter extends HapiConverter<DataType> {

    private final String prefix;

    RelativeValueConverter(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Object fromHapi(Object input) {
      String uri =  ((IPrimitiveType) input).getValueAsString();

      return uri != null && uri.startsWith(prefix)
          ? uri.substring(uri.lastIndexOf('/') + 1)
          : null;
    }

    @Override
    public DataType getDataType() {
      return DataTypes.StringType;
    }

    @Override
    public String getElementType() {

      return "String";
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      // Returns a field setter that does nothing, since this is for a synthetic type-specific
      // reference field, and the value will be set from the primary field.
      return NOOP_FIELD_SETTER;
    }

  }

  private static final HapiConverter DATE_CONVERTER = new StringConverter(DataTypes.StringType);

  private static final HapiConverter BOOLEAN_CONVERTER = new PrimitiveConverter<DataType>() {

    @Override
    public DataType getDataType() {
      return DataTypes.BooleanType;
    }
  };

  private static final HapiConverter INTEGER_CONVERTER = new PrimitiveConverter<DataType>() {

    @Override
    public DataType getDataType() {
      return DataTypes.IntegerType;
    }
  };

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

  @Override
  public HapiConverter visitPrimitive(String elementName,
      String primitiveType) {

    return TYPE_TO_CONVERTER.get(primitiveType);
  }

  @Override
  public HapiConverter<DataType> visitComposite(String elementName,
      String elementType,
      List<StructureField<HapiConverter<DataType>>> children) {

    StructField[] fields = children.stream()
        .map(entry -> new StructField(entry.fieldName(),
            entry.result().getDataType(),
            true,
            Metadata.empty()))
        .toArray(StructField[]::new);

    return new CompositeToSparkConverter(elementType,
        children, new StructType(fields), fhirSupport);
  }

  @Override
  public HapiConverter visitReference(String elementName,
      List<String> referenceTypes,
      List<StructureField<HapiConverter<DataType>>> children) {

    // Add direct references
    List<StructureField<HapiConverter<DataType>>> fieldsWithReferences =
        referenceTypes.stream()
        .map(refUri -> {

          String relativeType = refUri.substring(refUri.lastIndexOf('/') + 1);

          return new StructureField<HapiConverter<DataType>>("reference",
              relativeType + "Id",
              null,
              false,
              new RelativeValueConverter(relativeType));

        }).collect(Collectors.toList());

    fieldsWithReferences.addAll(children);

    StructField[] fields = fieldsWithReferences.stream()
        .map(entry -> new StructField(entry.fieldName(),
            entry.result().getDataType(),
            true,
            Metadata.empty()))
        .toArray(StructField[]::new);

    return new CompositeToSparkConverter(null,
        fieldsWithReferences,
        new StructType(fields), fhirSupport);
  }

  @Override
  public HapiConverter visitParentExtension(String elementName,
      String extensionUrl,
      List<StructureField<HapiConverter<DataType>>> children) {

    // Ignore extension fields that don't have declared content for now.
    if (children.isEmpty()) {
      return null;
    }

    StructField[] fields = children.stream()
        .map(entry ->
            new StructField(entry.fieldName(),
                entry.result().getDataType(),
                true,
                Metadata.empty()))
        .toArray(StructField[]::new);

    return new CompositeToSparkConverter(null,
        children,
        new StructType(fields),
        fhirSupport,
        extensionUrl);
  }

  @Override
  public HapiConverter<DataType> visitLeafExtension(String elementName,
      String extensionUri,
      HapiConverter elementConverter) {

    return new LeafExtensionConverter<DataType>(extensionUri, elementConverter);
  }

  @Override
  public HapiConverter<DataType> visitMultiValued(String elementName,
      HapiConverter arrayElement) {

    return new MultiValuedToSparkConverter(arrayElement);
  }

  @Override
  public HapiConverter<DataType> visitChoice(String elementName,
      Map<String,HapiConverter<DataType>> choiceTypes) {

    StructField[] fields = choiceTypes.entrySet().stream()
        .map(entry -> {

          // Ensure first character of the field is lower case.
          String fieldName = Character.toLowerCase(entry.getKey().charAt(0))
              + entry.getKey().substring(1);

          return new StructField(fieldName,
              entry.getValue().getDataType(),
              true,
              Metadata.empty());

        })
        .toArray(StructField[]::new);

    return new ChoiceToSparkConverter(choiceTypes,
        new StructType(fields),
        fhirSupport);
  }
}
