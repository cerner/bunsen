package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.DefinitionVisitorsUtil;
import com.cerner.bunsen.definitions.EnumConverter;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.HapiChoiceConverter;
import com.cerner.bunsen.definitions.HapiCompositeConverter;
import com.cerner.bunsen.definitions.HapiContainedConverter;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiFieldSetter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.HapiConverter.MultiValueConverter;
import com.cerner.bunsen.definitions.LeafExtensionConverter;
import com.cerner.bunsen.definitions.PrimitiveConverter;
import com.cerner.bunsen.definitions.StringConverter;
import com.cerner.bunsen.definitions.StructureField;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

/**
 * A visitor implementation to travers a FHIR resource definition and produce
 * a converter class that translates from FHIR to Spark and vice versa.
 */
public class DefinitionToSparkVisitor implements DefinitionVisitor<HapiConverter<DataType>> {

  private final FhirConversionSupport fhirSupport;

  private final String basePackage;

  private final Map<String, HapiConverter<DataType>> visitedConverters;

  /**
   * Creates a visitor to construct Spark Row conversion objects.
   *
   * @param fhirSupport support for FHIR conversions.
   * @param basePackage the base package to be used as a prefix for unique keys to cache generated
   *        converters.
   * @param visitedConverters a mutable cache of generated converters that may
   *        be reused by types that contain them.
   */
  public DefinitionToSparkVisitor(FhirConversionSupport fhirSupport,
      String basePackage,
      Map<String, HapiConverter<DataType>> visitedConverters) {
    this.fhirSupport = fhirSupport;
    this.basePackage = basePackage;
    this.visitedConverters = visitedConverters;
  }

  private static final DataType decimalType = DataTypes.createDecimalType(12, 4);

  private static class HapiChoiceToSparkConverter extends HapiChoiceConverter<DataType> {

    HapiChoiceToSparkConverter(Map<String,HapiConverter<DataType>> choiceTypes,
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
        Object sparkObject) {}

    @Override
    public IBase toHapi(Object input) {
      return null;
    }

  }

  private static final HapiFieldSetter NOOP_FIELD_SETTER = new NoOpFieldSetter();


  private static class HapiCompositeToSparkConverter extends HapiCompositeConverter<DataType> {

    HapiCompositeToSparkConverter(String elementType,
        List<StructureField<HapiConverter<DataType>>> children,
        StructType structType,
        FhirConversionSupport fhirSupport) {
      this(elementType, children, structType, fhirSupport, null);
    }

    HapiCompositeToSparkConverter(String elementType,
        List<StructureField<HapiConverter<DataType>>> children,
        StructType structType,
        FhirConversionSupport fhirSupport,
        String extensionUrl) {

      super(elementType, children, structType, fhirSupport, extensionUrl);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      // The row being converted may have come from a different schema or profile
      // than what is being requested by the caller, so we must look up fields
      // by name.
      String fieldName = ((StructType) getDataType()).apply(index).name();

      scala.Option fieldIndex = ((GenericRowWithSchema) composite)
          .schema()
          .getFieldIndex(fieldName);

      if (fieldIndex.isDefined()) {
        return ((Row) composite).get((Integer) fieldIndex.get());
      } else {
        return null;
      }
    }

    @Override
    protected Object createComposite(Object[] children) {

      return new GenericRowWithSchema(children, (StructType) getDataType());
    }

    @Override
    protected boolean isMultiValued(DataType schemaType) {
      return schemaType instanceof ArrayType;
    }
  }

  private static class HapiContainedToSparkConverter extends HapiContainedConverter<DataType> {

    private final Map<Integer, Integer> structTypeHashToIndex = new HashMap<>();

    private final StructType containerType;

    private HapiContainedToSparkConverter(
        Map<String, StructureField<HapiConverter<DataType>>> contained,
        DataType dataType) {

      super(contained, dataType);

      this.containerType = (StructType) ((ArrayType) dataType).elementType();

      for (int i = 0; i < containerType.fields().length; i++) {

        structTypeHashToIndex.put(containerType.apply(i).dataType().hashCode(), i);
      }
    }

    @Override
    protected List<ContainerEntry> getContained(Object container) {

      WrappedArray containedArray = (WrappedArray) container;

      List<ContainerEntry> containedEntries = new ArrayList<>();

      for (int i = 0; i < containedArray.length(); i++) {

        GenericRowWithSchema resourceContainer = (GenericRowWithSchema) containedArray.apply(i);

        // The number of contained fields will be low, so this nested loop has low cost
        for (int j = 0; j < resourceContainer.schema().fields().length; j++) {

          if (resourceContainer.get(j) != null) {

            GenericRowWithSchema row = (GenericRowWithSchema) resourceContainer.get(j);
            String columnName = resourceContainer.schema().fields()[j].name();

            containedEntries.add(new ContainerEntry(columnName, row));

            break;
          }
        }
      }

      return containedEntries;
    }

    @Override
    protected Object createContained(Object[] contained) {

      GenericRowWithSchema[] containerArray = new GenericRowWithSchema[contained.length];

      for (int i = 0; i < contained.length; i++) {

        GenericRowWithSchema[] containerFields =
            new GenericRowWithSchema[structTypeHashToIndex.size()];

        GenericRowWithSchema containedRow = (GenericRowWithSchema) contained[i];
        int containedEntryStructTypeHash = containedRow.schema().hashCode();

        containerFields[structTypeHashToIndex.get(containedEntryStructTypeHash)] = containedRow;

        containerArray[i] = new GenericRowWithSchema(containerFields, containerType);
      }

      return containerArray;
    }
  }

  private static class MultiValuedToSparkConverter extends HapiConverter implements
      MultiValueConverter {

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
    public HapiConverter getElementConverter() {
      return elementConverter;
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

  private static HapiConverter DECIMAL_CONVERTER = new PrimitiveConverter("Decimal") {

    @Override
    public void toHapi(Object input, IPrimitiveType primitive) {

      primitive.setValueAsString(((BigDecimal) input).toPlainString());
    }

    @Override
    public Object getDataType() {
      return decimalType;
    }
  };

  private static final HapiConverter STRING_CONVERTER = new StringConverter(DataTypes.StringType);


  private static final HapiConverter ENUM_CONVERTER = new EnumConverter(DataTypes.StringType);

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

  private static final HapiConverter BOOLEAN_CONVERTER = new PrimitiveConverter<DataType>(
      "Boolean") {

    @Override
    public DataType getDataType() {
      return DataTypes.BooleanType;
    }
  };

  private static final HapiConverter INTEGER_CONVERTER = new PrimitiveConverter<DataType>(
      "Integer") {

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
      String elementPath, String baseType,
      String elementTypeUrl, List<StructureField<HapiConverter<DataType>>> children) {

    String recordName = DefinitionVisitorsUtil.recordNameFor(elementPath);
    String recordNamespace = DefinitionVisitorsUtil.namespaceFor(basePackage, elementTypeUrl);
    String fullName = recordNamespace + "." + recordName;

    HapiConverter<DataType> converter = visitedConverters.get(fullName);

    if (converter == null) {
      StructField[] fields = children.stream()
          .map(entry -> new StructField(entry.fieldName(),
              entry.result().getDataType(),
              true,
              Metadata.empty()))
          .toArray(StructField[]::new);

      converter = new HapiCompositeToSparkConverter(baseType,
          children, new StructType(fields), fhirSupport);

      visitedConverters.put(fullName, converter);
    }

    return converter;

  }

  @Override
  public HapiConverter visitContained(String elementPath,
      String elementTypeUrl,
      Map<String, StructureField<HapiConverter<DataType>>> contained) {

    StructField[] fields = contained.values()
        .stream()
        .map(containedEntry -> new StructField(containedEntry.fieldName(),
            containedEntry.result().getDataType(),
            true,
            Metadata.empty()))
        .toArray(StructField[]::new);

    ArrayType container = new ArrayType(new StructType(fields), true);

    return new HapiContainedToSparkConverter(contained, container);
  }

  @Override
  public HapiConverter visitReference(String elementName,
      List<String> referenceTypes,
      List<StructureField<HapiConverter<DataType>>> children) {

    String recordName = referenceTypes.stream().collect(Collectors.joining()) + "Reference";
    String fullName = basePackage + "." + recordName;

    HapiConverter<DataType> converter = visitedConverters.get(fullName);

    if (converter == null) {
      // Add direct references
      List<StructureField<HapiConverter<DataType>>> fieldsWithReferences =
          referenceTypes.stream()
              .map(refUri -> {

                String relativeType = refUri.substring(refUri.lastIndexOf('/') + 1);

                return new StructureField<HapiConverter<DataType>>("reference",
                    relativeType + "Id",
                    null,
                    false,
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

      converter = new HapiCompositeToSparkConverter(null,
          fieldsWithReferences,
          new StructType(fields), fhirSupport);

      visitedConverters.put(fullName, converter);
    }

    return converter;
  }

  @Override
  public HapiConverter visitParentExtension(String elementName,
      String extensionUrl,
      List<StructureField<HapiConverter<DataType>>> children) {

    // Ignore extension fields that don't have declared content for now.
    if (children.isEmpty()) {
      return null;
    }

    String recordNamespace = DefinitionVisitorsUtil.namespaceFor(basePackage, extensionUrl);

    String localPart = extensionUrl.substring(extensionUrl.lastIndexOf('/') + 1);

    String[] parts = localPart.split("[-|_]");

    String recordName = Arrays.stream(parts).map(part ->
        part.substring(0, 1).toUpperCase() + part.substring(1))
        .collect(Collectors.joining());

    String fullName = recordNamespace + "." + recordName;

    HapiConverter<DataType> converter = visitedConverters.get(fullName);

    if (converter == null) {
      StructField[] fields = children.stream()
          .map(entry ->
              new StructField(entry.fieldName(),
                  entry.result().getDataType(),
                  true,
                  Metadata.empty()))
          .toArray(StructField[]::new);

      converter = new HapiCompositeToSparkConverter(null,
          children,
          new StructType(fields),
          fhirSupport,
          extensionUrl);
    }

    return converter;
  }

  @Override
  public HapiConverter<DataType> visitLeafExtension(String elementName,
      String extensionUri,
      HapiConverter elementConverter) {

    return new LeafExtensionConverter<>(extensionUri, elementConverter);
  }

  @Override
  public HapiConverter<DataType> visitMultiValued(String elementName,
      HapiConverter arrayElement) {

    return new MultiValuedToSparkConverter(arrayElement);
  }

  @Override
  public HapiConverter<DataType> visitChoice(String elementName,
      Map<String, HapiConverter<DataType>> choiceTypes) {

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

    String fieldTypesString = choiceTypes.entrySet()
        .stream()
        .map(choiceEntry -> {

          // References need their full record name, which includes the permissible referent types
          if (choiceEntry.getKey().equals("Reference")) {

            StructType structType = (StructType) choiceEntry.getValue().getDataType();

            return Arrays.stream(structType.fields())
                .filter(field -> field.name().endsWith("Id") & !field.name().equals("id"))
                .map(field -> field.name().substring(0, field.name().lastIndexOf("Id")))
                .sorted()
                .map(StringUtils::capitalize)
                .collect(Collectors.joining());

          } else {

            return choiceEntry.getKey();
          }
        }).sorted()
        .map(StringUtils::capitalize)
        .collect(Collectors.joining());

    String fullName = basePackage + "." + "Choice" + fieldTypesString;

    HapiConverter<DataType> converter = visitedConverters.get(fullName);

    if (converter == null) {

      converter = new HapiChoiceToSparkConverter(choiceTypes,
          new StructType(fields),
          fhirSupport);

      visitedConverters.put(fullName, converter);
    }

    return converter;
  }

  @Override
  public int getMaxDepth(String elementTypeUrl, String path) {
    return 1;
  }
}
