package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;
import ca.uhn.fhir.context.RuntimeElemContainedResourceList;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiFieldSetter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.StructureField;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.hl7.fhir.instance.model.api.IAnyResource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseExtension;
import org.hl7.fhir.instance.model.api.IBaseHasExtensions;
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

  private static class LeafExtensionToSparkConverter extends HapiConverter {

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

    private final HapiConverter<DataType> valueConverter;

    LeafExtensionToSparkConverter(String extensionUrl, HapiConverter valueConverter) {

      this.extensionUrl = extensionUrl;
      this.valueConverter = valueConverter;
    }

    @Override
    public Object fromHapi(Object input) {

      IBaseExtension extension = (IBaseExtension) input;

      return valueConverter.fromHapi(extension.getValue());
    }

    @Override
    public DataType getDataType() {
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

  private static class ChoiceToSparkConverter extends HapiConverter {

    private class ChoiceFieldSetter implements HapiFieldSetter {

      private final Map<String,HapiFieldSetter> choiceFieldSetters;

      ChoiceFieldSetter(Map<String,HapiFieldSetter> choiceFieldSetters) {
        this.choiceFieldSetters = choiceFieldSetters;
      }

      @Override
      public void setField(IBase parentObject,
          BaseRuntimeChildDefinition fieldToSet,
          Object sparkObject) {

        Row row = ((Row) sparkObject);

        Iterator<Map.Entry<String,HapiFieldSetter>> setterIterator =
            choiceFieldSetters.entrySet().iterator();

        // Co-iterate with an index so we place the correct values into the corresponding locations.
        for (int valueIndex = 0; valueIndex < choiceTypes.size(); ++valueIndex) {

          Map.Entry<String, HapiFieldSetter> setterEntry = setterIterator.next();

          if (row.get(valueIndex) != null) {

            HapiFieldSetter setter = setterEntry.getValue();

            setter.setField(parentObject, fieldToSet, row.get(valueIndex));

            // We set a non-null field for the choice type, so stop looking.
            break;
          }
        }
      }
    }

    private final Map<String,HapiConverter<DataType>> choiceTypes;

    private final StructType structType;

    private final FhirConversionSupport fhirSupport;

    ChoiceToSparkConverter(Map<String,HapiConverter<DataType>> choiceTypes,
        StructType structType,
        FhirConversionSupport fhirSupport) {
      this.choiceTypes = choiceTypes;
      this.structType = structType;
      this.fhirSupport = fhirSupport;
    }

    @Override
    public Object fromHapi(Object input) {

      String fhirType = fhirSupport.fhirType((IBase) input);

      Object[] values = new Object[choiceTypes.size()];

      Iterator<Map.Entry<String,HapiConverter<DataType>>> schemaIterator =
          choiceTypes.entrySet().iterator();

      // Co-iterate with an index so we place the correct values into the corresponding locations.
      for (int valueIndex = 0; valueIndex < choiceTypes.size(); ++valueIndex) {

        Map.Entry<String,HapiConverter<DataType>> choiceEntry = schemaIterator.next();

        // Set the nested field that matches the choice type.
        if (choiceEntry.getKey().equals(fhirType)) {

          HapiConverter converter = choiceEntry.getValue();

          values[valueIndex] = converter.fromHapi(input);
        }

      }

      return RowFactory.create(values);
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      Map<String,HapiFieldSetter> fieldSetters = new LinkedHashMap<>();

      for (Map.Entry<String,HapiConverter<DataType>> choiceEntry: choiceTypes.entrySet()) {

        // The list is small and only consumed when generating the conversion functions,
        // so a nested loop isn't a performance issue.
        for (BaseRuntimeElementDefinition elementDefinition: elementDefinitions) {

          if (elementDefinition.getName().equals(choiceEntry.getKey())) {

            fieldSetters.put(choiceEntry.getKey(),
                choiceEntry.getValue().toHapiConverter(elementDefinition));
          }
        }
      }

      return new ChoiceFieldSetter(fieldSetters);
    }

    @Override
    public DataType getDataType() {
      return structType;
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


  private static class CompositeToSparkConverter extends HapiConverter<DataType> {

    private final String elementType;

    private final List<StructureField<HapiConverter<DataType>>> children;

    private final StructType structType;

    private final String extensionUrl;

    private final FhirConversionSupport fhirSupport;

    private class CompositeFieldSetter implements HapiFieldSetter,
        HapiObjectConverter {

      private final List<StructureField<HapiFieldSetter>> children;


      private final BaseRuntimeElementCompositeDefinition compositeDefinition;

      CompositeFieldSetter(BaseRuntimeElementCompositeDefinition compositeDefinition,
          List<StructureField<HapiFieldSetter>> children) {
        this.compositeDefinition = compositeDefinition;
        this.children = children;
      }

      @Override
      public IBase toHapi(Object rowObject) {

        IBase fhirObject = compositeDefinition.newInstance();

        Row row = (Row) rowObject;

        // TODO: interrogate schema to obtain fields in case they are somehow reordered?

        // Rows may be larger than the expected HAPI structure in case they
        // include added columns.
        if (row.size() < children.size()) {
          throw new IllegalStateException("Unexpected row during deserialization "
              + row.toString());
        }

        Iterator<StructureField<HapiFieldSetter>> childIterator = children.iterator();

        for (int fieldIndex = 0; fieldIndex < children.size(); ++fieldIndex) {

          StructureField<HapiFieldSetter> child = childIterator.next();

          // Some children are ignored, for instance when terminating recursive
          // fields.
          if (child == null || child.result() == null) {
            continue;
          }

          Object fieldValue = row.get(fieldIndex);

          if (fieldValue != null) {

            if (child.extensionUrl() != null) {

              BaseRuntimeChildDefinition childDefinition =
                  compositeDefinition.getChildByName("extension");

              child.result().setField(fhirObject, childDefinition, fieldValue);

            } else {

              String propertyName = child.isChoice()
                  ? child.propertyName() + "[x]"
                  : child.propertyName();

              BaseRuntimeChildDefinition childDefinition =
                  compositeDefinition.getChildByName(propertyName);

              child.result().setField(fhirObject, childDefinition, fieldValue);

            }
          }
        }

        if (extensionUrl != null) {

          ((IBaseExtension) fhirObject).setUrl(extensionUrl);
        }

        return fhirObject;
      }

      @Override
      public void setField(IBase parentObject,
          BaseRuntimeChildDefinition fieldToSet,
          Object sparkObject) {

        Row row = (Row) sparkObject;

        IBase fhirObject = toHapi(row);

        if (extensionUrl != null) {

          fieldToSet.getMutator().addValue(parentObject, fhirObject);

        } else {
          fieldToSet.getMutator().setValue(parentObject, fhirObject);
        }

      }
    }

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

      this.elementType = elementType;
      this.children = children;
      this.structType = structType;
      this.extensionUrl = extensionUrl;
      this.fhirSupport = fhirSupport;
    }

    @Override
    public Object fromHapi(Object input) {

      IBase composite = (IBase) input;

      Object[] values = new Object[children.size()];

      if (composite instanceof IAnyResource) {

        values[0] = ((IAnyResource) composite).getIdElement().getValueAsString();
      }

      Map<String,List> properties = fhirSupport.compositeValues(composite);

      Iterator<StructureField<HapiConverter<DataType>>> schemaIterator
          = children.iterator();

      // Co-iterate with an index so we place the correct values into the corresponding locations.
      for (int valueIndex = 0; valueIndex < children.size(); ++valueIndex) {

        StructureField<HapiConverter<DataType>> schemaEntry = schemaIterator.next();

        String propertyName = schemaEntry.propertyName();

        // Append the [x] suffix for choice properties.
        if (schemaEntry.isChoice()) {
          propertyName = propertyName + "[x]";
        }

        HapiConverter converter = schemaEntry.result();

        List propertyValues = properties.get(propertyName);

        if (propertyValues != null && !propertyValues.isEmpty()) {

          if (converter.getDataType() instanceof ArrayType) {

            values[valueIndex] = schemaEntry.result().fromHapi(propertyValues);

          } else {

            values[valueIndex] = schemaEntry.result().fromHapi(propertyValues.get(0));
          }
        } else if (converter.extensionUrl() != null) {

          // No corresponding property for the name, so see if it is an extension.
          List<? extends IBaseExtension> extensions =
              ((IBaseHasExtensions) composite).getExtension();

          for (IBaseExtension extension: extensions) {

            if (extension.getUrl().equals(converter.extensionUrl())) {

              values[valueIndex] = schemaEntry.result().fromHapi(extension);
            }
          }
        }
      }

      return RowFactory.create(values);
    }


    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      BaseRuntimeElementDefinition elementDefinition = elementDefinitions[0];

      if (elementDefinition instanceof RuntimeElemContainedResourceList) {
        return NOOP_FIELD_SETTER;
      }

      BaseRuntimeElementCompositeDefinition compositeDefinition =
          (BaseRuntimeElementCompositeDefinition) elementDefinition;

      List<StructureField<HapiFieldSetter>> toHapiChildren = children.stream().map(child -> {

        HapiFieldSetter childConverter;

        // Handle extensions.
        if (child.extensionUrl() != null) {

          BaseRuntimeChildDefinition childDefinition =
              compositeDefinition.getChildByName("extension");

          childConverter = child.result()
              .toHapiConverter(childDefinition.getChildByName("extension"));

        } else {

          String propertyName = child.propertyName();

          // Append the [x] suffix for choice properties.
          if (child.isChoice()) {

            propertyName = propertyName + "[x]";
          }

          BaseRuntimeChildDefinition childDefinition =
              compositeDefinition.getChildByName(propertyName);

          BaseRuntimeElementDefinition[] childElementDefinitions;

          if (child.isChoice()) {

            int childCount = childDefinition.getValidChildNames().size();

            childElementDefinitions = new BaseRuntimeElementDefinition[childCount];

            int index = 0;

            for (String childName: childDefinition.getValidChildNames()) {

              childDefinition.getChildByName(childName);

              childElementDefinitions[index++] = childDefinition.getChildByName(childName);
            }

          } else {

            childElementDefinitions = new BaseRuntimeElementDefinition[] {
                childDefinition.getChildByName(propertyName)
            };
          }

          childConverter = child.result().toHapiConverter(childElementDefinitions);
        }

        return new StructureField<HapiFieldSetter>(child.propertyName(),
            child.fieldName(),
            child.extensionUrl(),
            child.isChoice(),
            childConverter);

      }).collect(Collectors.toList());

      return new CompositeFieldSetter(compositeDefinition, toHapiChildren);
    }

    @Override
    public DataType getDataType() {
      return structType;
    }

    @Override
    public String extensionUrl() {
      return extensionUrl;
    }

    @Override
    public String getElementType() {
      return elementType;
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


  private static class SparkDecimalToHapi implements HapiFieldSetter,
      HapiObjectConverter {

    private final BaseRuntimeElementDefinition elementDefinition;

    SparkDecimalToHapi(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      fieldToSet.getMutator().setValue(parentObject, toHapi(sparkObject));
    }

    @Override
    public IBase toHapi(Object sparkObject) {

      IPrimitiveType element = (IPrimitiveType) elementDefinition.newInstance();

      element.setValueAsString(((BigDecimal) sparkObject).toPlainString());

      return element;
    }
  }

  private static class DecimalConverter extends HapiConverter<DataType> {

    @Override
    public Object fromHapi(Object input) {

      return ((IPrimitiveType) input).getValue();
    }

    @Override
    public DataType getDataType() {
      return decimalType;
    }

    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkDecimalToHapi(elementDefinitions[0]);
    }
  }

  private static HapiConverter DECIMAL_CONVERTER = new DecimalConverter();


  private static class SparkStringToHapi implements HapiFieldSetter,
      HapiObjectConverter {

    private final BaseRuntimeElementDefinition elementDefinition;

    SparkStringToHapi(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      fieldToSet.getMutator().setValue(parentObject, toHapi(sparkObject));
    }

    @Override
    public IBase toHapi(Object sparkObject) {

      IPrimitiveType element = (IPrimitiveType) elementDefinition.newInstance();

      element.setValueAsString((String) sparkObject);

      return element;
    }
  }

  private static class SparkBooleanToHapi implements HapiFieldSetter {

    private final BaseRuntimeElementDefinition elementDefinition;

    SparkBooleanToHapi(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      IPrimitiveType<Boolean> element = (IPrimitiveType<Boolean>) elementDefinition.newInstance();

      element.setValue((Boolean) sparkObject);

      fieldToSet.getMutator().setValue(parentObject, element);

    }
  }

  private static class SparkIntegerToHapi implements HapiFieldSetter {

    private final BaseRuntimeElementDefinition elementDefinition;

    SparkIntegerToHapi(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      IPrimitiveType<Integer> element = (IPrimitiveType<Integer>) elementDefinition.newInstance();

      element.setValue((Integer) sparkObject);

      fieldToSet.getMutator().setValue(parentObject, element);

    }
  }


  private static class StringConverter extends HapiConverter {

    @Override
    public Object fromHapi(Object input) {
      return ((IPrimitiveType) input).getValueAsString();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.StringType;
    }

    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkStringToHapi(elementDefinitions[0]);
    }

    @Override
    public String getElementType() {

      return "String";
    }
  }

  private static final HapiConverter STRING_CONVERTER = new StringConverter();


  private static class SparkEnumToHapi implements HapiFieldSetter,
      HapiObjectConverter {

    private final BaseRuntimeElementDefinition elementDefinition;

    SparkEnumToHapi(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject,
        BaseRuntimeChildDefinition fieldToSet,
        Object sparkObject) {

      // The enumerated value must be set from the runtime child definition
      // to initialize the Java enum itself.
      IPrimitiveType element = (IPrimitiveType) elementDefinition
          .newInstance(fieldToSet.getInstanceConstructorArguments());

      element.setValueAsString((String) sparkObject);

      fieldToSet.getMutator().setValue(parentObject, element);
    }

    @Override
    public IBase toHapi(Object sparkObject) {

      IPrimitiveType element = (IPrimitiveType) elementDefinition.newInstance();

      element.setValueAsString((String) sparkObject);

      return element;
    }
  }


  private static class EnumConverter extends HapiConverter {

    @Override
    public Object fromHapi(Object input) {
      return ((IPrimitiveType) input).getValueAsString();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.StringType;
    }

    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkEnumToHapi(elementDefinitions[0]);
    }

    @Override
    public String getElementType() {

      return "String";
    }
  }

  private static final HapiConverter ENUM_CONVERTER = new EnumConverter();

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

  private static final HapiConverter DATE_CONVERTER = new HapiConverter() {

    @Override
    public Object fromHapi(Object input) {
      return ((IPrimitiveType) input).getValueAsString();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.StringType;
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkStringToHapi(elementDefinitions[0]);
    }
  };

  private static final HapiConverter BOOLEAN_CONVERTER = new HapiConverter<DataType>() {

    @Override
    public Object fromHapi(Object input) {

      return ((IPrimitiveType<Boolean>) input).getValue();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.BooleanType;
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkBooleanToHapi(elementDefinitions[0]);
    }
  };

  private static final HapiConverter INTEGER_CONVERTER = new HapiConverter<DataType>() {

    @Override
    public Object fromHapi(Object input) {
      return ((IPrimitiveType) input).getValue();
    }

    @Override
    public DataType getDataType() {
      return DataTypes.IntegerType;
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      return new SparkIntegerToHapi(elementDefinitions[0]);
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

    return new LeafExtensionToSparkConverter(extensionUri, elementConverter);
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
