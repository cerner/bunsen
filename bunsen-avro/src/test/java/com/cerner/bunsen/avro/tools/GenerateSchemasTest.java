package com.cerner.bunsen.avro.tools;

import com.cerner.bunsen.stu3.TestData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class GenerateSchemasTest {

  @Test
  public void testWriteSchema() throws IOException {

    Path generatedCodePath = Files.createTempDirectory("schema_directory");

    generatedCodePath.toFile().deleteOnExit();

    Path outputFile = generatedCodePath.resolve("out.asvc");

    int result = GenerateSchemas.main(new String[]
        {outputFile.toString(),
            TestData.US_CORE_PATIENT,
            TestData.US_CORE_CONDITION,
            TestData.US_CORE_MEDICATION,
            TestData.US_CORE_MEDICATION_REQUEST});

    Assert.assertEquals(0, result);

    Assert.assertTrue(outputFile.toFile().exists());
  }
}
