/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import vast_flatbuf.tabular.ImportDataRequest;
import vast_flatbuf.tabular.S3File;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArrowSchemaUtils
{
    @Test(dataProvider = "numOfFiles")
    public void testNewImportDataRequestMultipleFiles(int numOfFiles)
    {
        String bucket = "bucket";
        String fileNamePrefix = "src/file";
        String colAName = "colA";
        String colBName = "colB";
        Field colAField = new Field(colAName, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Field colBField = new Field(colBName, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null);
        Map<String, Field> fieldsMap = ImmutableMap.of(colAName, colAField, colBName, colBField);

        List<String> resultFileNameExpectation = new ArrayList<>(numOfFiles);
        List<ImportDataFile> importDataFiles = new ArrayList<>(numOfFiles);
        for (int i = 0; i < numOfFiles; i++) {
            String fileName = format("%s%d", fileNamePrefix, i);
            String valName = format("val%d", i);
            resultFileNameExpectation.add(fileName);
            ImportDataFile importFile = new ImportDataFile(bucket, fileName, ImmutableMap.of(colAName, valName, colBName, valName));
            importFile.setFieldsDefaultValues(fieldsMap);
            importDataFiles.add(importFile);
        }

        ByteBuffer byteBuffer = new ArrowSchemaUtils().newImportDataRequest(
                new ImportDataContext(importDataFiles, "bucket/schema/table"),
                new RootAllocator());
        ImportDataRequest root = ImportDataRequest.getRootAsImportDataRequest(byteBuffer);
        List<String> collectedFiles = new ArrayList<>();
        for (int i = 0; i < root.s3FilesLength(); i++) {
            S3File s3File = root.s3Files(i);
            String fileName = s3File.fileName();
            collectedFiles.add(fileName);
            assertTrue(s3File.partitionsLength() > 0, "Expected to have partitions vector");
        }
        collectedFiles.sort(String::compareTo);
        assertEquals(collectedFiles, resultFileNameExpectation);
    }

    @DataProvider
    public Object[][] numOfFiles()
    {
        return new Object[][] {{1}, {4}};
    }
}
