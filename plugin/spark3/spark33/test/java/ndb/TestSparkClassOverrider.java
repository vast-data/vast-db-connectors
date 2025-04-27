/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class TestSparkClassOverrider
{
//    disabled because when running all tests together Filter class is already loaded in bootstrap class loader. Test works only if ran alone
    @Test(enabled = false)
    public void testOverrideV2Relation()
            throws NotFoundException, CannotCompileException, NoSuchMethodException
    {
        SparkClassOverrider.overrideV2Relation();
        DataSourceV2Relation dataSourceV2Relation = new DataSourceV2Relation(null, null, null, null, null);
        assertNotNull(dataSourceV2Relation.getClass().getMethod("overrideWrapper"));
    }
}
