/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.trino.tablefunction;

import com.google.common.base.VerifyException;
import org.junit.jupiter.api.Test;

import static com.vastdata.trino.tablefunction.VastConnectorTableFunctionHandle.GROUPS_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVastConnectorTableFunctionHandle
{
    @Test
    public void constructorShouldStoreQueryAndEnforceIdentity()
    {
        VastConnectorTableFunctionHandle handle = new VastConnectorTableFunctionHandle(
                "SELECT * FROM table", true);
        assertEquals("SELECT * FROM table", handle.query());
        assertTrue(handle.enforceIdentity());
    }

    @Test
    public void constructorShouldNotHandleEmptyQuery()
    {
        assertThrows(VerifyException.class,
                () -> new VastConnectorTableFunctionHandle("", true));
    }

    @Test
    public void constructorShouldNotHandleNullQuery()
    {
        assertThrows(RuntimeException.class,
                () -> new VastConnectorTableFunctionHandle(null, true));
    }

    @Test
    public void testReplacingGroups()
    {
        VastConnectorTableFunctionHandle handle = new VastConnectorTableFunctionHandle(
                "SELECT * FROM TABLE(vast.execute(\"select * from t where col in [<groups>]\")",
                true);
        String replacedQuery = handle.query().replaceAll(GROUPS_KEYWORD,
                "group1,group2");
        assertEquals(
                "SELECT * FROM TABLE(vast.execute(\"select * from t where col in [group1,group2]\")",
                replacedQuery);
    }

    @Test
    public void testMultipleReplacingGroups()
    {
        VastConnectorTableFunctionHandle handle = new VastConnectorTableFunctionHandle(
                "SELECT * FROM TABLE(vast.execute(\"select * from t where col in [<groups>] OR col2 in [<groups>]\")",
                true);
        String replacedQuery = handle.query().replaceAll(GROUPS_KEYWORD,
                "group1,group2");
        assertEquals(
                "SELECT * FROM TABLE(vast.execute(\"select * from t where col in [group1,group2] OR col2 in [group1,group2]\")",
                replacedQuery);
    }

    @Test
    public void identityPatternShouldMatchGroupsKeyword()
    {
        assertTrue(VastConnectorTableFunctionHandle.IDENTITY_PATTERN
                .matcher("some text <groups> more text")
                .matches());
    }
}
