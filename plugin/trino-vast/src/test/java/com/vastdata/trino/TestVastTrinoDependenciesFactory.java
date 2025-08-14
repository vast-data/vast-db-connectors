/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import com.google.common.collect.Multimap;
import com.vastdata.client.RequestsHeaders;
import com.vastdata.client.VastRequestHeadersBuilder;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.trino.tx.VastTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestVastTrinoDependenciesFactory
{
    private static final String DEFAULT_END_USER = "default-end-user";

    @Test
    public void testSchemaNameValidator()
    {
        Predicate<String> unit = new VastTrinoDependenciesFactory().getSchemaNameValidator();
        assertFalse(unit.test("shouldFail"));
        assertTrue(unit.test("should/succeed"));
        assertTrue(unit.test("should/succeed/too"));
    }

    private void assertMapKeyValue(Multimap<String, String> map, String key, String expectedVal)
    {
        assertTrue(map.containsKey(key), String.format("Map does not contain key: %s", key));
        Collection<String> strings = map.get(key);
        assertEquals(strings.size(), 1, String.format("Size of values collections is not 1 for key: %s", key));
        Object actualVal = strings.toArray()[0];
        assertEquals(actualVal, expectedVal, String.format("Map contains a different value for key %s. Expected %s, got %s", key, expectedVal, actualVal));
    }

    @Test
    public void testDefaultHeaders()
    {
        VastRequestHeadersBuilder unit = new VastTrinoDependenciesFactory().getHeadersFactory(DEFAULT_END_USER);
        Multimap<String, String> headers = unit.build();

        assertMapKeyValue(headers, RequestsHeaders.TABULAR_API_VERSION_ID.getHeaderName(), VastRequestHeadersBuilder.VAST_CLIENT_API_VERSION);

        assertFalse(headers.containsKey(RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName()));
    }

    @Test
    public void testAllHeaders()
    {
        VastRequestHeadersBuilder unit = new VastTrinoDependenciesFactory().getHeadersFactory(DEFAULT_END_USER);
        String testTxidStr = "514026084031791104";
        VastTransaction testTx = new VastTransactionHandle(Long.parseUnsignedLong(testTxidStr));
        unit.withTransaction(testTx);
        Long testNextKey = 777L;
        unit.withNextKey(testNextKey);
        Multimap<String, String> headers = unit.build();

        assertMapKeyValue(headers, RequestsHeaders.TABULAR_API_VERSION_ID.getHeaderName(), VastRequestHeadersBuilder.VAST_CLIENT_API_VERSION);

        assertMapKeyValue(headers, RequestsHeaders.TABULAR_NEXT_KEY.getHeaderName(), String.valueOf(testNextKey));
        assertMapKeyValue(headers, RequestsHeaders.TABULAR_TRANSACTION_ID.getHeaderName(), testTxidStr);
    }
}
