/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VastMetadataUtils
{
    private static final byte[] EMPTY_MAP = "{}".getBytes(StandardCharsets.UTF_8);
    public static final String SORTED_BY_PROPERTY = "sorted_by";

    public String getPropertiesString(Map<String, Object> properties)
    {
        VastPayloadSerializer<Map> instanceForMap = VastPayloadSerializer.getInstanceForMap();
        return new String(instanceForMap.apply(properties).orElse(EMPTY_MAP), StandardCharsets.UTF_8);
    }

    public static List<Integer> colNamesToIndex(List<String> allColumnNames, List<String> columnNames)
    {
	List<Integer> rv = new ArrayList<>(columnNames.size());
	for (String n : columnNames) {
	    for (int i = 0; i < allColumnNames.size(); i++) {
		if (allColumnNames.get(i).equals(n)) {
		    rv.add(i);
		    break;
		}
	    }
	}
	return rv;
    }
}
