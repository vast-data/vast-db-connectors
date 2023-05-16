/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.mockserver;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MockListBucketsReply
        implements Function<Map<String, Set<MockMapSchema>>, String>
{
    public static final String LIST_BUCKETS_REPLY_PREFIX = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>\n" +
            "100</ID><DisplayName>vast-user</DisplayName></Owner><Buckets>";
    public static final String LIST_BUCKETS_REPLY_SUFFIX = "</Buckets></ListAllMyBucketsResult>";
    private static final String BUCKET_ENTRY_FORMAT = "<Bucket><Name>%s</Name><CreationDate>2022-04-14T07:15:51.000Z</CreationDate></Bucket>";
    //    <?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>
    //100</ID><DisplayName>vast-user</DisplayName></Owner><Buckets><Bucket><Name>bucket-for-tabular-api-create-schema</Name><CreationDate>2022-04-14T07:15:51.000Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>

    @Override
    public String apply(Map<String, Set<MockMapSchema>> schema)
    {
        StringBuilder sb = new StringBuilder(LIST_BUCKETS_REPLY_PREFIX);
        schema.keySet().forEach(k -> sb.append(String.format(BUCKET_ENTRY_FORMAT, k)));
        sb.append(LIST_BUCKETS_REPLY_SUFFIX);
        return sb.toString();
    }
}
