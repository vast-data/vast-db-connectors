/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.connector.catalog.Table;

import java.util.function.Predicate;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;

public class ComplexRowIDPredicate
        implements Predicate<Table>
{
    @Override
    public boolean test(Table table)
    {
        boolean partitioned = table.partitioning() != null && table.partitioning().length > 0;
        return partitioned || table.properties().containsKey(
                SORTED_BY_PROPERTY);
    }
}
