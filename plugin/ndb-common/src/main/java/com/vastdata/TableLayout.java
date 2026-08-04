/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata;

import com.vastdata.client.partition.PartitionColumnMetadata;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.function.Function;

public final class TableLayout
{
    public static final TableLayout EMPTY = new TableLayout(
            new Schema(List.of()), List.of(), List.of());

    private final Schema schema;
    private final List<Field> sortedColumns;
    private final List<PartitionColumnMetadata> partitionColumnMetadata;

    public TableLayout(Schema schema, List<Field> sortedColumns,
            List<PartitionColumnMetadata> partitionColumnMetadata)
    {
        if (TableLayout.hasDuplicateNames(schema.getFields(), Field::getName)) {
            throw new IllegalArgumentException(
                    "Schema contains duplicate columns: " + schema.getFields());
        }

        if (TableLayout.hasDuplicateNames(sortedColumns, Field::getName)) {
            throw new IllegalArgumentException(
                    "Sorted columns contain duplicate columns: " + sortedColumns);
        }

        if (TableLayout.hasDuplicateNames(partitionColumnMetadata,
                PartitionColumnMetadata::getSourceColumnName)) {
            throw new IllegalArgumentException(
                    "Partitioning metadata contain duplicate source columns: " + partitionColumnMetadata);
        }

        if (TableLayout.hasDuplicateNames(partitionColumnMetadata,
                PartitionColumnMetadata::getColumnName)) {
            throw new IllegalArgumentException(
                    "Partitioning metadata contain duplicate columns: " + partitionColumnMetadata);
        }

        partitionColumnMetadata.forEach(
                metadata -> schema.findField(metadata.getSourceColumnName()));

        this.schema = schema;
        this.sortedColumns = List.copyOf(sortedColumns);
        this.partitionColumnMetadata = List.copyOf(partitionColumnMetadata);
    }

    public static TableLayout regularTable(Schema schema)
    {
        return new TableLayout(schema, List.of(), List.of());
    }

    private static <T> boolean hasDuplicateNames(List<T> elements,
            Function<T, String> toNameConvertor)
    {
        return elements
                .stream()
                .map(toNameConvertor)
                .distinct()
                .count() != elements.size();
    }

    public Schema getSchema()
    {
        return this.schema;
    }

    public boolean hasSortedColumns()
    {
        return !this.sortedColumns.isEmpty();
    }

    public List<Field> getSortedColumns()
    {
        return this.sortedColumns;
    }

    public boolean hasPartitionColumns()
    {
        return !this.partitionColumnMetadata.isEmpty();
    }

    public List<PartitionColumnMetadata> getPartitionColumnsMetadata()
    {
        return this.partitionColumnMetadata;
    }

    @Override
    public String toString()
    {
        return "Schema: " + this.schema.getFields() + "\nSortedColumns: " + getSortedColumns() + "\nPartitionColumns: " + this.partitionColumnMetadata;
    }
}
