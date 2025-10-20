package com.vastdata.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;

public class TableAlreadyExistsException extends TrinoException {
    private final SchemaTableName schemaTableName;

    public TableAlreadyExistsException(final SchemaTableName schemaTableName)
    {
        this(schemaTableName, null);
    }

    public TableAlreadyExistsException(final SchemaTableName schemaTableName, final Throwable cause)
    {
        this(schemaTableName, format("Table already exists: '%s'", schemaTableName), cause);
    }

    public TableAlreadyExistsException(final SchemaTableName schemaTableName, final String message, final Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.schemaTableName = schemaTableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }
}
