package com.vastdata.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;

public class ViewAlreadyExistsException extends TrinoException {
    private final SchemaTableName schemaTableName;

    public ViewAlreadyExistsException(final SchemaTableName schemaTableName)
    {
        this(schemaTableName, null);
    }

    public ViewAlreadyExistsException(final SchemaTableName schemaTableName, final Throwable cause)
    {
        this(schemaTableName, format("View already exists: '%s'", schemaTableName), cause);
    }

    public ViewAlreadyExistsException(final SchemaTableName schemaTableName, final String message, final Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.schemaTableName = schemaTableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }
}
