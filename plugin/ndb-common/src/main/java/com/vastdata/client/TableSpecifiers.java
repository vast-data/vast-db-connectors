/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TableSpecifiers
{
    public static final String IMPORT_DATA_SPECIFIER = "vast.import_data";
    public static final String NON_ACID_SPECIFIER = "vast.allow_non_acid";

    private static final Pattern IMPORT_DATA_PATTERN = Pattern.compile(
            "\\s+vast\\.import_data(?:\\(([^)]*)\\))?");
    private static final Pattern NON_ACID_PATTERN = Pattern.compile(
            "\\s+vast\\.allow_non_acid");

    private final String tableName;
    private final boolean forNonAcidOperation;
    private final boolean forImportDataOperation;
    private final Set<String> importDataColumns;

    private TableSpecifiers(String tableName, boolean nonAcidAllowed,
            boolean forImportData, Set<String> importDataColumns)
    {
        this.tableName = tableName;
        this.forNonAcidOperation = nonAcidAllowed;
        this.forImportDataOperation = forImportData;
        this.importDataColumns = importDataColumns;
    }

    /**
     * parses "<tableName> <specifier-1> <specifier-2> ... <specifier-n>" to the
     * tableName and the available specifiers
     */
    public static TableSpecifiers parse(String tableName)
    {
        requireNonNull(tableName, "tableName cannot be null");

        boolean nonAcidAllowed = false;
        boolean forImportData = false;
        Set<String> importDataColumns = null;
        String parsedName = tableName.trim();

        Matcher importMatcher = IMPORT_DATA_PATTERN.matcher(parsedName);
        if (importMatcher.find()) {
            forImportData = true;
            String colsGroup = importMatcher.group(1);
            if (colsGroup != null) {
                importDataColumns = Arrays
                        .stream(colsGroup.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toSet());
            }
            parsedName = importMatcher.replaceAll("");
        }

        Matcher nonAcidMatcher = NON_ACID_PATTERN.matcher(parsedName);
        if (nonAcidMatcher.find()) {
            nonAcidAllowed = true;
            parsedName = nonAcidMatcher.replaceAll("");
        }

        return new TableSpecifiers(parsedName.trim(), nonAcidAllowed,
                forImportData, importDataColumns);
    }

    public static <E extends Exception> String parseUnspecifiedTableName(
            String tableName, E errIfHasSpecifiers)
            throws E
    {
        TableSpecifiers specifiers = parse(tableName);
        if (specifiers.hasOperationSpecifiers()) {
            throw errIfHasSpecifiers;
        }
        return specifiers.getTableName();
    }

    public String getTableName()
    {
        return tableName;
    }

    public boolean isForNonAcidOperation()
    {
        return forNonAcidOperation;
    }

    public boolean isForImportDataOperation()
    {
        return forImportDataOperation;
    }

    public Set<String> getImportDataColumns()
    {
        return importDataColumns;
    }

    public boolean hasOperationSpecifiers()
    {
        return forImportDataOperation || forNonAcidOperation;
    }
}
