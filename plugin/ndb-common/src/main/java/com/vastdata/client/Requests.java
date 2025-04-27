/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

enum Requests
{
    CREATE_SCHEMA("schema"),
    DROP_SCHEMA("schema"),
    LIST_SCHEMA("schema"),
    ALTER_SCHEMA("schema"),
    LIST_TABLES("table"),
    LIST_VIEWS("view"),
    CREATE_TABLE("table"),
    CREATE_VIEW("view"),
    VIEW_DETAILS("view-details"),
    DROP_TABLE("table"),
    DROP_VIEW("view"),
    ALTER_TABLE("table"),
    ADD_COLUMN("column"),
    DROP_COLUMN("column"),
    LIST_COLUMNS("columns"),
    ALTER_COLUMNS("columns"),
    IMPORT_DATA("data"),
    QUERY_DATA("data"),
    INSERT_ROWS("rows"),
    DELETE_ROWS("rows"),
    UPDATE_ROWS("rows"),
    START_TRANSACTION("transaction"),
    ROLLBACK_TRANSACTION("transaction"),
    COMMIT_TRANSACTION("transaction"),
    GET_TRANSACTION("transaction"),
    GET_SCHEDULING_INFO("schedule"),
    GET_TABLE_STATS("stats");

    private final String requestName;

    Requests(String requestName)
    {
        this.requestName = requestName;
    }

    String getRequestParam()
    {
        return requestName;
    }
}
