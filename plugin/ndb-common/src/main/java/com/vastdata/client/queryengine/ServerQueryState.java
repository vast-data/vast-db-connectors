/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata.client.queryengine;

public enum ServerQueryState
{
    Invalid(0), Init(1), InProgress(2), Completed(3);

    private final int ipcValue;

    ServerQueryState(int ipcValue)
    {
        this.ipcValue = ipcValue;
    }

    public static ServerQueryState fromIpcValue(int ipcValue)
    {
        for (ServerQueryState state : ServerQueryState.values()) {
            if (state.ipcValue == ipcValue) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown IPC value: " + ipcValue);
    }
}
