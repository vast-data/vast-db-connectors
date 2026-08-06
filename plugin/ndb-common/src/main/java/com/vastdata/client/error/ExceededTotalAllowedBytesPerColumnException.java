/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.error;

public class ExceededTotalAllowedBytesPerColumnException
        extends VastRuntimeException
{
    private final int numOfRowsSuggestion;

    public ExceededTotalAllowedBytesPerColumnException(String message, int numOfRowsSuggestion)
    {
        super(message, ErrorType.CLIENT);

        this.numOfRowsSuggestion = numOfRowsSuggestion;
    }

    public int getNumOfRowsSuggestion()
    {
        return numOfRowsSuggestion;
    }
}
