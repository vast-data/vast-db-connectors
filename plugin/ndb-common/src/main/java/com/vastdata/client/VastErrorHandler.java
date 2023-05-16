/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client;

import com.google.common.base.Strings;
import io.airlift.log.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.lang.String.format;

public class VastErrorHandler
        extends DefaultHandler
{
    private static final Logger LOG = Logger.get(VastErrorHandler.class);
    private final StringBuilder resultBuilder = new StringBuilder();
    private final StringBuilder text = new StringBuilder();
    private static final Map<String, BiConsumer<StringBuilder, String>> fieldHandlers = new HashMap<>();
    private static final BiConsumer<StringBuilder, String> DO_NOTHING = (builder, value) -> {};
    private static BiConsumer<StringBuilder, String> getEntryAdder(String entryName)
    {
        return (builder, value) -> {
            if (!Strings.isNullOrEmpty(value)) {
                builder.append(format("%s: %s. ", entryName, value));
            }
        };
    }

    public static final String ERROR = "Error";

    public static final String MESSAGE = "Message";

    public static final String RESOURCE = "Resource";

    public static final String CODE = "Code";

    public static final String REQUEST_ID = "RequestId";

    static {
        fieldHandlers.put(ERROR, DO_NOTHING);
        fieldHandlers.put(MESSAGE, getEntryAdder(MESSAGE));
        fieldHandlers.put(RESOURCE, getEntryAdder(RESOURCE));
        fieldHandlers.put(CODE, getEntryAdder(CODE));
        fieldHandlers.put(REQUEST_ID, getEntryAdder(REQUEST_ID));
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
    {
        text.setLength(0);
    }

    @Override
    public void endDocument()
    {
        text.setLength(0);
    }

    @Override
    public void endElement(String uri, String localName, String qName)
    {
        String entryValue = sanitize(text.toString());
        if (!Strings.isNullOrEmpty(entryValue)) {
            if (!fieldHandlers.containsKey(localName)) {
                LOG.warn("Unexpected key: %s, value: %s", localName, entryValue);
            }
            else {
                fieldHandlers.get(localName).accept(resultBuilder, entryValue);
            }
        }
    }

    private String sanitize(String rawValue)
    {
        if (Strings.isNullOrEmpty(rawValue)) {
            return null;
        }
        else {
            if (rawValue.endsWith(".")) {
                // in case string ends with a dot, like in "Your key is too long.", in order to avoid printing "Your key is too long.."
                return rawValue.substring(0, rawValue.length() - 1);
            }
            else {
                return rawValue;
            }
        }
    }

    @Override
    public final void characters(char[] ch, int start, int length)
    {
        text.append(ch, start, length);
    }

    public String getMessage()
    {
        return resultBuilder.toString();
    }
}
