package com.vastdata;

import java.util.OptionalLong;
import java.util.function.UnaryOperator;

public class OptionalPrimitiveHelpers {
    public static OptionalLong map(final OptionalLong optional, final UnaryOperator<Long> f) {
        return optional.isPresent() ? OptionalLong.of(f.apply(optional.getAsLong())) : OptionalLong.empty();
    }
}
