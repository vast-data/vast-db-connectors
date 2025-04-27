package com.vastdata.spark;

import com.vastdata.spark.adaptor.SparkVectorAdaptorFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

class AbstractEmptyBatchSupplier<T, V>
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEmptyBatchSupplier.class);

    private final String traceStr;
    private final Schema schema;
    private final AtomicBoolean hasNext = new AtomicBoolean(true);
    private final Function<FieldVector, V> vectorFunction;
    private final BiFunction<VectorSchemaRoot, Function<FieldVector, V>, T> resultFunction;
    private static final SparkVectorAdaptorFactory vectorAdaptorFactory = new SparkVectorAdaptorFactory();

    AbstractEmptyBatchSupplier(String traceStr, Schema schema, Function<FieldVector, V> vectorFunction, BiFunction<VectorSchemaRoot, Function<FieldVector, V>, T> resultFunction)
    {
        this.traceStr = traceStr;
        this.schema = schema;
        this.vectorFunction = vectorFunction;
        this.resultFunction = resultFunction;
    }

    protected boolean hasNext()
    {
        boolean hasNext = this.hasNext.getAndSet(false);
        LOG.debug("{} hasNext: {}", this.traceStr, hasNext);
        return hasNext;
    }

    public T supply()
    {
        RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        Function<FieldVector, V> mapper = getVectorAdaptor(allocator).andThen(vectorFunction);
        T result = resultFunction.apply(root, mapper);
        LOG.debug("{} Returning empty page: {}", traceStr, result);
        return result;
    }

    static Function<FieldVector, FieldVector> getVectorAdaptor(RootAllocator allocator)
    {
        return vector -> vectorAdaptorFactory.forField(vector.getField()).orElse((v, f, a) -> vector).adapt(vector, vector.getField(), allocator);
    }
}
