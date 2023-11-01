/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.trino;

import org.openjdk.jol.info.ClassData;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldData;
import org.openjdk.jol.util.MathUtil;
import org.openjdk.jol.vm.VM;
import org.openjdk.jol.vm.VirtualMachine;

import static java.lang.Math.toIntExact;

public final class ClassInstanceSize
{
    private ClassInstanceSize() {}

//    Implementation based on io.airlift.slice.instanceSize without requiring implementation
    static int sizeOf(Class<?> clazz) {
//        return instanceSize(clazz);
        try {
            return toIntExact(ClassLayout.parseClass(clazz).instanceSize());
        }
        catch (RuntimeException e) {
            VirtualMachine vm = VM.current();
            ClassData classData = ClassData.parseClass(clazz);
            long instanceSize = vm.objectHeaderSize();
            for (FieldData field : classData.fields()) {
                instanceSize += vm.sizeOfField(field.typeClass());
            }
            instanceSize = MathUtil.align(instanceSize, vm.objectAlignment());
            return toIntExact(instanceSize);
        }
    }
}
