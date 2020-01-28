package com.facebook.presto.operator;

import com.facebook.presto.jni.PrestoJNILoader;

import java.nio.ByteBuffer;

public class PrestoNativeOperator
{

    public PrestoNativeOperator(){
        PrestoJNILoader.loadJNI("presto-jni.so");
    }

    protected native ByteBuffer apply(ByteBuffer buf);
}
