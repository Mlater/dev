package com.later.flink_demo;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;

/**
 * @description:
 * @author: Liu Jun Jun
 * @create: 2020-06-12 18:34
 **/
public class acc implements Accumulator {
    @Override
    public void add(Object value) {

    }

    @Override
    public Serializable getLocalValue() {
        return null;
    }

    @Override
    public void resetLocal() {

    }

    @Override
    public void merge(Accumulator other) {

    }

    @Override
    public Accumulator clone() {
        return null;
    }
}
