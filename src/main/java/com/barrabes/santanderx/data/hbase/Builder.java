package com.barrabes.santanderx.data.hbase;

import org.apache.hadoop.hbase.client.Mutation;

public interface Builder<M extends Mutation> {

    M build();

}
