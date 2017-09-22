package com.barrabes.santanderx.data.hbase;

import com.barrabes.santanderx.data.hbase.Entity.Column;
import org.apache.hadoop.hbase.util.Bytes;

public class HLong<E extends Entity<E>> extends Column<E, Long> {

    public HLong(String family, String qualifier) {
        super(family, qualifier);
    }

    @Override
    public Long valueOf(byte[] bytes) {
        return Bytes.toLong(bytes);
    }
}
