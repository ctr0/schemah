package com.barrabes.santanderx.data.hbase;

import com.barrabes.santanderx.data.hbase.Entity.Column;
import org.apache.hadoop.hbase.util.Bytes;

public class HString<E extends Entity<E>> extends Column<E, String> {

    public HString(String family, String qualifier) {
        super(family, qualifier);
    }

    @Override
    public String valueOf(byte[] bytes) {
        return Bytes.toString(bytes);
    }
}
