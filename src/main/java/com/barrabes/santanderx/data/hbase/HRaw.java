package com.barrabes.santanderx.data.hbase;


import com.barrabes.santanderx.data.hbase.Entity.Column;

import java.nio.ByteBuffer;

public class HRaw<E extends Entity<E>> extends Column<E, ByteBuffer> {

    public HRaw(String family, String qualifier) {
        super(family, qualifier);
    }
}

