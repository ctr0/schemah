package com.barrabes.santanderx.data.hbase.entities;


import com.barrabes.santanderx.data.hbase.Builder;
import com.barrabes.santanderx.data.hbase.Entity;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * action:
 *   table: Actions
 *   key:
 *     - user: String
 *     - type: String
 *     - resource: String
 *   data:
 *     ts: boolean
 */
public class HAction implements Entity {

    public static final String TABLE = "actions";

    // Key segmet
    private String user;

    // Key segmet
    private String type;

    // Key segmet
    private String resource;

    // Column family: data
    private long ts;

    private transient String key;


    public HAction() {}

    public HAction(String user, String type, String resource) {
        this.user = user;
        this.type = type;
        this.resource = resource;
    }

    public String key() {
        if (key == null) {
            StringBuilder builder = new StringBuilder();
            builder.append(user);
            builder.append(KEY_SEP);
            builder.append(type);
            builder.append(KEY_SEP);
            builder.append(resource);
            key = builder.toString();
        }
        return key;
    }

    public String getUser() {
        return user;
    }

    public String getType() {
        return type;
    }

    public String getResource() {
        return resource;
    }

    public long getTs() {
        return ts;
    }

    public HAction setTs(long ts) {
        this.ts = ts;
        return this;
    }

    public PutBuilder putBuilder() {
        return new PutBuilder();
    }

    public class PutBuilder implements Builder<Put> {

        Put put = new Put(Bytes.toBytes(key()));

        public PutBuilder ts() {
            put.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("ts"), Bytes.toBytes(ts));
            return this;
        }

        public Put build() {
            return put;
        }
    }

    public static HAction parse(Result result) {
        String[] keyTokens = Bytes.toString(result.getRow()).split(KEY_SEP);
        HAction entity = new HAction(
                keyTokens[0],
                keyTokens[1],
                keyTokens[2]);
        entity.setTs(Bytes.toLong(result.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("ts")).getValueArray()));
        return entity;
    }
}