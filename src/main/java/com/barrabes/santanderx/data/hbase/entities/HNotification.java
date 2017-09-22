package com.barrabes.santanderx.data.hbase.entities;

import com.barrabes.santanderx.data.hbase.Builder;
import com.barrabes.santanderx.data.hbase.Entity;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * notification:
 *   table: Notifications
 *   key:
 *     - user: String
 *     - ts: long
 *     - type: String
 *     - resource: String
 *   data:
 *     - ts: boolean
 */
public class HNotification implements Entity {

    public static final String TABLE = "notifications";

    // Key segment
    private String user;

    // Key segment
    private long ts;

    // Key segment
    private String type;

    // Key segment
    private String resource;

    // Column family: data
    private boolean readed;

    private transient String key;

    public HNotification() {};

    public HNotification(String user, long ts, String type, String resource) {
        this.user = user;
        this.ts = ts;
        this.type = type;
        this.resource = resource;
    }

    public String key() {
        if (key == null) {
            StringBuilder builder = new StringBuilder();
            builder.append(user);
            builder.append(KEY_SEP);
            builder.append(ts);
            builder.append(KEY_SEP);
            builder.append(resource);
            key = builder.toString();
        }
        return key;
    }

    public String getUser() {
        return user;
    }

    public long getTs() {
        return ts;
    }

    public String getType() {
        return type;
    }

    public String getResource() {
        return resource;
    }

    public boolean isReaded() {
        return readed;
    }

    public HNotification setReaded(boolean readed) {
        this.readed = readed;
        return this;
    }

    public PutBuilder putBuilder() {
        return new PutBuilder();
    }

    public class PutBuilder implements Builder<Put> {

        Put put = new Put(Bytes.toBytes(key()));

        public PutBuilder readed() {
            put.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("readed"), Bytes.toBytes(readed));
            return this;
        }

        public Put build() {
            return put;
        }
    }

    public static HNotification parse(Result result) {
        String[] keyTokens = Bytes.toString(result.getRow()).split(KEY_SEP);
        HNotification notification = new HNotification(
                keyTokens[0],
                Long.parseLong(keyTokens[1]),
                keyTokens[2],
                keyTokens[3]);
        return notification;
    }
}
