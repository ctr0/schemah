package com.barrabes.santanderx.data.hbase.entities;

import com.barrabes.santanderx.data.hbase.Entity;
import com.barrabes.santanderx.data.hbase.HLong;

/**
 * action:
 *   table: santanderx:actions
 *   key:
 *     - user: String
 *     - type: String
 *     - resource: String
 *   data:
 *     dataTs: boolean
 */
public class HAction extends Entity<HAction> {

    public static final String TABLE = "santanderx:actions";

    public static HLong<HAction> DATA_TS = new HLong<>(getClass(), "data", "ts");


    // Key segment
    private String user;

    // Key segment
    private String type;

    // Key segment
    private String resource;

    // Column 'data:ts'
    private long dataTs;

    /**
     * Default constructor
     */
    public HAction() {}

    /**
     * Constructor from row rey
     */
    public HAction(String user, String type, String resource) {
        this.user = user;
        this.type = type;
        this.resource = resource;
    }

    /**
     * Constructor from row rey
     *
     * @param rowKey the row key
     */
    public HAction(RowKey<HAction> rowKey) {
        String[] keyTokens = rowKey.value.split(KEY_SEP);
        this.user = keyTokens[0];
        this.type = keyTokens[1];
        this.resource = keyTokens[2];
    }

    /**
     * @return the key segment 'user'
     */
    public String getUser() {
        return user;
    }

    /**
     * @return the key segment 'type'
     */
    public String getType() {
        return type;
    }

    /**
     * @return the key segment 'resource'
     */
    public String getResource() {
        return resource;
    }

    /**
     * @return the last cell valueOf for column 'data:dataTs'
     */
    public long getDataTs() {
        return dataTs;
    }

    /**
     * Sets the valueOf for column 'data:dataTs'
     *
     * @param dataTs
     * @return this entity
     */
    public HAction setDataTs(long dataTs) {
        this.dataTs = dataTs;
        return this;
    }

    /**
     * @return The partial key '${user}KEY_SEP'
     */
    public Key<HAction> partialKeyUser() {
        return partialKeyUser(user);
    }

    /**
     * @return The partial key '${user}KEY_SEP'
     */
    public static Key<HAction> partialKeyUser(String user) {
        return partialKey(user);
    }

    /**
     * @return The partial key '${user}KEY_SEP${type}KEY_SEP'
     */
    public Key<HAction> partialKeyUserType() {
        return partialKey(user, type);
    }

    /**
     * @return The partial key '${user}KEY_SEP${type}KEY_SEP'
     */
    public static Key<HAction> partialKeyUserType(String user, String type) {
        return partialKey(user, type);
    }

    /**
     * @return The partial key '${user}KEY_SEP${type}KEY_SEP'
     */
    public static RowKey<HAction> rowKey(String user, String type, String resource) {
        return Entity.rowKey(user, type, resource);
    }

    protected final Object[] keyValues() {
        return new Object[] { user, type, resource };
    }
}