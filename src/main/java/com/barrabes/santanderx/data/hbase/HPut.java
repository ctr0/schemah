package com.barrabes.santanderx.data.hbase;

import com.barrabes.santanderx.data.hbase.Entity.Column;
import com.barrabes.santanderx.data.hbase.Entity.Key;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

public class HPut<E extends Entity> {

    private Put put;

    public static <E extends Entity> HPut<E> from(Key<E> key) {
        return new HPut<>(key.value(), HConstants.LATEST_TIMESTAMP);
    }

    public static <E extends Entity> HPut<E> from(Key<E> key, long ts) {
        return new HPut<>(key.value(), HConstants.LATEST_TIMESTAMP);
    }

    private HPut(E entity) {
        put = new Put(entity.key().value());
    }

    public <T> Put addColumn(Column<E, T> column) {
        return put.addColumn(column.family(), column.qualifier(), column.from());
    }

    public Put addImmutable(Column<E> column, byte[] value) {
        return put.addImmutable(column.family(), column.qualifier(), value);
    }

    public Put addImmutable(Column<E> column, byte[] value, Tag[] tag) {
        return put.addImmutable(column.family(), column.qualifier(), value, tag);
    }

    public Put addColumn(Column<E> column, long ts, byte[] value) {
        return put.addColumn(column.family(), column.qualifier(), ts, value);
    }

    public Put addImmutable(Column<E> column, long ts, byte[] value) {
        return put.addImmutable(column.family(), column.qualifier(), ts, value);
    }

    public Put addImmutable(Column<E> column, long ts, byte[] value, Tag[] tag) {
        return put.addImmutable(column.family(), column.qualifier(), ts, value, tag);
    }

//    public Put addImmutable(Column<E> column, long ts, ByteBuffer valueOf, Tag[] tag) {
//        return put.addImmutable(column.family(), column.qualifier(), ts, valueOf, tag);
//    }

//    public Put addColumn(Column<E> column, long ts, ByteBuffer valueOf) {
//        return put.addColumn(column.family(), column.qualifier(), ts, valueOf);
//    }

//    public Put addImmutable(Column<E> column, long ts, ByteBuffer valueOf) {
//        return put.addImmutable(column.family(), column.qualifier(), ts, valueOf);
//    }

    public Put add(Cell kv) throws IOException {
        return put.add(kv);
    }

    public boolean has(Column<E> column) {
        return put.has(column.family(), column.qualifier());
    }

    public boolean has(Column<E> column, long ts) {
        return put.has(column.family(), column.qualifier(), ts);
    }

    public boolean has(Column<E> column, byte[] value) {
        return put.has(column.family(), column.qualifier(), value);
    }

    public boolean has(Column<E> column, long ts, byte[] value) {
        return put.has(column.family(), column.qualifier(), ts, value);
    }

    public List<Cell> get(Column<E> column) {
        return put.get(column.family(), column.qualifier());
    }

    public Put setAttribute(String name, byte[] value) {
        return put.setAttribute(name, value);
    }

    public Put setId(String id) {
        return put.setId(id);
    }

    public Put setDurability(Durability d) {
        return put.setDurability(d);
    }

    public Put setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
        return put.setFamilyCellMap(map);
    }

    public Put setClusterIds(List<UUID> clusterIds) {
        return put.setClusterIds(clusterIds);
    }

    public Put setCellVisibility(CellVisibility expression) {
        return put.setCellVisibility(expression);
    }

    public Put setACL(String user, Permission perms) {
        return put.setACL(user, perms);
    }

    public Put setACL(Map<String, Permission> perms) {
        return put.setACL(perms);
    }

    public Put setTTL(long ttl) {
        return put.setTTL(ttl);
    }

    public CellScanner cellScanner() {
        return put.cellScanner();
    }

    public Map<String, Object> getFingerprint() {
        return put.getFingerprint();
    }

    public Map<String, Object> toMap(int maxCols) {
        return put.toMap(maxCols);
    }

    public Durability getDurability() {
        return put.getDurability();
    }

    public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
        return put.getFamilyCellMap();
    }

    public boolean isEmpty() {
        return put.isEmpty();
    }

    public byte[] getRow() {
        return put.getRow();
    }

    public int compareTo(Row d) {
        return put.compareTo(d);
    }

    public long getTimeStamp() {
        return put.getTimeStamp();
    }

    public List<UUID> getClusterIds() {
        return put.getClusterIds();
    }

    public CellVisibility getCellVisibility() throws DeserializationException {
        return put.getCellVisibility();
    }

    public int size() {
        return put.size();
    }

    public int numFamilies() {
        return put.numFamilies();
    }

    public long heapSize() {
        return put.heapSize();
    }

    public byte[] getACL() {
        return put.getACL();
    }

    public long getTTL() {
        return put.getTTL();
    }

    public byte[] getAttribute(String name) {
        return put.getAttribute(name);
    }

    public Map<String, byte[]> getAttributesMap() {
        return put.getAttributesMap();
    }

    public String getId() {
        return put.getId();
    }

    public Map<String, Object> toMap() {
        return put.toMap();
    }

    public String toJSON(int maxCols) throws IOException {
        return put.toJSON(maxCols);
    }

    public String toJSON() throws IOException {
        return put.toJSON();
    }

    public String toString(int maxCols) {
        return put.toString(maxCols);
    }

    @Override
    public String toString() {
        return put.toString();
    }
}
