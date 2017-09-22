package com.barrabes.santanderx.data.hbase;

import com.barrabes.santanderx.data.hbase.Entity.Column;
import com.barrabes.santanderx.data.hbase.Entity.RowKey;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;


public class HResult<E extends Entity<E>> {

    private Result result;

    public HResult(Result result) {
        this.result = result;
    }

    public RowKey<E> getRowKey() {
        return Entity.rowKey(result.getRow());
    }

    public Cell[] rawCells() {
        return result.rawCells();
    }

    public List<Cell> listCells() {
        return result.listCells();
    }

    public <T> Stream<HCell<T>> getColumnCells(Column<E, T> column) {
        return result.getColumnCells(column.family(), column.qualifier())
                .stream().map(cell -> new HCell<T>(cell));
    }

    public <T> HCell<T> getColumnLatestCell(Column<E, T> column) {
        return new HCell<>(result.getColumnLatestCell(column.family(), column.qualifier()));
    }

    public <T> T getValue(Column<E, T> column) {
        return column.valueOf(result.getValue(column.family(), column.qualifier()));
    }

    public ByteBuffer getValue(HRaw<E> column) {
        return result.getValueAsByteBuffer(column.family(), column.qualifier());
    }

    public boolean loadValue(HRaw<E> column, ByteBuffer dst) throws BufferOverflowException {
        return result.loadValue(column.family(), column.qualifier(), dst);
    }

    public <T> boolean containsNonEmptyColumn(Column<E, T> column) {
        return result.containsNonEmptyColumn(column.family(), column.qualifier());
    }

    public <T> boolean containsEmptyColumn(Column<E, T> column) {
        return result.containsEmptyColumn(column.family(), column.qualifier());
    }

    public <T> boolean containsColumn(Column<E, T> column) {
        return result.containsColumn(column.family(), column.qualifier());
    }

//    public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
//        return result.getMap();
//    }

//    public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
//        return result.getNoVersionMap();
//    }

//    public NavigableMap<byte[], byte[]> getFamilyMap(byte[] family) {
//        return result.getFamilyMap(family);
//    }

    public byte[] value() {
        return result.value();
    }

    public boolean isEmpty() {
        return result.isEmpty();
    }

    public int size() {
        return result.size();
    }

    @Override
    public String toString() {
        return result.toString();
    }

    public static void compareResults(Result res1, Result res2) throws Exception {
        Result.compareResults(res1, res2);
    }

    public static Result createCompleteResult(List<Result> partialResults) throws IOException {
        return Result.createCompleteResult(partialResults);
    }

    public static long getTotalSizeOfCells(Result result) {
        return Result.getTotalSizeOfCells(result);
    }

    public void copyFrom(Result other) {
        result.copyFrom(other);
    }

    public CellScanner cellScanner() {
        return result.cellScanner();
    }

    public Cell current() {
        return result.current();
    }

    public boolean advance() {
        return result.advance();
    }

    public Boolean getExists() {
        return result.getExists();
    }

    public void setExists(Boolean exists) {
        result.setExists(exists);
    }

    public boolean isStale() {
        return result.isStale();
    }

    public boolean isPartial() {
        return result.isPartial();
    }

    public ClientProtos.RegionLoadStats getStats() {
        return result.getStats();
    }
}
