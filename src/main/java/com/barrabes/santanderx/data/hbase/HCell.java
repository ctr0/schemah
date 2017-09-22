package com.barrabes.santanderx.data.hbase;

import org.apache.hadoop.hbase.Cell;

public class HCell<T> {

    private Cell cell;

    HCell(Cell cell) {
        this.cell = cell;
    }

    public byte[] getRowArray() {
        return cell.getRowArray();
    }

    public int getRowOffset() {
        return cell.getRowOffset();
    }

    public short getRowLength() {
        return cell.getRowLength();
    }

    public byte[] getFamilyArray() {
        return cell.getFamilyArray();
    }

    public int getFamilyOffset() {
        return cell.getFamilyOffset();
    }

    public byte getFamilyLength() {
        return cell.getFamilyLength();
    }

    public byte[] getQualifierArray() {
        return cell.getQualifierArray();
    }

    public int getQualifierOffset() {
        return cell.getQualifierOffset();
    }

    public int getQualifierLength() {
        return cell.getQualifierLength();
    }

    public long getTimestamp() {
        return cell.getTimestamp();
    }

    public byte getTypeByte() {
        return cell.getTypeByte();
    }

    public long getSequenceId() {
        return cell.getSequenceId();
    }

    public byte[] getValueArray() {
        return cell.getValueArray();
    }

    public int getValueOffset() {
        return cell.getValueOffset();
    }

    public int getValueLength() {
        return cell.getValueLength();
    }

    public byte[] getTagsArray() {
        return cell.getTagsArray();
    }

    public int getTagsOffset() {
        return cell.getTagsOffset();
    }

    public int getTagsLength() {
        return cell.getTagsLength();
    }

    @Deprecated
    public byte[] getValue() {
        return cell.getValue();
    }

    @Deprecated
    public byte[] getFamily() {
        return cell.getFamily();
    }

    @Deprecated
    public byte[] getQualifier() {
        return cell.getQualifier();
    }

    @Deprecated
    public byte[] getRow() {
        return cell.getRow();
    }
}
