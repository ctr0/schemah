package com.barrabes.santanderx.data.hbase;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;

public abstract class Entity<E extends Entity<E>> {

    public static final String KEY_SEP = "|";

    protected static <E extends Entity<E>> RowKey<E> rowKey(Object... segments) {
        return new RowKey<>(segments);
    }

    protected static <E extends Entity<E>> RowKey<E> rowKey(byte[] bytes) {
        return new RowKey<>(bytes);
    }

    protected static <E extends Entity<E>> Key<E> partialKey(Object... segments) {
        return new Key<>(segments);
    }

    public RowKey<E> key() {
        return new RowKey<>(keyValues());
    }

    protected abstract Object[] keyValues();

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + key().toString() + ']';
    }

    public static class Key<E extends Entity<E>> {

        public final String value;

        Key(byte[] bytes) {
            this.value = Bytes.toString(bytes);
        }

        Key(Object... segments) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < segments.length; i++) {
                builder.append(segments[i].toString());
                if (i < segments.length - 1) {
                    builder.append(KEY_SEP);
                }
            }
            this.value = builder.toString();
        }

        public byte[] value() {
            return value.getBytes();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key<?> key = (Key<?>) o;

            return value != null ? value.equals(key.value) : key.value == null;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static class RowKey<E extends Entity<E>> extends Key<E> {

        RowKey(byte[] bytes) {
            super(bytes);
        }

        RowKey(Object... segments) {
            super(segments);
        }
    }

    public abstract class Column<E extends Entity, T> {

        private final Field field;

        public final String family;

        public final String qualifier;

        public Column(Class<E> c, String family, String qualifier) throws NoSuchFieldException {
            this.field = c.getField(family + StringUtils.capitalize(family));
            this.family = family;
            this.qualifier = qualifier;
        }

        public byte[] family() {
            return family.getBytes();
        }

        public byte[] qualifier() {
            return family.getBytes();
        }

        public abstract T valueOf(byte[] bytes);

        public T valueFrom(E entity) {
            field.get(entity)
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + '[' + family + ':' + qualifier + ']';
        }
    }
/*
    public class Builder<> {

        private Result result;

        protected Builder(Result result) {
            this.result = result;
        }

        protected String getString(String family, String column) {
            return Bytes.toString(getCell(family, column).getValueArray());
        }

        protected boolean getBoolean(String family, String column) {
            return Bytes.toBoolean(getCell(family, column).getValueArray());
        }

        protected int getInt(String family, String column) {
            return Bytes.toInt(getCell(family, column).getValueArray());
        }

        protected long getLong(String family, String column) {
            return Bytes.toLong(getCell(family, column).getValueArray());
        }

        protected double getDouble(String family, String column) {
            return Bytes.toDouble(getCell(family, column).getValueArray());
        }

        protected Cell getCell(String family, String column) {
            return result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(column));
        }

        @SuppressWarnings("unchecked")
        public E build() {
            return (E) Entity.this;
        }

    }
*/

}
