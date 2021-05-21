///*
// * Copyright [2021] [EnginePlus Team]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch;
//
//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
//import org.apache.spark.sql.types.*;
//import org.apache.spark.sql.vectorized.*;
//import org.apache.spark.unsafe.types.CalendarInterval;
//import org.apache.spark.unsafe.types.UTF8String;
//import scala.Tuple2;
//
//class MergeColumnarBatchRow extends MergeBatchRow {
//
//    private final ColumnVector[] columns;
//    Tuple2<Integer, Integer>[] idMix;
//
//    MergeColumnarBatchRow(ColumnVector[] columns) {
//        this.columns = columns;
//    }
//
//    void updateColumns(ColumnarBatch mergeMolumns, int[] updateIndex) {
//        for (int i = 0; i < updateIndex.length; i++) {
//            columns[updateIndex[i]] = mergeMolumns.column(i);
//        }
//    }
//
//    private Tuple2<Integer, Integer> getIndex(int ordinal) {
//        return idMix[ordinal];
//    }
//
//    @Override
//
//    public int numFields() {
//        return idMix.length;
//    }
//
//
//    @Override
//    public InternalRow copy() {
//        GenericInternalRow row = new GenericInternalRow(idMix.length);
//        for (int i = 0; i < numFields(); i++) {
//            if (isNullAt(i)) {
//                row.setNullAt(i);
//            } else {
//                Tuple2<Integer, Integer> colIdAndRowId = getIndex(i);
//                DataType dt = columns[colIdAndRowId._1()].dataType();
//                setRowData(i, dt, row);
//            }
//        }
//        return row;
//    }
//
//    @Override
//    public boolean anyNull() {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public boolean isNullAt(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return colIdAndRowId._1 == -1 || columns[colIdAndRowId._1()].isNullAt(colIdAndRowId._2());
//    }
//
//    @Override
//    public boolean getBoolean(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getBoolean(colIdAndRowId._2());
//    }
//
//    @Override
//    public byte getByte(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getByte(colIdAndRowId._2());
//    }
//
//    @Override
//    public short getShort(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getShort(colIdAndRowId._2());
//    }
//
//    @Override
//    public int getInt(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getInt(colIdAndRowId._2());
//    }
//
//    @Override
//    public long getLong(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getLong(colIdAndRowId._2());
//    }
//
//    @Override
//    public float getFloat(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getFloat(colIdAndRowId._2());
//    }
//
//    @Override
//    public double getDouble(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getDouble(colIdAndRowId._2());
//    }
//
//    @Override
//    public Decimal getDecimal(int ordinal, int precision, int scale) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getDecimal(colIdAndRowId._2(), precision, scale);
//    }
//
//    @Override
//    public UTF8String getUTF8String(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getUTF8String(colIdAndRowId._2());
//    }
//
//    @Override
//    public byte[] getBinary(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getBinary(colIdAndRowId._2());
//    }
//
//    @Override
//    public CalendarInterval getInterval(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getInterval(colIdAndRowId._2());
//    }
//
//    @Override
//    public ColumnarRow getStruct(int ordinal, int numFields) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getStruct(colIdAndRowId._2());
//    }
//
//    @Override
//    public ColumnarArray getArray(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getArray(colIdAndRowId._2());
//    }
//
//    @Override
//    public ColumnarMap getMap(int ordinal) {
//        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
//        return columns[colIdAndRowId._1()].getMap(colIdAndRowId._2());
//    }
//
//    @Override
//    public void update(int ordinal, Object value) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void setNullAt(int ordinal) {
//        throw new UnsupportedOperationException();
//    }
//
//}
