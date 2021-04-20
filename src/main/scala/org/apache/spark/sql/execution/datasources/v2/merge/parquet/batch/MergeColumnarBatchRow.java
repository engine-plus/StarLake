/*
 * Copyright [2021] [EnginePlus Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;

class MergeColumnarBatchRow extends InternalRow {

    private final ColumnVector[] columns;
    Tuple2<Integer, Integer>[] idMix;

    MergeColumnarBatchRow(ColumnVector[] columns) {
        this.columns = columns;
    }

    void updateColumns(ColumnarBatch mergeMolumns, int[] updateIndex) {
        for (int i = 0; i < updateIndex.length; i++) {
            columns[updateIndex[i]] = mergeMolumns.column(i);
        }
    }

    private Tuple2<Integer, Integer> getIndex(int ordinal) {
        return idMix[ordinal];
    }

    @Override

    public int numFields() {
        return idMix.length;
    }


    @Override
    public InternalRow copy() {
        GenericInternalRow row = new GenericInternalRow(idMix.length);
        for (int i = 0; i < numFields(); i++) {
            if (isNullAt(i)) {
                row.setNullAt(i);
            } else {
                Tuple2<Integer, Integer> colIdAndRowId = getIndex(i);
                DataType dt = columns[colIdAndRowId._1()].dataType();
                if (dt instanceof BooleanType) {
                    row.setBoolean(i, getBoolean(i));
                } else if (dt instanceof ByteType) {
                    row.setByte(i, getByte(i));
                } else if (dt instanceof ShortType) {
                    row.setShort(i, getShort(i));
                } else if (dt instanceof IntegerType) {
                    row.setInt(i, getInt(i));
                } else if (dt instanceof LongType) {
                    row.setLong(i, getLong(i));
                } else if (dt instanceof FloatType) {
                    row.setFloat(i, getFloat(i));
                } else if (dt instanceof DoubleType) {
                    row.setDouble(i, getDouble(i));
                } else if (dt instanceof StringType) {
                    row.update(i, getUTF8String(i).copy());
                } else if (dt instanceof BinaryType) {
                    row.update(i, getBinary(i));
                } else if (dt instanceof DecimalType) {
                    DecimalType t = (DecimalType) dt;
                    row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
                } else if (dt instanceof DateType) {
                    row.setInt(i, getInt(i));
                } else if (dt instanceof TimestampType) {
                    row.setLong(i, getLong(i));
                } else {
                    throw new RuntimeException("Not implemented. " + dt);
                }
            }
        }
        return row;
    }

    @Override
    public boolean anyNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return colIdAndRowId._1 == -1 ? true : columns[colIdAndRowId._1()].isNullAt(colIdAndRowId._2());
    }

    @Override
    public boolean getBoolean(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getBoolean(colIdAndRowId._2());
    }

    @Override
    public byte getByte(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getByte(colIdAndRowId._2());
    }

    @Override
    public short getShort(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getShort(colIdAndRowId._2());
    }

    @Override
    public int getInt(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getInt(colIdAndRowId._2());
    }

    @Override
    public long getLong(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getLong(colIdAndRowId._2());
    }

    @Override
    public float getFloat(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getFloat(colIdAndRowId._2());
    }

    @Override
    public double getDouble(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getDouble(colIdAndRowId._2());
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getDecimal(colIdAndRowId._2(), precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getUTF8String(colIdAndRowId._2());
    }

    @Override
    public byte[] getBinary(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getBinary(colIdAndRowId._2());
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getInterval(colIdAndRowId._2());
    }

    @Override
    public ColumnarRow getStruct(int ordinal, int numFields) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getStruct(colIdAndRowId._2());
    }

    @Override
    public ColumnarArray getArray(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getArray(colIdAndRowId._2());
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        Tuple2<Integer, Integer> colIdAndRowId = getIndex(ordinal);
        return columns[colIdAndRowId._1()].getMap(colIdAndRowId._2());
    }


    @Override
    public Object get(int ordinal, DataType dataType) {
        if (dataType instanceof BooleanType) {
            return getBoolean(ordinal);
        } else if (dataType instanceof ByteType) {
            return getByte(ordinal);
        } else if (dataType instanceof ShortType) {
            return getShort(ordinal);
        } else if (dataType instanceof IntegerType) {
            return getInt(ordinal);
        } else if (dataType instanceof LongType) {
            return getLong(ordinal);
        } else if (dataType instanceof FloatType) {
            return getFloat(ordinal);
        } else if (dataType instanceof DoubleType) {
            return getDouble(ordinal);
        } else if (dataType instanceof StringType) {
            return getUTF8String(ordinal);
        } else if (dataType instanceof BinaryType) {
            return getBinary(ordinal);
        } else if (dataType instanceof DecimalType) {
            DecimalType t = (DecimalType) dataType;
            return getDecimal(ordinal, t.precision(), t.scale());
        } else if (dataType instanceof DateType) {
            return getInt(ordinal);
        } else if (dataType instanceof TimestampType) {
            return getLong(ordinal);
        } else if (dataType instanceof ArrayType) {
            return getArray(ordinal);
        } else if (dataType instanceof StructType) {
            return getStruct(ordinal, ((StructType) dataType).fields().length);
        } else if (dataType instanceof MapType) {
            return getMap(ordinal);
        } else {
            throw new UnsupportedOperationException("Datatype not supported " + dataType);
        }
    }

    @Override
    public void update(int ordinal, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNullAt(int ordinal) {
        throw new UnsupportedOperationException();
    }

}
