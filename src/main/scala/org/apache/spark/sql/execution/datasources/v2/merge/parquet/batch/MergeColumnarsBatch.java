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
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Tuple2;


/**
 * Created by luozhenglin
 * on 2020/11/6 3:06 PM
 */
public final class MergeColumnarsBatch implements AutoCloseable {

    private final ColumnVector[] columns;

    // Staging row returned from `getRow`.
    private final MergeColumnarBatchRow row;

    public MergeColumnarsBatch(ColumnVector[] columns) {
        this.columns = columns;
        this.row = new MergeColumnarBatchRow(columns);
    }

    public void updateBatch(ColumnarBatch mergeMolumns, int[] updateIndex) {
        for (int i = 0; i < updateIndex.length; i++) {
            columns[updateIndex[i]] = mergeMolumns.column(i);
        }

        row.updateColumns(mergeMolumns, updateIndex);
    }

    public InternalRow getRow(Tuple2<Integer, Integer>[] resultScheme) {
        row.idMix = resultScheme;
        return row;
    }

    @Override
    public void close() {
        for (ColumnVector c : columns) {
            c.close();
        }
    }
}