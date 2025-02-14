/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

/** Short {@link ColumnReader}. Using INT32 to store short, so just cast int to short. */
public class ShortColumnReader extends AbstractColumnReader<WritableShortVector> {

    public ShortColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
            throws IOException {
        super(descriptor, pageReader);
        checkTypeName(PrimitiveType.PrimitiveTypeName.INT32);
    }

    @Override
    protected void readBatch(int rowId, int num, WritableShortVector column) {
        int left = num;
        while (left > 0) {
            if (runLenDecoder.currentCount == 0) {
                runLenDecoder.readNextGroup();
            }
            int n = Math.min(left, runLenDecoder.currentCount);
            switch (runLenDecoder.mode) {
                case RLE:
                    if (runLenDecoder.currentValue == maxDefLevel) {
                        for (int i = 0; i < n; i++) {
                            column.setShort(rowId + i, (short) readDataBuffer(4).getInt());
                        }
                    } else {
                        column.setNulls(rowId, n);
                    }
                    break;
                case PACKED:
                    for (int i = 0; i < n; ++i) {
                        if (runLenDecoder.currentBuffer[runLenDecoder.currentBufferIdx++]
                                == maxDefLevel) {
                            column.setShort(rowId + i, (short) readDataBuffer(4).getInt());
                        } else {
                            column.setNullAt(rowId + i);
                        }
                    }
                    break;
            }
            rowId += n;
            left -= n;
            runLenDecoder.currentCount -= n;
        }
    }

    @Override
    protected void readBatchFromDictionaryIds(
            int rowId, int num, WritableShortVector column, WritableIntVector dictionaryIds) {
        for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
                column.setShort(i, (short) dictionary.decodeToInt(dictionaryIds.getInt(i)));
            }
        }
    }
}
