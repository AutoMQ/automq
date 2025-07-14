/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.automq.table.worker;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;

import java.util.Locale;
import java.util.Map;

public class RecordWrapper implements Record {

    private final Record delegate;
    private final Operation op;

    public RecordWrapper(Record delegate, String cdcField, boolean upsert) {
        this.delegate = delegate;
        String cdcValue = extract(delegate, cdcField);
        if (cdcValue == null || cdcValue.isEmpty()) {
            op = upsert ? Operation.UPDATE : Operation.INSERT;
        } else {
            switch (cdcValue.toUpperCase(Locale.ROOT).charAt(0)) {
                case 'I':
                    op = Operation.INSERT;
                    break;
                case 'U':
                    op = Operation.UPDATE;
                    break;
                case 'D':
                    op = Operation.DELETE;
                    break;
                default:
                    op = Operation.INSERT;
            }
        }
    }

    public RecordWrapper(Record delegate, Operation op) {
        this.delegate = delegate;
        this.op = op;
    }

    public Operation op() {
        return op;
    }

    @Override
    public StructType struct() {
        return delegate.struct();
    }

    @Override
    public Object getField(String name) {
        return delegate.getField(name);
    }

    @Override
    public void setField(String name, Object value) {
        delegate.setField(name, value);
    }

    @Override
    public Object get(int pos) {
        return delegate.get(pos);
    }

    @Override
    public Record copy() {
        return new RecordWrapper(delegate.copy(), op);
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        return new RecordWrapper(delegate.copy(overwriteValues), op);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return delegate.get(pos, javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
        delegate.set(pos, value);
    }

    public static String extract(Record record, String cdcField) {
        if (cdcField == null) {
            return null;
        }
        String[] fieldChain = cdcField.split("\\.");
        return extract0(record, fieldChain, 0);
    }

    public static String extract0(Object parent, String[] fieldChain, int idx) {
        Object value = null;
        if (parent instanceof Record) {
            value = ((Record) parent).getField(fieldChain[idx]);
        } else if (parent instanceof Map) {
            value = ((Map<?, ?>) parent).get(fieldChain[idx]);
        }
        if (value == null) {
            return null;
        }
        if (idx == fieldChain.length - 1) {
            if (value instanceof String) {
                return (String) value;
            } else {
                return null;
            }
        } else {
            return extract0(value, fieldChain, idx + 1);
        }

    }
}
