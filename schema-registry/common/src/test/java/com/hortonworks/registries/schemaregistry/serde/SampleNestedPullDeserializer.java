/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.schemaregistry.serde.pull.EndFieldContext;
import com.hortonworks.registries.schemaregistry.serde.pull.EndRecordContext;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullEventContext;
import com.hortonworks.registries.schemaregistry.serde.pull.StartFieldContext;
import com.hortonworks.registries.schemaregistry.serde.pull.StartRecordContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Sample {@link PullDeserializer} implementation for nested payloads
 */
public class SampleNestedPullDeserializer implements PullDeserializer<SchemaDetails, Schema.Field> {

    private final SchemaDetails schemaDetails;
    private final BufferedReader payloadReader;

    private State nextState = State.START_RECORD;

    private String currentLine;
    private List<PullEventContext.FieldValue> values = new ArrayList<>();

    private final Stack<FieldWithIndex> schemaFields = new Stack<>();
    private int rootIndex;

    public SampleNestedPullDeserializer(SchemaDetails schemaDetails, InputStream payloadInputStream) {
        this.schemaDetails = schemaDetails;
        this.payloadReader = new BufferedReader(new InputStreamReader(payloadInputStream));
    }

    @Override
    public boolean hasNext() throws SerDesException {
        return (nextState != State.END_DESERIALIZE);
    }

    @Override
    public PullEventContext<Schema.Field> next() throws SerDesException {

        PullEventContext<Schema.Field> context;
        if (isStartEvent()) {
            context = handleStartEvent();
        } else if (isEndEvent()) {
            context = new EndRecordContext<>();
            currentLine = null;
            handleEndEvent();
        } else {
            context = processFields();
        }

        return context;
    }

    private boolean isEndEvent() {
        return nextState == State.END_RECORD;
    }

    private PullEventContext<Schema.Field> processFields() throws SerDesException {
        PullEventContext<Schema.Field> context;
        int startFieldIndex = currentLine.indexOf(':');
        int endFieldIndex = currentLine.indexOf(',');

        if (endFieldIndex == -1) {
            endFieldIndex = Integer.MAX_VALUE;
        }
        if (startFieldIndex == -1) {
            startFieldIndex = Integer.MAX_VALUE;
        }

        if (startFieldIndex == endFieldIndex) {
            throw new SerDesException("Invalid payload format!!");
        }

        // get current field
        if (startFieldIndex < endFieldIndex) {
            //start field
            String fieldName = currentLine.substring(0, startFieldIndex);
            //todo go through schema also and validate the current field with the respective field in schema.
            Schema.Field currentField;
            if (schemaFields.empty()) {
                currentField = nextRootField();
                schemaFields.push(new FieldWithIndex(currentField));
            } else {
                FieldWithIndex peek = schemaFields.peek();
                Schema.Field peekField = peek.field;
                if (!(peekField instanceof Schema.NestedField)) {
                    throw new SerDesException("Expected nested field");
                }
                peek.index++;

                currentField = ((Schema.NestedField) peekField).getFields().get(peek.index);
                schemaFields.push(new FieldWithIndex(currentField));
            }

            if (!fieldName.equals(currentField.getName())) {
                throw new SerDesException("Expected name [" + fieldName + "], encountered unexpected name [" + currentField.getName() + "]");
            }

            context = new StartFieldContext<>(currentField);
            updateCurrentLine(startFieldIndex);
        } else {
            //end field
            String value = currentLine.substring(0, endFieldIndex);

            Schema.Field schemaField = schemaFields.pop().field;

            MyFieldValue currentFieldValue;
            if (value.isEmpty()) {
                // this means all the values stored will be part of this parent field.
                currentFieldValue = new MyFieldValue(schemaField, values);
                values = new ArrayList<>();
            } else {
                currentFieldValue = new MyFieldValue(schemaField, value);
            }
            values.add(currentFieldValue);
            context = new EndFieldContext<>(currentFieldValue);
            updateCurrentLine(endFieldIndex);
        }

        if (currentLine.isEmpty()) {
            nextState = State.END_RECORD;
        }

        return context;
    }

    @Override
    public void init(Map<String, ?> config) {

    }

    private static class FieldWithIndex {
        private final Schema.Field field;
        private int index;

        public FieldWithIndex(Schema.Field field) {
            this.field = field;
            index = -1;
        }

        public FieldWithIndex(Schema.Field field, int index) {
            this.field = field;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldWithIndex that = (FieldWithIndex) o;

            if (index != that.index) return false;
            return field != null ? field.equals(that.field) : that.field == null;

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + index;
            return result;
        }

        @Override
        public String toString() {
            return "FieldWithIndex{" +
                    "field=" + field +
                    ", index=" + index +
                    '}';
        }
    }

    private Schema.Field nextRootField() {
        return schemaDetails.schema.getFields().get(rootIndex++);
    }

    private void handleEndEvent() throws SerDesException {
        try {
            currentLine = payloadReader.readLine();
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        nextState = currentLine != null ? State.START_RECORD : State.END_DESERIALIZE;
    }


    private PullEventContext<Schema.Field> handleStartEvent() throws SerDesException {
        rootIndex = 0;
        PullEventContext<Schema.Field> context;
        try {
            if (currentLine == null) {
                currentLine = payloadReader.readLine();
            }
        } catch (IOException e) {
            throw new SerDesException(e);
        }
        if (currentLine != null) {
            nextState = State.PROCESS_FIELDS;
            context = new StartRecordContext<>();
        } else {
            nextState = State.END_DESERIALIZE;
            context = new EndRecordContext<>();
        }
        return context;
    }

    private void updateCurrentLine(int index) {
        if (index < currentLine.length()) {
            currentLine = currentLine.substring(index + 1);
        }
    }

    private boolean isStartEvent() {
        return nextState == State.START_RECORD;
    }

    @Override
    public SchemaDetails schema() {
        return schemaDetails;
    }

    @Override
    public void close() throws Exception {
        payloadReader.close();
    }

    private class MyFieldValue implements PullEventContext.FieldValue<Schema.Field> {
        private final Schema.Field field;
        private final Object value;

        public MyFieldValue(Schema.Field field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public Schema.Field field() {
            return field;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyFieldValue that = (MyFieldValue) o;

            if (field != null ? !field.equals(that.field) : that.field != null) return false;
            return value != null ? value.equals(that.value) : that.value == null;

        }

        @Override
        public int hashCode() {
            int result = field != null ? field.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MyFieldValue{" +
                    "field=" + field +
                    ", value=" + value +
                    '}';
        }
    }
}
