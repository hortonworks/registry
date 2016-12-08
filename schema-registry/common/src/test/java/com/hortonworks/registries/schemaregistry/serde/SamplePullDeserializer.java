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
import com.hortonworks.registries.schemaregistry.serde.pull.FieldValueContext;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullEventContext;
import com.hortonworks.registries.schemaregistry.serde.pull.StartRecordContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Sample {@link PullDeserializer} implementation
 */
public class SamplePullDeserializer implements PullDeserializer<SchemaDetails, Schema.Field> {

    private final SchemaDetails schemaDetails;
    private final BufferedReader payloadReader;

    private State nextState = State.START_RECORD;

    private String currentLine;
    private String[] currentFields;
    private int currentFieldIndex;

    public SamplePullDeserializer(SchemaDetails schemaDetails, InputStream payloadInputStream) {
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
            try {
                currentLine = payloadReader.readLine();
            } catch (IOException e) {
                throw new SerDesException(e);
            }
            nextState = currentLine != null ? State.PROCESS_FIELDS : State.END_DESERIALIZE;

            context = new StartRecordContext<>();
        } else {
            if (currentFields == null) {
                currentFields = currentLine.split(",");
                currentFieldIndex = 0;
            }

            // when there are no field/values in current event return null
            final MyFieldValue fieldValue = currentFields.length > 0 ? new MyFieldValue(currentFields[currentFieldIndex++]) : null;
            context = new FieldValueContext<>(fieldValue);

            if (currentFieldIndex == currentFields.length) {
                nextState = State.START_RECORD;
                currentFields = null;
            }
        }

        return context;
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

    @Override
    public void init(Map<String, ?> config) {

    }

    private class MyFieldValue implements PullEventContext.FieldValue<Schema.Field> {

        private final String fieldPayload;
        private final int separatorIndex;

        public MyFieldValue(String fieldPayload) {
            this.fieldPayload = fieldPayload;
            separatorIndex = fieldPayload.indexOf(":");
            if (separatorIndex == -1) {
                throw new RuntimeException("Invalid field payload format");
            }
        }

        @Override
        public Schema.Field field() {
            return Schema.Field.of(fieldPayload.substring(0, separatorIndex), Schema.Type.STRING);
        }

        @Override
        public Object value() {
            return fieldPayload.substring(separatorIndex + 1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyFieldValue)) return false;

            MyFieldValue that = (MyFieldValue) o;

            if (separatorIndex != that.separatorIndex) return false;
            return fieldPayload.equals(that.fieldPayload);

        }

        @Override
        public int hashCode() {
            int result = fieldPayload.hashCode();
            result = 31 * result + separatorIndex;
            return result;
        }
    }
}
