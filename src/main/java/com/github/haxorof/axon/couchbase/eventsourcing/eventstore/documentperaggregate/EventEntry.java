/*
 * The MIT License
 *
 * Copyright 2017 Bj&ouml;rn Oscarsson.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.haxorof.axon.couchbase.eventsourcing.eventstore.documentperaggregate;

import com.couchbase.client.java.document.json.JsonObject;
import java.time.Instant;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import static org.axonframework.serialization.MessageSerializer.serializeMetaData;
import static org.axonframework.serialization.MessageSerializer.serializePayload;

/**
 * Data needed by different types of event logs.
 *
 * @author Bj&ouml;rn Oscarsson
 */
public class EventEntry implements DomainEventData<Object> {

    /**
     * Property name in Couchbase for the Aggregate Identifier.
     */
    private static final String AGGREGATE_IDENTIFIER_PROPERTY = "aggregateIdentifier";

    /**
     * Property name in Couchbase for the Sequence Number.
     */
    private static final String SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";

    /**
     * Property name in Couchbase for the Aggregate's Type Identifier.
     */
    private static final String AGGREGATE_TYPE_PROPERTY = "type";

    /**
     * Property name in Couchbase for the Time Stamp.
     */
    private static final String TIME_STAMP_PROPERTY = "timeStamp";

    private static final String SERIALIZED_PAYLOAD_PROPERTY = "serializedPayload";
    private static final String PAYLOAD_TYPE_PROPERTY = "payloadType";
    private static final String PAYLOAD_REVISION_PROPERTY = "payloadRevision";
    private static final String META_DATA_PROPERTY = "serializedMetaData";
    private static final String EVENT_IDENTIFIER_PROPERTY = "eventIdentifier";
    /**
     * Charset used for the serialization is usually UTF-8, which is presented
     * by this constant.
     */
    private final String aggregateIdentifier;
    private final long sequenceNumber;
    private final String timeStamp;
    private final String aggregateType;
    private final Object serializedPayload;
    private final String payloadType;
    private final String payloadRevision;
    private final Object serializedMetaData;
    private final String eventIdentifier;

    /**
     * Constructor used to create a new event entry to store in Mongo.
     *
     * @param event The actual DomainEvent to store
     * @param serializer Serializer to use for the event to store
     */
    EventEntry(DomainEventMessage<?> event, Serializer serializer) {
        this.aggregateType = event.getType();
        this.aggregateIdentifier = event.getAggregateIdentifier();
        this.sequenceNumber = event.getSequenceNumber();
        this.eventIdentifier = event.getIdentifier();
        Class<?> serializationTarget = String.class;
        if (serializer.canSerializeTo(JsonObject.class)) {
            serializationTarget = JsonObject.class;
        }
        SerializedObject<?> serializedPayloadObject = serializePayload(event, serializer, serializationTarget);
        SerializedObject<?> serializedMetaDataObject = serializeMetaData(event, serializer, serializationTarget);

        this.serializedPayload = serializedPayloadObject.getData();
        this.payloadType = serializedPayloadObject.getType().getName();
        this.payloadRevision = serializedPayloadObject.getType().getRevision();
        this.serializedMetaData = serializedMetaDataObject.getData();
        this.timeStamp = event.getTimestamp().toString();
    }

    /**
     * Creates a new EventEntry based on data provided by Couchbase.
     *
     * @param jsonObject Mongo object that contains data to represent an
     * EventEntry
     */
    EventEntry(JsonObject jsonObject) {
        this.aggregateIdentifier = (String) jsonObject.get(AGGREGATE_IDENTIFIER_PROPERTY);
        this.sequenceNumber = ((Number) jsonObject.get(SEQUENCE_NUMBER_PROPERTY)).longValue();
        this.serializedPayload = jsonObject.get(SERIALIZED_PAYLOAD_PROPERTY);
        this.timeStamp = (String) jsonObject.get(TIME_STAMP_PROPERTY);
        this.aggregateType = (String) jsonObject.get(AGGREGATE_TYPE_PROPERTY);
        this.payloadType = (String) jsonObject.get(PAYLOAD_TYPE_PROPERTY);
        this.payloadRevision = (String) jsonObject.get(PAYLOAD_REVISION_PROPERTY);
        this.serializedMetaData = jsonObject.get(META_DATA_PROPERTY);
        this.eventIdentifier = (String) jsonObject.get(EVENT_IDENTIFIER_PROPERTY);
    }

    private Class<?> getRepresentationType() {
        Class<?> representationType = String.class;
        if (serializedPayload instanceof JsonObject) {
            representationType = JsonObject.class;
        }
        return representationType;
    }

    /**
     * Returns the current EventEntry as a Couchbase JsonObject.
     *
     * @return JsonObject representing the EventEntry
     */
    public JsonObject asJsonObject() {
        return JsonObject.empty()
                .put(AGGREGATE_IDENTIFIER_PROPERTY, aggregateIdentifier)
                .put(SEQUENCE_NUMBER_PROPERTY, sequenceNumber)
                .put(SERIALIZED_PAYLOAD_PROPERTY, serializedPayload)
                .put(TIME_STAMP_PROPERTY, timeStamp)
                .put(AGGREGATE_TYPE_PROPERTY, aggregateType)
                .put(PAYLOAD_TYPE_PROPERTY, payloadType)
                .put(PAYLOAD_REVISION_PROPERTY, payloadRevision)
                .put(META_DATA_PROPERTY, serializedMetaData)
                .put(EVENT_IDENTIFIER_PROPERTY, eventIdentifier);
    }

    @Override
    public String getType() {
        return aggregateType;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public Instant getTimestamp() {
        return Instant.parse(timeStamp);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getMetaData() {
        return new SerializedMetaData(serializedMetaData, getRepresentationType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getPayload() {
        return new SimpleSerializedObject(serializedPayload, getRepresentationType(), payloadType, payloadRevision);
    }
}
