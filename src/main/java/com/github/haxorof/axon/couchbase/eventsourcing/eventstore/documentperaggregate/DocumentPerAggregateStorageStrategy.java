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

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.github.haxorof.axon.couchbase.eventsourcing.eventstore.CouchbaseStoreStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.EventUtils;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;

/**
 *
 * @author Bj&ouml;rn Oscarsson
 */
@Slf4j
public class DocumentPerAggregateStorageStrategy implements CouchbaseStoreStrategy {

    private final String EVENT_PREFIX = "cbes:ev:";

    @Override
    public void appendEvents(Bucket bucket, List<? extends EventMessage<?>> events, Serializer serializer) {
        List<JsonObject> jsonObjects = createEventDocuments(events, serializer).collect(Collectors.toList());
        jsonObjects.forEach((e) -> {
            String docId = EVENT_PREFIX + e.getString("aggregateIdentifier");
            if (!bucket.exists(docId)) {
                JsonArray eventArray = JsonArray.empty()
                        .add(e);
                JsonObject data = JsonObject.empty()
                        .put("events", eventArray);
                JsonDocument doc = JsonDocument.create(docId, data);
                bucket.insert(doc);
            } else {
                bucket.mutateIn(docId)
                        .arrayAppend("events", e)
                        .execute();
            }

        });
    }

    protected Stream<JsonObject> createEventDocuments(List<? extends EventMessage<?>> events, Serializer serializer) {
        return events.stream()
                .map(EventUtils::asDomainEventMessage)
                .map(event -> new EventEntry(event, serializer))
                .map(entry -> entry.asJsonObject());
    }

    @Override
    public void appendSnapshot(Bucket bucket, DomainEventMessage<?> snapshot, Serializer serializer) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Optional<? extends DomainEventData<?>> findLastSnapshot(Bucket bucket, String aggregateIdentifier) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<? extends DomainEventData<?>> findDomainEvents(Bucket bucket, String aggregateIdentifier, long firstSequenceNumber, int batchSize) {
        String docId = EVENT_PREFIX + aggregateIdentifier;
        JsonDocument doc = bucket.get(docId);
        List<EventEntry> eventEntries = new ArrayList<>();
        if (doc != null) {
            DocumentFragment<Lookup> fragment = bucket.lookupIn(docId)
                    .get("events")
                    .execute();
            JsonArray events = fragment.content("events", JsonArray.class);
            events.forEach((event) -> {
                eventEntries.add(new EventEntry((JsonObject) event));
            });
        }
        return eventEntries;
    }

    @Override
    public List<? extends TrackedEventData<?>> findTrackedEvents(Bucket bucket, TrackingToken lastToken, int batchSize) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
