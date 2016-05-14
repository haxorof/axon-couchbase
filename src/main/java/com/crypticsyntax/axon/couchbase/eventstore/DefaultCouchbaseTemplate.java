/*
 * The MIT License
 *
 * Copyright 2016 Bj&ouml;rn Oscarsson.
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
package com.crypticsyntax.axon.couchbase.eventstore;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.serializer.Serializer;
import org.axonframework.upcasting.UpcasterChain;

/**
 * This implementation stores one document per aggregate. The aggregate
 * document contains references to the related events.
 *
 * Be aware of that maximum size of a document in Couchbase is restricted to
 * 20Mb.
 *
 * @author Bj&ouml;rn Oscarsson
 */
public class DefaultCouchbaseTemplate implements CouchbaseTemplate {

    private static final String SEPARATOR = ":";
    private static final String PREFIX_AGG_ID = "ev:aggid:";
    private static final String PREFIX_EV_ID = "ev:id:";

    private final Bucket bucket;

    public DefaultCouchbaseTemplate(final Bucket bucket) {
        this.bucket = bucket;
    }

    private Bucket eventBucket() {
        return bucket;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertEvent(EventEntry eventEntry) {
        JsonDocument eventDoc = JsonDocument.create(PREFIX_EV_ID + eventEntry.getEventIdentifier(), eventEntry.asJsonObject());
        JsonDocument aggDoc = eventBucket().get(PREFIX_AGG_ID + eventEntry.getAggregateType() + SEPARATOR + eventEntry.getAggregateIdentifier());
        if (aggDoc == null) {
            JsonObject eventRefList = JsonObject.create();
            eventRefList.put("eventRefs", Arrays.asList(PREFIX_EV_ID + eventEntry.getEventIdentifier()));
            aggDoc = JsonDocument.create(PREFIX_AGG_ID + eventEntry.getAggregateType() + SEPARATOR + eventEntry.getAggregateIdentifier(), eventRefList);
        } else {
            JsonArray eventRefs = aggDoc.content().getArray("eventRefs");
            eventRefs.add(PREFIX_EV_ID + eventEntry.getEventIdentifier());
            aggDoc.content().put("eventRefs", eventRefs);
        }
        eventBucket().insert(eventDoc);
        eventBucket().upsert(aggDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(String type, Object identifier, Serializer eventSerializer, UpcasterChain upcasterChain) {
        Iterator<Object> eventRefs = bucket.get(PREFIX_AGG_ID + type + SEPARATOR + identifier)
                .content()
                .getArray("eventRefs")
                .iterator();
        return new QueryBackedDomainEventStream(bucket, eventRefs, null, identifier, Long.MAX_VALUE, true, eventSerializer, upcasterChain);
    }

    private static class QueryBackedDomainEventStream implements DomainEventStream {

        private final Bucket bucket;
        private DomainEventMessage next;
        private Iterator<DomainEventMessage> messagesToReturn = Collections.<DomainEventMessage>emptyList().iterator();
        private final boolean skipUnknownTypes;
        private final Object actualAggregateIdentifier;
        private final long lastSequenceNumber;
        private final Iterator<Object> cachedEventRefs;
        private final Serializer eventSerializer;
        private final UpcasterChain upcasterChain;

        private QueryBackedDomainEventStream(Bucket bucket,
                Iterator<Object> cachedEventRefs,
                List<DomainEventMessage> lastSnapshotCommit,
                Object actualAggregateIdentifier,
                long lastSequenceNumber,
                boolean skipUnknownTypes,
                Serializer eventSerializer,
                UpcasterChain upcasterChain) {
            if (lastSnapshotCommit != null) {
                messagesToReturn = lastSnapshotCommit.iterator();
            }
            this.bucket = bucket;
            this.actualAggregateIdentifier = actualAggregateIdentifier;
            this.lastSequenceNumber = lastSequenceNumber;
            this.skipUnknownTypes = skipUnknownTypes;
            this.eventSerializer = eventSerializer;
            this.upcasterChain = upcasterChain;
            this.cachedEventRefs = cachedEventRefs;
            initializeNextItem();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return next != null && next.getSequenceNumber() <= lastSequenceNumber;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DomainEventMessage next() {
            DomainEventMessage itemToReturn = next;
            initializeNextItem();
            return itemToReturn;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DomainEventMessage peek() {
            return next;
        }

        private void initializeNextItem() {

            while (!messagesToReturn.hasNext() && cachedEventRefs.hasNext()) {
                String eventRef = (String) cachedEventRefs.next();
                JsonDocument eventDocument = bucket.get(eventRef);
                final EventEntry eventEntry = new EventEntry(eventDocument.content());
                messagesToReturn = eventEntry.getDomainEvents(actualAggregateIdentifier, eventSerializer, upcasterChain, skipUnknownTypes).iterator();
            }
            next = messagesToReturn.hasNext() ? messagesToReturn.next() : null;
        }

    }
}
