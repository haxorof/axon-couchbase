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
package com.github.haxorof.axon.couchbase.eventsourcing.eventstore;

import com.github.haxorof.axon.couchbase.eventsourcing.eventstore.documentperaggregate.DocumentPerAggregateStorageStrategy;
import java.util.List;
import java.util.Optional;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;

/**
 *
 * @author Bj&ouml;rn Oscarsson
 */
public class CouchbaseEventStoreEngine extends BatchingEventStorageEngine {

    private static final int DEFAULT_BATCH = 100;
    private final CouchbaseStoreStrategy storageStrategy;
    private final CouchbaseTemplate template;

    public CouchbaseEventStoreEngine(Serializer serializer,
            EventUpcaster upcasterChain,
            PersistenceExceptionResolver persistenceExceptionResolver,
            Integer batchSize,
            CouchbaseStoreStrategy storageStrategy,
            CouchbaseTemplate template) {
        super(serializer, upcasterChain, persistenceExceptionResolver, batchSize);
        this.storageStrategy = storageStrategy;
        this.template = template;
    }
    
    public CouchbaseEventStoreEngine(Serializer serializer,
            EventUpcaster upcasterChain,
            Integer batchSize,
            CouchbaseStoreStrategy storageStrategy,
            CouchbaseTemplate template) {
        super(serializer, upcasterChain, CouchbaseEventStoreEngine::isDuplicateKeyException, batchSize);
        this.storageStrategy = storageStrategy;
        this.template = template;
    }
    
    public CouchbaseEventStoreEngine(Serializer serializer,
            EventUpcaster upcasterChain,
            CouchbaseStoreStrategy storageStrategy,
            CouchbaseTemplate template) {
        super(serializer, upcasterChain, CouchbaseEventStoreEngine::isDuplicateKeyException, DEFAULT_BATCH);
        this.storageStrategy = storageStrategy;
        this.template = template;
    }
    
    public CouchbaseEventStoreEngine(CouchbaseTemplate template) {
        super(null, null, CouchbaseEventStoreEngine::isDuplicateKeyException, DEFAULT_BATCH);
        this.storageStrategy = new DocumentPerAggregateStorageStrategy();
        this.template = template;
    }
    
    private static boolean isDuplicateKeyException(Exception exception) {
        // FIXME
        return false;
    }    

    @Override
    protected List<? extends TrackedEventData<?>> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        return storageStrategy.findTrackedEvents(template.getEventBucket(), lastToken, batchSize);
    }

    @Override
    protected List<? extends DomainEventData<?>> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber, int batchSize) {
        return storageStrategy.findDomainEvents(template.getEventBucket(), aggregateIdentifier, firstSequenceNumber, batchSize);
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        if (!events.isEmpty()) {
            try {
                storageStrategy.appendEvents(template.getEventBucket(), events, serializer);
            } catch (Exception e) {
                handlePersistenceException(e, events.get(0));
            }
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        try {
            storageStrategy.appendSnapshot(template.getSnapshotBucket(), snapshot, serializer);
        } catch (Exception e) {
            handlePersistenceException(e, snapshot);
        }
    }

    @Override
    protected Optional<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        return storageStrategy.findLastSnapshot(template.getSnapshotBucket(), aggregateIdentifier);
    }

}
