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

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.eventstore.EventStore;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.xml.XStreamSerializer;
import org.axonframework.upcasting.SimpleUpcasterChain;
import org.axonframework.upcasting.UpcasterAware;
import org.axonframework.upcasting.UpcasterChain;

/**
 * This implements an event store in Couchbase.
 *
 * @author Bj&ouml;rn Oscarsson
 */
public class CouchbaseEventStore implements EventStore, UpcasterAware {

    private UpcasterChain upcasterChain = SimpleUpcasterChain.EMPTY;
    private final CouchbaseTemplate template;
    private final Serializer eventSerializer;

    public CouchbaseEventStore(CouchbaseTemplate template) {
        this.template = template;
        this.eventSerializer = new XStreamSerializer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUpcasterChain(UpcasterChain upcasterChain) {
        this.upcasterChain = upcasterChain;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendEvents(String type, DomainEventStream events) {
        if (!events.hasNext()) {
            return;
        }
        while (events.hasNext()) {
            DomainEventMessage message = events.next();
            template.insertEvent(new EventEntry(type, message, eventSerializer));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(String type, Object identifier) {
        return template.readEvents(type, identifier, eventSerializer, upcasterChain);
    }

}
