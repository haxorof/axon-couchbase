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

import org.axonframework.domain.DomainEventStream;
import org.axonframework.serializer.Serializer;
import org.axonframework.upcasting.UpcasterChain;

/**
 * @author Bj&ouml;rn Oscarsson
 */
interface CouchbaseTemplate {

    /**
     * Inserts an event into the event store.
     * 
     * @param eventEntry the event entry to be inserted.
     */
    public void insertEvent(EventEntry eventEntry);

    /**
     * Returns a stream to read events for a specific type and aggregate.
     * 
     * @param type the aggregate type.
     * @param identifier the aggregate identifier.
     * @param eventSerializer the event serializer.
     * @param upcasterChain the upcaster chain.
     * @return a stream to read events for a specific type and aggregate.
     */
    public DomainEventStream readEvents(String type, 
            Object identifier, 
            Serializer eventSerializer, 
            UpcasterChain upcasterChain);

}
