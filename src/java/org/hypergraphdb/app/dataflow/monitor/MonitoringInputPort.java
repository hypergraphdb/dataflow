/*******************************************************************************
 * Copyright (c) 2011 Kobrix Software, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser Public License v2.1
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *    Borislav Iordanov, Murilo Saraiva de Queiroz - initial API and implementation
 ******************************************************************************/
package org.hypergraphdb.app.dataflow.monitor;

import org.hypergraphdb.app.dataflow.Channel;
import org.hypergraphdb.app.dataflow.InputPort;

/**
 * <p>
 * Record average delay b/w successive reads.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <V>
 */
public class MonitoringInputPort<V> extends InputPort<V>
{
    private long lastTimestamp = 0;
    private AvgAccumulator accumulator;
    private InputPort<V> wrapped;
    
    public MonitoringInputPort(InputPort<V> wrapped, AvgAccumulator acc)
    {
        this.wrapped = wrapped;
        this.accumulator = acc;
    }

    public long getLastTimestamp() { return lastTimestamp; }
    
    @Override
    public synchronized void await() throws InterruptedException
    {
        wrapped.await();
    }

    @Override
    public void clear()
    {
        wrapped.clear();
    }

    @Override
    public void close() throws InterruptedException
    {
        wrapped.close();
    }

    @Override
    public int getCurrentDataCount()
    {
        return wrapped.getCurrentDataCount();
    }

    @Override
    public boolean isEOS(Object o)
    {
        return wrapped.isEOS(o);
    }

    @Override
    public V poll()
    {
        return wrapped.poll();
    }

    @Override
    public boolean put(V o) throws InterruptedException
    {
        return wrapped.put(o);
    }

    @Override
    public V take() throws InterruptedException
    {
        if (lastTimestamp > 0)
            accumulator.add(System.currentTimeMillis() - lastTimestamp);
        V x = wrapped.take();
        lastTimestamp = System.currentTimeMillis();
        return x;
    }

    @Override
    public String toString()
    {
        return wrapped.toString();
    }

    @Override
    public Channel<V> getChannel()
    {
        return wrapped.getChannel();
    }

    @Override
    public synchronized boolean isOpen()
    {
        return wrapped.isOpen();
    }

    @Override
    public synchronized void open()
    {
        wrapped.open();
    }

    @Override
    public void setChannel(Channel<V> channel)
    {
        wrapped.setChannel(channel);
    }
}
