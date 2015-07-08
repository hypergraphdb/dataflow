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
import org.hypergraphdb.app.dataflow.OutputPort;

/**
 * <p>
 * Record average delay b/w successive writes. 
 * </p> 
 * @author Borislav Iordanov
 *
 * @param <V>
 */
public class MonitoringOutputPort<V> extends OutputPort<V>
{
    private long lastTimestamp;
    private OutputPort<V> wrapped;
    private AvgAccumulator accumulator;
    
    public MonitoringOutputPort(OutputPort<V> wrapped, AvgAccumulator acc)
    {
        this.wrapped = wrapped;
        this.accumulator = acc;
    }

    @Override
    public void close() throws InterruptedException
    {
        wrapped.close();
    }

    @Override
    public boolean put(V v) throws InterruptedException
    {
        if (lastTimestamp > 0)
            accumulator.add(System.currentTimeMillis() - lastTimestamp);
        boolean result = wrapped.put(v);
        lastTimestamp = System.currentTimeMillis();
        return result;
    }

    @Override
    public String toString()
    {
        return wrapped.toString();
    }

    @Override
    public synchronized void await() throws InterruptedException
    {
        wrapped.await();
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
