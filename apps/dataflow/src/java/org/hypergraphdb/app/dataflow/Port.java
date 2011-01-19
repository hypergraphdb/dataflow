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
package org.hypergraphdb.app.dataflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * A port establishes the link between a channel and a processor. {@link InputPort}s
 * are for reading by {@link Processor}s from specific {@link Channel}s 
 * while {@link OutputPort}s are for writing.    
 * </p>
 * 
 * @author muriloq
 * 
 * @param <V>
 */
public class Port<V>
{
    private static Log log = LogFactory.getLog(Port.class);
    
    private boolean isOpen;
    protected Channel<V> channel;

    protected Port()
    {        
    }
    
    Port(Channel<V> channel)
    {
        this.channel = channel;
    }

    public Channel<V> getChannel()
    {
        return channel;
    }

    public void setChannel(Channel<V> channel)
    {
        this.channel = channel;
    }

    public synchronized void close() throws InterruptedException
    {
        if (!isOpen)
            return;
        isOpen = false;
        log.debug(this + " closed");
    }

    public synchronized void open()
    {
        isOpen = true;
        log.debug(this + " opened");
        notifyAll();
    }

    public synchronized void await() throws InterruptedException
    {
        while (!isOpen)
            wait();
    }

    public synchronized boolean isOpen()
    {
        return isOpen;
    }
}