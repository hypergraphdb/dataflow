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

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>
 * An <code>InputPort</code> connects a {@link Channel} to a {@link Processor} for reading.
 * It is read to by a <code>Processor</code> implementation.
 * </p>
 * 
 * @author muriloq
 * 
 * @param <V> The type of data going through this port.
 */
public class InputPort<V> extends Port<V> implements Iterable<V>
{
	protected BlockingQueue<V> queue;
	
	protected InputPort()
	{	    
	}
	
	InputPort(Channel<V> channel)
	{
		super(channel);
		this.queue = new LinkedBlockingQueue<V>(channel.getCapacity());
	}

	public int getCurrentDataCount()
	{
		return queue.size();
	}

	public void clear()
	{
		queue.clear();
	}

	/**
	 * <p>Retrieve the next element waiting on this input port, blocking if there isn't any yet.
	 * Returns an EOS (End-Of-Stream) object when the port is closed - the end of stream object
	 * can be checked by calling <code>channel.isEOF(x)</code>.
	 * </p>
	 * @return
	 */
	public V take() throws InterruptedException
	{
		if (queue.isEmpty() && !isOpen())
			return channel.getEOS();
		try
		{
			V result = queue.take();
			if (isEOS(result))
			{
			    close();
			    queue.clear();
			}
			return result;
		} 
		catch (InterruptedException ignored)
		{
			close();
			queue.clear();
			return channel.getEOS();
		}
	}

	/**
	 * <p>
	 * Non-blocking return: EOS if this input port is closed or the next element waiting to be retrieved
	 * if any or <code>null</code> if there's no such next element.
	 * </p>
	 */
	public V poll()
	{
	    if (!isOpen())
	        return channel.getEOS();
	    else
	        return queue.poll();
	}
	
	public boolean put(V o) throws InterruptedException
	{
		synchronized (this)
		{
			if (!isOpen())
				return false;
		}
		try
		{
			this.queue.put(o);
		} 
		catch (InterruptedException ignored)
		{
			close();
			queue.clear();
			return false;
		}
		return true;
	}

	public boolean isEOS(Object o)
	{
		return channel.getEOS().equals(o);
	}

	public void close() throws InterruptedException
	{
		synchronized (this)
		{
			if (!isOpen())
				return;
			super.close();
			//
			// We can't put an EOF here because it should be possible for a
			// consumer
			// of this port to close it down in which case, nobody's going to
			// read
			// what's in the queue and if it exceeds the channel's capacity it
			// will
			// block indefinitely.
			// try {
			// this.queue.put(channel.getEOS());
			// } catch (InterruptedException ignored) {
			// }
			this.queue.clear();
		}
		channel.channelOutputClosed();
	}

	public synchronized void await() throws InterruptedException
	{
		while (!isOpen() && queue.isEmpty())
			wait();
	}
	
	public Iterator<V> iterator()
    {
        return new Iterator<V>()
        {
            V next = advance();
            
            private V advance()
            {
                try
                {
                    return take();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            
            public boolean hasNext()
            {
                return next != null && !isEOS(next);
            }

            public V next()
            {                
                V x = next;
                next = advance();
                return x;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }            
        };
    }

    public String toString()
	{
		return "InputPort from " + channel;
	}
}