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

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>
 * A Channel is an entity that has one or more input streams and one or more
 * output streams. A channel's job is to transmit data from the input streams 
 * to the output streams in an orderly fashion. It merges data from multiple 
 * input streams in an undefined way (as they arrive), but it preserves the 
 * order from a single input stream. It sends the data merged from all input 
 * streams to all output streams in the
 * same order. (i.e. all output ports get the exact same stream of data).
 * </p>
 * 
 * <p>
 * Each channel is identified by a unique within the network name that is
 * assigned by the programmer. In addition, each channel has a designed 
 * <em>EOS</em> (End-Of-Stream) marker that is of the same type as the 
 * data type going through this channel.
 * </p>
 * 
 * @author muriloq, borislav
 * 
 * @param <ID> The type of the channel identifier. 
 * @param <E> The type of data traveling through this channel.
 */
public class Channel<E>
{
	protected String id;
	protected int capacity;
    protected transient Lock lock = new ReentrantLock();
    protected transient Condition uncloggedCondition = lock.newCondition();
	protected transient Set<OutputPort<E>> channelInputs; // OutputPorts are the "input"
												// of channels
	protected transient Set<InputPort<E>> channelOutputs; // InputPorts are the "output"
												// of channels
	protected E EOS;
	protected int capsCount = 0;
	
	public Channel()
	{
        channelInputs = new HashSet<OutputPort<E>>();
        channelOutputs = new HashSet<InputPort<E>>();	    
	}
	
	public Channel(String id, E EOS)
	{
		// same valued used in LinkedBlockingQueue default constructor, see
		// InputPort(Channel)
		capacity = Integer.MAX_VALUE;
		init(id, EOS);
	}

	public Channel(String id, E EOS, int capacity)
	{
		// capacity is used LinkedBlockingQueue constructor, see
		// InputPort(Channel)
		this.capacity = capacity;
		init(id, EOS);
	}

	private void init(String id, E EOS)
	{
		this.id = id;
		this.EOS = EOS;
		channelInputs = new HashSet<OutputPort<E>>();
		channelOutputs = new HashSet<InputPort<E>>();
	}

	public InputPort<E> newInputPort()
	{
	    lock.lock();
	    try
	    {
	        InputPort<E> in = new InputPort<E>(this);
	        channelOutputs.add(in);	       
	        return in;
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}

	public OutputPort<E> newOutputPort()
	{
	    lock.lock();
	    try
	    {
	        OutputPort<E> out = new OutputPort<E>(this);
	        channelInputs.add(out);
	        return out;
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}

	public void closePorts() throws InterruptedException
	{
	    lock.lock();
	    try
	    {
            for (Port<E> port : channelInputs)
                port.close();
            for (Port<E> port : channelOutputs)
                port.close();
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public String toString()
	{
		return "Channel " + getId().toString();
	}

	/**
	 * Assuming the channel lock is acquired, this method
	 * implements the waiting loop for blocked channels (i.e.
	 * when the number of "caps" is > 0). 
	 */
	protected void waitBlocked() throws InterruptedException
	{
        while (isBlocked())
            uncloggedCondition.await();	    
	}
	
	public void put(E e) throws InterruptedException
	{
	    lock.lock();
	    try
	    {
	        waitBlocked();
    		for (InputPort<E> port : channelOutputs)
    		{
    			port.put(e);
    		}
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}

	/**
	 * <p>
	 * Add a blocking cap to this channel. A channel may be blocked
	 * by adding caps to it. If it is blocked, a <code>put</code> operation
	 * will block until all caps previously added through this method have
	 * been removed through the <code>removeCap</code> method.
	 * </p>  
	 */
	public void addCap()
	{
	    lock.lock();
	    try
	    {
	        capsCount++;
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}
	
	public void removeCap()
	{
	    lock.lock();
	    try
	    {
	        if (capsCount > 0)
	        {
	            if (--capsCount == 0)
	                uncloggedCondition.signalAll();
	        }
	        else
    	        throw new RuntimeException("More channel caps removed than added.");
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}
	
	public boolean isBlocked()
	{
	    return capsCount > 0;
	}
	
	/**
	 * When all input ports connected to a channel get closed and its data
	 * buffer is empty, the channel must close all its output ports.
	 */
	public void channelInputClosed() throws InterruptedException
	{
	    lock.lock();
	    try
	    {
    		int closed = 0;
    		for (OutputPort<E> port : channelInputs)
    		{
    			if (!port.isOpen())
    				closed++;
    		}
    		if (closed == channelInputs.size())
    		{
    //		    System.out.println("Closing channel " + this);
                // We close by signaling EOF because we want to give
                // a chance to consumers to read all remaining data.
                // port.close();		    
    		    put(getEOS());
    		}
	    }
	    finally
	    {
	        lock.unlock();	        
	    }
	}

	/**
	 * when all output ports in a channel are closed, the channel close all its
	 * input ports
	 */
	public void channelOutputClosed() throws InterruptedException
	{
	    lock.lock();
	    try
	    {
    		int closed = 0;
    		for (InputPort<E> port : channelOutputs)
    		{
    			if (!port.isOpen())
    				closed++;
    		}
    		if (closed == channelOutputs.size())
    		{
    			for (OutputPort<E> port : channelInputs)
    			{
    				if (port.isOpen())
    					port.close();
    			}
    		}
	    }
	    finally
	    {
	        lock.unlock();
	    }
	}

//	public Set<OutputPort<E>> getChannelInputs()
//	{
//		return channelInputs;
//	}
//
//	public void setChannelInputs(Set<OutputPort<E>> channelInputs)
//	{
//		this.channelInputs = channelInputs;
//	}
//
//	public Set<InputPort<E>> getChannelOutputs()
//	{
//		return channelOutputs;
//	}
//
//	public void setChannelOutputs(Set<InputPort<E>> channelOutputs)
//	{
//		this.channelOutputs = channelOutputs;
//	}

	public int getCapacity()
	{
		return capacity;
	}

	public void setCapacity(int capacity)
	{
		this.capacity = capacity;
	}
	
	public E getEOS()
	{
		return EOS;
	}
	
	public void setEOS(E EOS)
	{
		this.EOS = EOS;
	}
	
	public int hashCode()
	{
	    return id == null ? 0 : id.hashCode();	                      
	}
	
	public boolean equals(Object x)
	{
	    if (! (x instanceof Channel<?>))
	        return false;
	    else
	    {
	        Object xid = ((Channel<?>)x).getId();
	        return xid == null ? id == null : xid.equals(id);
	    }
	}
}
