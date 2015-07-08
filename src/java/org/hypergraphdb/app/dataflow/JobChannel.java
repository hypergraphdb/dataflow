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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A channel dedicated to a specific Job. A data flow network could be executing
 * several jobs at the same time. Each job gets its separate set of channels so
 * that there's no interference between the data of different jobs flowing through
 * the same channel. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <DataType>
 * @param <JobType>
 */
public class JobChannel<DataType> extends Channel<DataType>
{ 
    private Integer expectedOutputCount;
    private Job job;
    private BlockingQueue<DataType> buffer;
    private List<ChannelListener> listeners = new ArrayList<ChannelListener>();
    private Condition expectedOutputReached = lock.newCondition();
    
    public JobChannel(int expectedOutputCount, String id, Job job, DataType EOF)
    {
        super(id, EOF);
        this.job = job;
        this.expectedOutputCount = expectedOutputCount;
        buffer = new LinkedBlockingQueue<DataType>(getCapacity());
    }
    
    public JobChannel(int expectedOutputCount, String id, Job job, DataType EOF, int capacity)
    {
        super(id, EOF, capacity);
        this.job = job;
        this.expectedOutputCount = expectedOutputCount;
        buffer = new LinkedBlockingQueue<DataType>(getCapacity());        
    }

    public InputPort<DataType> newInputPort()
    {
        lock.lock();
        try
        {
            assert 
                expectedOutputCount <= channelOutputs.size() :
                new RuntimeException("Attempt to add more than expected output counts to channel");
            
            InputPort<DataType> in = super.newInputPort();
            // The new port needs to be opened right so as to start buffering
            // input that's going to be read most likely shortly after the processor
            // starts. We need to do this because the channel will attempt to write
            // to all its input ports and if the port is closed while the channel
            // is writing then data will be missed.
            in.open();       
            for (DataType x : buffer)
                try 
                {
                    in.put(x);
                }
                catch (InterruptedException ex)
                {
                    throw new RuntimeException(
                      "Interrupted while flushing new port buffer on Job channel " + 
                      getId());
                }
            if (expectedOutputCount == channelOutputs.size())
            {
                buffer.clear();
                expectedOutputReached.signalAll();
            }
            return in;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    public void put(DataType x) throws InterruptedException
    {
        for (ChannelListener L : listeners)
            if (!L.beforePut(this, x))
                return;
        
        lock.lock();
        try        
        {
            waitBlocked();
            // If not all input ports have been connected yet, we need to
            // buffer the data that arrives. 
            if (expectedOutputCount > channelOutputs.size())
            {
                try 
                { 
                    if (EOS.equals(x) || !buffer.offer(x))
                        expectedOutputReached.await();
                }
                catch (InterruptedException ex) 
                { closePorts(); return; }
            }
            for (InputPort<DataType> port : channelOutputs)
            {
                port.put(x);
            }
        }
        finally
        {
            lock.unlock();
        }        
        for (ChannelListener L : listeners)
            L.afterPut(this, x);
    }

    public Job getJob()
    {
        return job;
    }

    public void addListener(ChannelListener L)
    {
        listeners.add(L);
    }
    
    public void removeListener(ChannelListener L)
    {
        listeners.remove(L);
    }
    
    public int hashCode()
    {
        return HGUtils.hashThem(getJob(), getId());
    }

    @SuppressWarnings("unchecked")
    public boolean equals(Object x)
    {
        if (! (x instanceof JobChannel))
            return false;
        JobChannel<DataType> y = (JobChannel<DataType>)x;
        return HGUtils.eq(getJob(), y.getJob()) &&
               HGUtils.eq(getId(), y.getId());
    }
    
    public String toString()
    {
        return getId().toString();
    }
}
