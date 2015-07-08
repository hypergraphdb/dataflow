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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A <code>ProcessingNode</code> is an entity encapsulating an executable
 * processing component (the {@link Processor}).
 * </p>
 * 
 * <p>
 * Each <code>ProcessingNode</code> is connected to one or more channels for
 * input through input ports and to one or more channels for output through
 * output ports. A ProcessingNode never accesses a Channel directly.
 * </p>
 * 
 * <p>
 * This is a framework implementation class and there's no need for applications to use
 * it.
 * </p>
 * 
 * @author muriloq
 * 
 */
public class ProcessingNode<ContextType> implements Runnable
{
    private static Log log = LogFactory.getLog(ProcessingNode.class);

    private static long thread_count = 0;

    protected DataFlowNetwork<ContextType> network;
    protected Processor<ContextType> processor;
    protected Ports ports;

    String getThreadName()
    {
        String name = processor.getName();
        name += "[";
        for (Iterator<InputPort<?>> iter = ports.getInputPorts().iterator(); iter
                                                                                 .hasNext();)
        {
            name += iter.next().getChannel().getId();
            if (iter.hasNext())
                name += ",";
        }
        name += " --> ";
        for (Iterator<OutputPort<?>> iter = ports.getOutputPorts().iterator(); iter
                                                                                   .hasNext();)
        {
            name += iter.next().getChannel().getId();
            if (iter.hasNext())
                name += ",";
        }
        name += "]";
        ;
        return name + "-" + (++thread_count);
    }

    /*
     * void prelude() { network.mainLock.lock(); network.runningNodes++;
     * network.mainLock.unlock(); log.debug("Starting ProcessingNode
     * "+processor.getClass().getCanonicalName()+" prelude, running nodes
     * "+network.runningNodes); }
     */

    void postlude()
    {
        network.mainLock.lock();
        if (network.runningNodes == 0)
            throw new Error("Wrong count of number of running ProcessingNode.");
        if (--network.runningNodes == 0)
        {
            network.termination.signalAll();
            network.completed();
        }
        network.mainLock.unlock();
        log.debug("Finished ProcessingNode "
                  + processor.getClass().getCanonicalName()
                  + " postlude, running nodes " + network.runningNodes);
    }

    public ProcessingNode(DataFlowNetwork<ContextType> network,
                          Processor<ContextType> processor, Ports ports)
    {
        super();
        this.network = network;
        this.processor = processor;
        this.ports = ports;
    }

    public void run()
    {
        try
        {
            // prelude();
            Thread.currentThread().setName(getThreadName());
            // Wait for all input ports to open.
            for (InputPort<?> inputPort : ports.getInputPorts())
            {
                inputPort.open();
            }
            // Open all output ports.
            for (OutputPort<?> outputPort : ports.getOutputPorts())
            {
                outputPort.open();
            }

            network.mainLock.lock();
            try
            {
                // The last node to get started gives the initiation signal.
                if (++network.runningNodes == network.nodes.size())
                    network.initiation.signalAll();
                else
                    network.initiation.await();
            }
            finally
            {
                network.mainLock.unlock();
            }
            log.debug("Starting ProcessingNode "
                      + Thread.currentThread().getName());
            processor.process(network.getContext(), ports);
        }
        catch (InterruptedException ex)
        {
            System.err.println("WARNING: Data Flow node '"
                               + Thread.currentThread().getName()
                               + "' interrupted, exiting prematurely...");
            return;
        }
        catch (Throwable t)
        {
            if (HGUtils.getRootCause(t) instanceof InterruptedException)
            {
                System.err.println("WARNING: Data Flow node '"
                                   + Thread.currentThread().getName()
                                   + "' interrupted, exiting prematurely...");
                return;                
            }
            log.error("Error in ProcessingNode " + processor, t);
            throw new RuntimeException(t);
        }
        finally
        {
            // Close all input and output ports
            for (InputPort<?> inputPort : ports.getInputPorts())
            {
                try
                {
                    inputPort.close();
                }
                catch (InterruptedException ex)
                {
                    System.err.println("Thread interrupt while closing input port ignored.");
                }
            }
            for (OutputPort<?> outputPort : ports.getOutputPorts())
            {
                try
                {
                    outputPort.close();
                }
                catch (InterruptedException ex)
                {
                    System.err.println("Thread interrupt while closing input port ignored.");
                }                    
            }
            postlude();
            Thread.currentThread().setName("DataFlow Thread");
        }
    }

    public Ports getPorts()
    {
        return ports;
    }

    public void setPorts(Ports ports)
    {
        this.ports = ports;
    }

    public DataFlowNetwork<ContextType> getNetwork()
    {
        return network;
    }

    public void setNetwork(DataFlowNetwork<ContextType> network)
    {
        this.network = network;
    }

    public Processor<ContextType> getProcessor()
    {
        return processor;
    }

    public void setProcessor(Processor<ContextType> processor)
    {
        this.processor = processor;
    }

    public String toString()
    {
        return "ProcessingNode::" + processor;
    }
}