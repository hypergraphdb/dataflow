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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

/**
 * 
 * <p>
 * Encapsulates all ports (both input and output) that a given {@link Processor} is 
 * connected to. An instance of this class is passed as a parameter to the
 * {@link Processor.process} method. Each port is identified by the name of the 
 * {@link Channel} to which it connects. This the {@link getInputMap} method returns
 * a map of "upstream" channel names to local ports connected to them, while the 
 * {@link getOutputMap} method returns the "downstream" channel to local ports mapping. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class Ports
{
    /** The input port from each channel */
    private Map<String, InputPort<?>> inputs;

    /** The output port to each channel */
    private Map<String, OutputPort<?>> outputs;

    public Ports()
    {
        this.inputs = new HashMap<String, InputPort<?>>();
        this.outputs = new HashMap<String, OutputPort<?>>();
    }

    public Map<String, InputPort<?>> getInputMap()
    {
    	return inputs;
    }
    
    public Map<String, OutputPort<?>> getOutputMap()
    {
    	return outputs;
    }
    
    public void addPort(InputPort<?> port)
    {
        this.inputs.put(port.getChannel().getId(), port);
    }

    public void addPort(OutputPort<?> port)
    {
        this.outputs.put(port.getChannel().getId(), port);
    }

    @SuppressWarnings("unchecked")
    public <T> InputPort<T> getInput(String channelId)
    {
        return (InputPort<T>) inputs.get(channelId);
    }

    @SuppressWarnings("unchecked")
    public <T> InputPort<T> getSingleInput()
    {
        return (InputPort<T>) inputs.values().iterator().next();
    }

    @SuppressWarnings("unchecked")
    public <T> OutputPort<T> getOutput(String channelId)
    {
        return (OutputPort<T>) outputs.get(channelId);
    }

    @SuppressWarnings("unchecked")
    public <T> OutputPort<T> getSingleOutput()
    {
        return (OutputPort<T>) outputs.values().iterator().next();
    }

    public int getInputCount()
    {
        return inputs.size();
    }

    public int getOutputCount()
    {
        return outputs.size();
    }

    public Set<String> getInputChannelIDs()
    {
        return inputs.keySet();
    }

    public Set<String> getOutputChannelIDs()
    {
        return outputs.keySet();
    }

    public Collection<InputPort<?>> getInputPorts()
    {
        return inputs.values();
    }

    public Collection<OutputPort<?>> getOutputPorts()
    {
        return outputs.values();
    }
    
    public void openAll()
    {
        openInputs();
        openOutputs();
    }
    
    public void closeAll() throws InterruptedException
    {
        closeInputs();
        closeOutputs();
    }
    
    public void openInputs()
    {
        for (Port<?> p : inputs.values()) 
        {
//            DU.log.info("Open input port " + p.getChannel().getId());
            p.open();
        }
    }

    public void openOutputs()
    {
        for (Port<?> p : outputs.values())
        {
//            DU.log.info("Open output port " + p.getChannel().getId());
            p.open();
        }
    }

    public void closeInputs() throws InterruptedException
    {
        for (Port<?> p : inputs.values()) p.close();
    }

    public void closeOutputs() throws InterruptedException
    {
        for (Port<?> p : outputs.values()) p.close();
    }    
}