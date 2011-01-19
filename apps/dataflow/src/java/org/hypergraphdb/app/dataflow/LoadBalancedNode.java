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

/**
 * <p>
 * Wraps a copy of a balanced processor so as to make all ports available 
 * under the original channel IDs. Reminder: load balanced copies get 
 * each a copy of the set of channels connected to the node being balanced.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public class LoadBalancedNode<ContextType> implements Processor<ContextType>
{
    private Processor<ContextType> wrapped;
    
    public LoadBalancedNode()
    {        
    }
    
    public LoadBalancedNode(Processor<ContextType> wrapped)
    {
        this.wrapped = wrapped;
    }
    
    public String getName()
    {
        return "LB[" + wrapped.getName() + "]";
    }

    public void process(ContextType ctx, Ports ports) throws InterruptedException
    {
        Ports inPorts = new Ports();
        for (String id : ports.getInputChannelIDs())
            inPorts.getInputMap().put(LoadBalancer.originalChannelId(id), 
                               		  ports.getInput(id));
        for (String id : ports.getOutputChannelIDs())
            inPorts.getOutputMap().put(LoadBalancer.originalChannelId(id), 
                                       ports.getOutput(id));
        wrapped.process(ctx, inPorts);
    }

    public Processor<ContextType> getWrapped()
    {
        return wrapped;
    }

    public void setWrapped(Processor<ContextType> wrapped)
    {
        this.wrapped = wrapped;
    }    
}
