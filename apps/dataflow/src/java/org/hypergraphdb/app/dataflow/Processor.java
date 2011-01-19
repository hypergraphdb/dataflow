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
 * 
 * <p>
 * A <code>Processor</code> implements the behavior of a <code>ProcessingNode</code>.
 * The <code>process</code> method connects to the rest of network through use of the
 * <code>ports</code> parameter. It is assumed that a processor will return from its
 * <code>process</code> method as soon as its job is complete. This generally happens
 * when there is no more input remaining to be processed and there is no further output
 * to be generated. A processor should generally respond to a thread interrupt in a friendly
 * fashion. That is, it should exit if interrupted via an <code>InterruptedException</code>.
 * </p>
 *
 * <p>
 * While a processor certainly has to make sure all external resources that it needs to close
 * before exit are closed, it is safe to let an exception propagate without closing input/output
 * ports, which will be taken care of by the framework.
 * </p>
 * 
 * <p>
 * Note that this interface does not provide any methods for initialization and destruction
 * (release of external resource). The core <code>DataFlowNetwork</code> framework is not
 * concerned about individual processor's resource management. When a network starts executing
 * it assumes that all its processors have been properly initialized. And shutting it down
 * does not perform any special cleanup on individual processors. A given processor may well 
 * be participating in more than one <code>DataFlowNetwork</code>.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType> The type of a generic contextual object holding global information
 * for the processor to use. Concrete type will depend on the application at hand and it is
 * the same for the whole <code>DataFlowNetwork</code> in which a processor operates.
 */
public interface Processor<ContextType> 
{   
    /**
     * <p>Return a display-friendly name of this processor.</p>
     */
    String getName();
    
    /**
     * <p>
     * Run the processor. This method is invoked by the framework and it will be executed
     * within its own thread. 
     * </p>
     * 
     * @param ctx The context of this processor. This is generally some global data structure
     * shared between processors.
     * @param ports The set of ports (input and output) to which this processor is connected.
     * @throws InterruptedException when the 
     */
	void process(ContextType ctx, Ports ports) throws InterruptedException; 
}
