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
 * A base class for implementing processors. This class provides a default implementation of 
 * the <code>getName</code> that returns the (non-qualified) name of the concrete 
 * implementation class.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public abstract class AbstractProcessor<ContextType> implements Processor<ContextType>
{
    public String getName()
    {
        String name = this.getClass().getName();
        int i = name.lastIndexOf('.');
        if (i > -1)
            name = name.substring(i + 1);
        return name;
    }
}
