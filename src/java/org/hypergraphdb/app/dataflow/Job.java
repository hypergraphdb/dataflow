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

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * A data flow job needs to be identifiable as a HyperGraphDB atom. When the network
 * is running entirely locally, job handles are ignored (at least at the time of this
 * writing). However, during a distributed run, they are crucial.                  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface Job
{
    void setHandle(HGHandle handle);
    HGHandle getHandle();
}
