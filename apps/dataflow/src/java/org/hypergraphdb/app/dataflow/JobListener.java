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

import java.util.List;

/**
 * <p>
 * One can listen to start/end job events either at the level of a single
 * JobProcessor or the whole job DFN. In the first case, the "source" of the
 * event will be a {@link JobProcessor} instance while in the second it will
 * be a {@link JobDataFlow} instance.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 * @param <JobType>
 */
public interface JobListener<ContextType>
{
    public void startJob(Job job, ContextType context, Object source);
    public void endJob(Job job, 
    				   ContextType context, 
    				   Object source, 
    				   List<DistributedException> exceptions);
}
