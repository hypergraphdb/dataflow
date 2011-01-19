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
 * Adapt a processing context to a given task. This may involve setting
 * some global parameters to an existing context or creating a new
 * context altogether.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <ContextType> The type of the processing context.
 * @param <TaskType> The type of the processing task.
 */
public interface ContextJobAdapter<ContextType>
{
    ContextType adapt(ContextType context, Job task);
}
