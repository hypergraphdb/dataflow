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
 * Represents an exception originating from the DataFlow framework.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DataFlowException extends RuntimeException
{
	private static final long serialVersionUID = -1;
	
	public DataFlowException(String msg)
	{
		super(msg);
	}
	
	public DataFlowException(String msg, Throwable cause)
	{
		super(msg, cause);
	}
	
	public DataFlowException(Throwable cause)
	{
		super(cause);
	}	
}
