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
package org.hypergraphdb.app.dataflow.monitor;

import java.util.ArrayList;

/**
 * <p>
 * A simple class to accumulate data about some average quantity. Can
 * be safely used from multiple threads.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class AvgAccumulator
{
    private double total = 0;
    private long count;
    private ArrayList<Double> trace = new ArrayList<Double>();
    
    public AvgAccumulator()
    {        
    }

    public synchronized void reset()
    {
        total = 0;
        count = 0;
    }
    
    public synchronized void add(double datum)
    {
        total += datum;
        count++;
        trace.add(datum);
    }
    
    public synchronized double getAvg()
    {
        return total/count;
    }    
}
