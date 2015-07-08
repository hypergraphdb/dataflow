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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

public class OutputCombiner<ContextType> implements Processor<ContextType>
{
    private int n;
    private String inputIdPrefix;
    private Map<String, Mapping<Object, Boolean>> batchControlPredicates = 
        new HashMap<String, Mapping<Object, Boolean>>();
    
    public OutputCombiner()
    {        
    }
    
    public OutputCombiner(int n, String inputIdPrefix)
    {
        this.n = n;
        this.inputIdPrefix = inputIdPrefix;
    }
    
    public String getName()
    {
        return "OutputCombiner";
    }

    public Map<String, Mapping<Object, Boolean>> getBatchControlPredicates()
    {
        return batchControlPredicates;
    }
    
    public void setBatchControlPredicates(Map<String, Mapping<Object, Boolean>> batchControlPredicates)
    {
        this.batchControlPredicates = batchControlPredicates;
    }
    
    @SuppressWarnings("unchecked")
    public void process(ContextType ctx, Ports ports) throws InterruptedException
    {
        Set<String> outputIds = ports.getOutputChannelIDs();
        
        // Buffer data refused by batch predicate 
        Map<Pair<Integer, String>, Object> buffer = 
            new HashMap<Pair<Integer, String>, Object>();
        
        // Clone a separate batch predicate for each load balanced node.
        Map<Pair<Integer, String>, Mapping<Object, Boolean>> predicates =
            new HashMap<Pair<Integer, String>, Mapping<Object, Boolean>>();       
        for (String chId : batchControlPredicates.keySet())
        {
            Mapping<Object, Boolean> pred = batchControlPredicates.get(chId);
            if (pred != null)
                for (int i = 0; i <  n; i++)
                    try
                    {
                        predicates.put(new Pair<Integer, String>(i, chId), 
                                       (Mapping<Object, Boolean>)HGUtils.cloneObject(pred, null));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
        }
            
        Map<String, Integer> openInputCount = new HashMap<String, Integer>();
        
        boolean done = false;
        while (!done)
        {
            done = true;

            // Count the number of input ports open for each of the output
            // channels: if all load balanced nodes have stopped on that 
            // particular channel, we want to close the channel as an output
            // for this node as well. The count is checked at the end
            // of the following loop and all outputs with a 0 count are closed&removed.
            
            for (String outputId : outputIds)
                openInputCount.put(outputId, 0);
            
            // for each split node
            for (int i = 0; i < n; i++)
            {
                // for each of the original outputs
                for (Iterator<String> outputIter = outputIds.iterator(); outputIter.hasNext(); )
                { 
                    String outputId = outputIter.next();
                    OutputPort<Object> out = ports.getOutput(outputId);
                    
                    // If a particular port is closed for output (because nobody is reading it
                    // upstream), we want to close all inputs from downstream as well.
                    if (!out.isOpen())  
                    {
                        for (int j = 0; j < n; j++)
                        {
                            String inId = LoadBalancer.makeChannelId(outputId, inputIdPrefix, j);
                            ports.getInput(inId).close();
                            Pair<Integer, String> chKey = new Pair<Integer, String>(i, outputId);
                            buffer.remove(chKey);
                        }
                        outputIter.remove();
                        continue;
                    }
                    
                    // Get the output of the current split node that we need to connect to the original
                    // output.
                    String inId = LoadBalancer.makeChannelId(outputId, inputIdPrefix, i);
                    InputPort<?> in = ports.getInput(inId);
                    if (in.isOpen())
                    {
                        done = false;
                        openInputCount.put(outputId, openInputCount.get(outputId) + 1);                        
                        Pair<Integer, String> chKey = new Pair<Integer, String>(i, outputId);
                        Mapping<Object, Boolean> batchPredicate = predicates.get(chKey);
                        Object data = buffer.get(chKey);
                        if (data != null)
                        {
                            if (batchPredicate != null && batchPredicate.eval(data))
                            {
                                buffer.remove(chKey);
                                if (!out.put(data))
                                    continue;
                            }
                            else
                                continue; // skip this channel
                        }
                        while (true) // read batch
                        {
                            data = in.take();
                            if (in.isEOS(data))
                                break;
                            if (batchPredicate == null || batchPredicate.eval(data))
                            {
                                if (!out.put(data))
                                    break;
                            }
                            else
                            {
                                buffer.put(chKey, data);
                                break;
                            }
                        }                        
                    }                        
                }
            }
            
            for (String outputId : openInputCount.keySet())
                if (openInputCount.get(outputId) == 0)
                {
                    OutputPort output = ports.getOutput(outputId);
                    if (output != null)                        
                        output.close();
                    outputIds.remove(outputId);
                }
        }
    }

    public int getN()
    {
        return n;
    }

    public void setN(int n)
    {
        this.n = n;
    }

    public String getInputIdPrefix()
    {
        return inputIdPrefix;
    }

    public void setInputIdPrefix(String inputIdPrefix)
    {
        this.inputIdPrefix = inputIdPrefix;
    }
}
