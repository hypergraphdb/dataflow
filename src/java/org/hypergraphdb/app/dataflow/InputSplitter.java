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
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * An <code>InputSplitter</code> is used for processing node load balancing. Conceptually,
 * it treats all inputs as a single input channel by constructing an input "tuple". It then
 * redirects that tuple iteratively to one of n tuple output channels. Tuples are not 
 * actually constructed. If there are K input ports, the processor expects K*n output
 * ports, divided into n groups of K outputs. 
 * </p>
 * <p>
 * The output channels are identified by the following naming schema: 
 * <code>outputIdPrefix + inputChannelId + "-" + i</code> where outputIdPrefix is a 
 * naming prefix specific to this splitter (specified at construction time), inputChannelId
 * is the id of the original input channel and 'i' is the i'th (i ranging from 0 to n-1) group
 * of output channels.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public class InputSplitter<ContextType> implements Processor<ContextType>
{
    private int n; 
    private String outputIdPrefix;
    private Map<String, Mapping<Object, Boolean>> batchControlPredicates = 
        new HashMap<String, Mapping<Object, Boolean>>();
    
    public InputSplitter()
    {        
    }
    
    public InputSplitter(int n, String outputIdPrefix)
    {
        this.n = n;
        this.outputIdPrefix = outputIdPrefix;
    }
    
    public String getName()
    {
        return "InputSplitter";
    }

    public Map<String, Mapping<Object, Boolean>> getBatchControlPredicates()
    {
        return batchControlPredicates;
    }
    
    public void setBatchControlPredicates(Map<String, Mapping<Object, Boolean>> batchControlPredicates)
    {
        this.batchControlPredicates = batchControlPredicates;
    }
    
    public void process(ContextType ctx, Ports ports) throws InterruptedException
    {
        Set<String> inputIds = ports.getInputChannelIDs();
        Map<String, Integer> openOutputCount = new HashMap<String, Integer>();        
        boolean done = false;        
        while (!done) 
        {
            for (String inputId : inputIds)
                openOutputCount.put(inputId, 0);
                        
            for (int i = 0; i < n && !done; i++)
            {
                done = true;
                for (Iterator<String> inIter = inputIds.iterator(); inIter.hasNext(); )
                {
                    String inputId = inIter.next();
                    InputPort<?> input = ports.getInput(inputId);
                    if (!input.isOpen())
                    {
                        for (int j = 0; j < n; j++)                            
                        {
                            String outId = LoadBalancer.makeChannelId(inputId, outputIdPrefix, i);
                            ports.getOutput(outId).close();
                        }
                        inIter.remove();
                        continue;
                    }
                    else
                        done = false;
                    Object data = ports.getInput(inputId).take();
                    String outId = LoadBalancer.makeChannelId(inputId, outputIdPrefix, i);
                    if (ports.getOutput(outId).put(data))
                        openOutputCount.put(inputId, openOutputCount.get(inputId) + 1);
                }
            }
            
            for (String inputId : openOutputCount.keySet())
                if (openOutputCount.get(inputId) == 0)
                {
                    InputPort<?> input = ports.getInput(inputId);
                    if (input != null)
                        input.close();
                    inputIds.remove(inputId);
                }            
        }
    }
    
/*    public void process(ContextType ctx, Ports ports) throws InterruptedException
    {
        Set<String> inputIds = ports.getInputChannelIDs();
        Map<String, Integer> openOutputCount = new HashMap<String, Integer>();
        
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
                                       (Mapping<Object, Boolean>)DU.cloneObject(pred, null));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
        }
        
        boolean done = false;        
        while (!done) 
        {
            for (String inputId : inputIds)
                openOutputCount.put(inputId, 0);
                        
            for (int i = 0; i < n && !done; i++)
            {
                done = true;
                for (Iterator<String> inIter = inputIds.iterator(); inIter.hasNext(); )
                {
                    String inputId = inIter.next();
                    InputPort<?> input = ports.getInput(inputId);
                    if (!input.isOpen())
                    {
                        for (int j = 0; j < n; j++)                            
                        {
                            String outId = LoadBalancer.makeChannelId(inputId, outputIdPrefix, i);
                            ports.getOutput(outId).close();
                        }
                        inIter.remove();
                        continue;
                    }
                    else
                        done = false;
                    
                    // Port is open, reads the next batch (if there's no batch predicate for this input
                    // then this input ID is simply the next input datum) and write out to the corresponding
                    // output connected to the current balancing node i.
                    Pair<Integer, String> chKey = new Pair<Integer, String>(i, inputId);
                    Mapping<Object, Boolean> batchPredicate = predicates.get(chKey);
                    String outId = LoadBalancer.makeChannelId(inputId, outputIdPrefix, i);
                    OutputPort<Object> out = ports.getOutput(outId);
                    if (out.isOpen())
                        openOutputCount.put(inputId, openOutputCount.get(inputId) + 1);                        
                    Object data = buffer.get(chKey);
                    if (data != null)
                    {
                        if (batchPredicate != null && batchPredicate.eval(data))
                        {
                            buffer.remove(chKey);
                            if (!out.put(data)) // port closed?
                                continue;
                        }
                        else
                            continue; // skip this channel for now
                    }
                    while (true)
                    {
                        data = input.take();
                        if (input.isEOS(data))
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
            
            for (String inputId : openOutputCount.keySet())
                if (openOutputCount.get(inputId) == 0)
                {
                    InputPort<?> input = ports.getInput(inputId);
                    if (input != null)
                        input.close();
                    inputIds.remove(inputId);
                }            
        }
    }
*/
    public int getN()
    {
        return n;
    }

    public void setN(int n)
    {
        this.n = n;
    }

    public String getOutputIdPrefix()
    {
        return outputIdPrefix;
    }

    public void setOutputIdPrefix(String outputIdPrefix)
    {
        this.outputIdPrefix = outputIdPrefix;
    }    
}
