package org.hypergraphdb.app.dataflow;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * 
 * <p>
 * Represents a predefined network that will create a channels and processing nodes upon
 * calling the <code>create</code> method.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public abstract class PredefinedNetwork<ContextType> extends DataFlowNetwork<ContextType>
{
	public PredefinedNetwork() { }
	public PredefinedNetwork(ContextType ctx) { super(ctx); }
	public abstract void create();
	
	/**
	 * <p>
	 * Find the first node whose name matches the regex pattern argument.
	 * </p>
	 */
	public Processor<ContextType> findOneNode(String pattern)
	{
	    Pattern regex = Pattern.compile(pattern);
	    for (Processor<ContextType> p : getNodes())
	        if (regex.matcher(p.getName()).matches())
	            return p;
	    return null;
	}
	
    /**
     * <p>
     * Find all nodes whose name matches the regex pattern argument.
     * </p>
     */
    public Set<Processor<ContextType>> findAllNodes(String pattern)
    {
        Set<Processor<ContextType>> S = new HashSet<Processor<ContextType>>();
        Pattern regex = Pattern.compile(pattern);
        for (Processor<ContextType> p : getNodes())
            if (regex.matcher(p.getName()).matches())
                S.add(p);
        return S;
    }	
}