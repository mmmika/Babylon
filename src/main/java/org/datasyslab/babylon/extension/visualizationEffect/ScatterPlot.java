/**
 * FILE: ScatterPlot.java
 * PATH: org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.visualizationEffect;

import org.apache.log4j.Logger;
import org.datasyslab.babylon.core.GenericVizEffect;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.enumerator.SpatialAggregatorOption;

import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.babylon.extension.coloringRule.LinearFunction;

// TODO: Auto-generated Javadoc
/**
 * The Class ScatterPlot.
 */
public class ScatterPlot extends GenericVizEffect{

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(ScatterPlot.class);

	/**
	 * Instantiates a new scatter plot.
	 *
	 * @param globalParameter the global parameter
	 */
	public ScatterPlot(GlobalParameter globalParameter) {
		super(globalParameter);
	}

	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.GenericVizEffect#SetParameter()
	 */
	@Override
	public boolean SetParameter()
	{
		this.globalParameter.spatialAggregatorOption = SpatialAggregatorOption.UNIFORM;
		this.globalParameter.coloringRule = new LinearFunction();
		return true;
	}
}
