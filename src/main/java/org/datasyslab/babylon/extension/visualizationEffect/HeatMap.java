/**
 * FILE: HeatMap.java
 * PATH: org.datasyslab.babylon.extension.visualizationEffect.HeatMap.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.visualizationEffect;


import org.apache.log4j.Logger;

import org.datasyslab.babylon.core.GenericVizEffect;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.enumerator.SpatialAggregatorOption;
import org.datasyslab.babylon.extension.coloringRule.PiecewiseFunction;

// TODO: Auto-generated Javadoc
/**
 * The Class HeatMap.
 */
public class HeatMap extends GenericVizEffect{

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(HeatMap.class);

	/**
	 * Instantiates a new heat map.
	 *
	 * @param globalParameter the global parameter
	 */
	public HeatMap(GlobalParameter globalParameter) {
		super(globalParameter);
	}

	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.GenericVizEffect#SetParameter()
	 */
	@Override
	public boolean SetParameter()
	{
		this.globalParameter.spatialAggregatorOption = SpatialAggregatorOption.COUNT;
		this.globalParameter.coloringRule = new PiecewiseFunction();
		return true;
	}
}
