/**
 * FILE: PhotoFilter.java
 * PATH: org.datasyslab.babylon.core.internalobject.PhotoFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.internalobject;

import java.io.Serializable;

import org.apache.log4j.Logger;

// TODO: Auto-generated Javadoc
/**
 * The Class PhotoFilter.
 */
public abstract class PhotoFilter implements Serializable{
	
	/** The filter radius. */
	protected int filterRadius;
	
	/** The convolution matrix. */
	protected Double[][] convolutionMatrix;

	/** The Constant MAX_FILTER_RADIUS. */
	public static final int MAX_FILTER_RADIUS  = 3;

	/** The Constant MIN_PARTITION_INTERVAL. */
	public static final int MIN_PARTITION_INTERVAL = 2*MAX_FILTER_RADIUS+1;

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(PhotoFilter.class);

	/**
	 * Instantiates a new photo filter.
	 *
	 * @param filterRadius the filter radius
	 */
	public PhotoFilter(int filterRadius)
	{
		if (filterRadius>MAX_FILTER_RADIUS) {logger.warn("[Babylon][Constructor] The given photo filter radius is larger than MAX ("+MAX_FILTER_RADIUS+"). It may lead to errors.");}
		this.filterRadius=filterRadius;
		this.convolutionMatrix = new Double[2*filterRadius+1][2*filterRadius+1];
	}
	
	/**
	 * Gets the filter radius.
	 *
	 * @return the filter radius
	 */
	public int getFilterRadius() {
		return filterRadius;
	}
	
	/**
	 * Gets the convolution matrix.
	 *
	 * @return the convolution matrix
	 */
	public Double[][] getConvolutionMatrix() {
		return convolutionMatrix;
	}
}
