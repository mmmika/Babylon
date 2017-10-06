/**
 * FILE: PixelAggregatorOption.java
 * PATH: org.datasyslab.babylon.core.enumerator.PixelAggregatorOption.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.enumerator;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Enum PixelAggregatorOption.
 */
public enum PixelAggregatorOption implements Serializable{

	/** The average. */
	SUM("sum"),

	/** The max. */
	MAX("max"),

	/** The min. */
	MIN("min"),

	/** The count. */
	COUNT("count"),

	/** The uniform. */
	UNIFORM("uniform");

	/** The type name. */
	private String typeName="uniform";

	private PixelAggregatorOption(String typeName)
	{
		this.setTypeName(typeName);
	}


	/**
	 * Gets the spatial aggregator option.
	 *
	 * @param str the str
	 * @return the spatial aggregator option
	 */
	public static PixelAggregatorOption getSpatialAggregatorOption(String str) {
		for (PixelAggregatorOption me : PixelAggregatorOption.values()) {
			if (me.name().equalsIgnoreCase(str))
				return me;
		}
		return null;
	}

	/**
	 * Gets the type name.
	 *
	 * @return the type name
	 */
	public String getTypeName() {
		return typeName;
	}

	/**
	 * Sets the type name.
	 *
	 * @param typeName the new type name
	 */
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
}
