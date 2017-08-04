/**
 * FILE: Logarithm.java
 * PATH: org.datasyslab.babylon.core.utils.Logarithm.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.utils;

// TODO: Auto-generated Javadoc
/**
 * The Class Logarithm.
 */
public class Logarithm {
    
    /**
     * Log.
     *
     * @param value the value
     * @param base the base
     * @return the double
     */
    static public double log(double value, double base) {
    return Math.log(value) / Math.log(base);
    }
}
