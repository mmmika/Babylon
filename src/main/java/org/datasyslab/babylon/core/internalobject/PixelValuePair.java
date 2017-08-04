/**
 * FILE: PixelValuePair.java
 * PATH: org.datasyslab.babylon.core.internalobject.PixelValuePair.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.internalobject;


import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class PixelValuePair.
 */
public class PixelValuePair implements Serializable{
    
    /** The pixel. */
    public Pixel pixel;
    
    /** The value. */
    public Double value;
    
    /**
     * Instantiates a new pixel value pair.
     *
     * @param pixel the pixel
     * @param value the value
     */
    public PixelValuePair(Pixel pixel, Double value)
    {
        this.pixel = pixel;
        this.value = value;
    }

}
