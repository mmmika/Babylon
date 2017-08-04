/**
 * FILE: PhotoFilterFactory.java
 * PATH: org.datasyslab.babylon.extension.photoFilter.PhotoFilterFactory.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.photoFilter;

import org.apache.log4j.Logger;
import org.datasyslab.babylon.core.internalobject.PhotoFilter;

// TODO: Auto-generated Javadoc
/**
 * A factory for creating PhotoFilter objects.
 */
public class PhotoFilterFactory {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(PhotoFilterFactory.class);
    
    /**
     * Gets the photo filter.
     *
     * @param filterName the filter name
     * @param filterRadius the filter radius
     * @return the photo filter
     */
    public static PhotoFilter getPhotoFilter(String filterName, int filterRadius)
    {
        if (filterName.equalsIgnoreCase("boxBlur")||filterName.equalsIgnoreCase("box"))
        {
            return new BoxBlur(filterRadius);
        }
        else if (filterName.equalsIgnoreCase("embose"))
        {
            return new Embose();
        }
        else if (filterName.equalsIgnoreCase("gaussianBlur")||filterName.equalsIgnoreCase("gaussian"))
        {
            return new GaussianBlur(filterRadius);
        }
        else if (filterName.equalsIgnoreCase("outline"))
        {
            return new Outline();
        }
        else if (filterName.equalsIgnoreCase("sharpen"))
        {
            return new Sharpen();
        }
        else
        {
            logger.error("[Babylon][getPhotoFilter] No such photo filter: "+filterName);
            return null;
        }
    }
}
