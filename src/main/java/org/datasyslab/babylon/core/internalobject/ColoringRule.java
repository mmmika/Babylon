/**
 * FILE: ColoringRule.java
 * PATH: org.datasyslab.babylon.core.internalobject.ColoringRule.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.internalobject;

import java.io.Serializable;

import org.datasyslab.babylon.core.parameters.GlobalParameter;

// TODO: Auto-generated Javadoc
/**
 * The Class ColoringRule.
 */
public abstract class ColoringRule implements Serializable{

    /**
     * Encode to RGB.
     *
     * @param normailizedCount the normailized count
     * @param globalParameter the global parameter
     * @return the integer
     * @throws Exception the exception
     */
    public abstract Integer EncodeToRGB(Double normailizedCount, final GlobalParameter globalParameter) throws Exception;
}
