/**
 * FILE: CRScoverter.java
 * PATH: org.datasyslab.babylon.core.queryoperator.CRScoverter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.queryoperator;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;

// TODO: Auto-generated Javadoc
/**
 * The Class CRScoverter.
 */
public class CRScoverter {

	/**
	 * Transform CRS.
	 *
	 * @param spatialObject the spatial object
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCRSCode the target epsg CRS code
	 * @return the geometry
	 * @throws FactoryException the factory exception
	 * @throws MismatchedDimensionException the mismatched dimension exception
	 * @throws TransformException the transform exception
	 */
	public static Geometry TransformCRS(Geometry spatialObject, String sourceEpsgCRSCode, String targetEpsgCRSCode) throws FactoryException, MismatchedDimensionException, TransformException
	{
	    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return JTS.transform(spatialObject,transform);
	}
}
