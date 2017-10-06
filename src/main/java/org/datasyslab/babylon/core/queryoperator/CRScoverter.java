package org.datasyslab.babylon.core.queryoperator;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;

public class CRScoverter {

	public static Geometry TransformCRS(Geometry spatialObject, String sourceEpsgCRSCode, String targetEpsgCRSCode) throws FactoryException, MismatchedDimensionException, TransformException
	{
	    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return JTS.transform(spatialObject,transform);
	}
}
