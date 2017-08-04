/**
 * FILE: EarthdataHDFPointMapper.java
 * PATH: org.datasyslab.babylon.core.utils.EarthdataHDFPointMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.sernetcdf.SerNetCDFUtils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import ucar.ma2.Array;
import ucar.nc2.dataset.NetcdfDataset;

// TODO: Auto-generated Javadoc
/**
 * The Class EarthdataHDFPointMapper.
 */
public class EarthdataHDFPointMapper implements FlatMapFunction<String, Geometry>{
	
	/** The offset. */
	private int offset = 0;
	
	/** The increment. */
	private int increment = 1;
	
	/** The root group name. */
	private String rootGroupName = "MOD_Swath_LST";
	
	/** The geolocation field. */
	private String geolocationField = "Geolocation_Fields";
	
	/** The longitude name. */
	private String longitudeName = "Longitude";
	
	/** The latitude name. */
	private String latitudeName = "Latitude";
	
	/** The data field name. */
	private String dataFieldName = "Data_Fields";
	
	/** The data variable name. */
	private String dataVariableName = "LST";

	/** The data variable list. */
	private String[] dataVariableList;
	
	/** The longitude path. */
	private String longitudePath = "";
	
	/** The latitude path. */
	private String latitudePath = "";
	
	/** The data path. */
	private String dataPath = "";
	
	/** The data path list. */
	private String[] dataPathList;
	
	/** The switch coordinate XY. By default, longitude is X, latitude is Y*/
	private boolean switchCoordinateXY=false;
	
	/** The url prefix. */
	private String urlPrefix = "";

	/**
	 * Instantiates a new earthdata HDF point mapper.
	 *
	 * @param offset the offset
	 * @param increment the increment
	 * @param rootGroupName the root group name
	 * @param dataVariableList the data variable list
	 * @param dataVariableName the data variable name
	 * @param switchCoordinateXY the switch coordinate XY
	 */
	public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName, boolean switchCoordinateXY)
	{
		this.offset = offset;
		this.increment = increment;
		this.rootGroupName = rootGroupName;
		this.dataVariableList = dataVariableList;
		this.dataVariableName = dataVariableName;
		this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
		this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
		this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
		this.dataPathList = new String[dataVariableList.length];
		for(int i=0;i<dataVariableList.length;i++)
		{
			dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
		}
		this.switchCoordinateXY = switchCoordinateXY;
	}
	
	/**
	 * Instantiates a new earthdata HDF point mapper.
	 *
	 * @param offset the offset
	 * @param increment the increment
	 * @param rootGroupName the root group name
	 * @param dataVariableList the data variable list
	 * @param dataVariableName the data variable name
	 */
	public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName)
	{
		this.offset = offset;
		this.increment = increment;
		this.rootGroupName = rootGroupName;
		this.dataVariableList = dataVariableList;
		this.dataVariableName = dataVariableName;
		this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
		this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
		this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
		this.dataPathList = new String[dataVariableList.length];
		for(int i=0;i<dataVariableList.length;i++)
		{
			dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
		}
	}
	
	/**
	 * Instantiates a new earthdata HDF point mapper.
	 *
	 * @param offset the offset
	 * @param increment the increment
	 * @param rootGroupName the root group name
	 * @param dataVariableList the data variable list
	 * @param dataVariableName the data variable name
	 * @param switchCoordinateXY the switch coordinate XY
	 * @param urlPrefix the url prefix
	 */
	public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName, 
			boolean switchCoordinateXY, String urlPrefix)
	{
		this.offset = offset;
		this.increment = increment;
		this.rootGroupName = rootGroupName;
		this.dataVariableList = dataVariableList;
		this.dataVariableName = dataVariableName;
		this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
		this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
		this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
		this.dataPathList = new String[dataVariableList.length];
		for(int i=0;i<dataVariableList.length;i++)
		{
			dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
		}
		this.switchCoordinateXY = switchCoordinateXY;
		this.urlPrefix = urlPrefix;
	}
	
	/**
	 * Instantiates a new earthdata HDF point mapper.
	 *
	 * @param offset the offset
	 * @param increment the increment
	 * @param rootGroupName the root group name
	 * @param dataVariableList the data variable list
	 * @param dataVariableName the data variable name
	 * @param urlPrefix the url prefix
	 */
	public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName,
			String urlPrefix)
	{
		this.offset = offset;
		this.increment = increment;
		this.rootGroupName = rootGroupName;
		this.dataVariableList = dataVariableList;
		this.dataVariableName = dataVariableName;
		this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
		this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
		this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
		this.dataPathList = new String[dataVariableList.length];
		for(int i=0;i<dataVariableList.length;i++)
		{
			dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
		}
		this.urlPrefix = urlPrefix;
	}
	
	/**
	 * Call.
	 *
	 * @param hdfAddress the hdf address
	 * @return the iterator
	 * @throws Exception the exception
	 */
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Geometry> call(String hdfAddress) throws Exception {
		NetcdfDataset netcdfSet =  SerNetCDFUtils.loadNetCDFDataSet(urlPrefix+hdfAddress);
		Array longitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet,this.longitudePath);
		Array latitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet,this.latitudePath);
		Array dataArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.dataPath);
		Array[] dataArrayList = new Array[this.dataVariableList.length];
		for(int i=0;i<this.dataVariableList.length;i++)
		{
			dataArrayList[i]=SerNetCDFUtils.getNetCDF2DArray(netcdfSet, dataPathList[i]);
		}
		int[] geolocationShape = longitudeArray.getShape();
	    List<Geometry> hdfData = new ArrayList<Geometry>();
	    GeometryFactory geometryFactory = new GeometryFactory();
	    for (int j = 0; j < geolocationShape[0]; j++) {
	    	for (int i = 0; i < geolocationShape[1]; i++) {
	    		// We probably need to switch longitude and latitude if needed.
	    		Coordinate coordinate = null;
	    		if(switchCoordinateXY)
	    		{
		    		coordinate = new Coordinate(SerNetCDFUtils.getDataSym(longitudeArray, j, i),
		    				SerNetCDFUtils.getDataSym(latitudeArray, j, i),SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
	    		}
	    		else
	    		{
		    		coordinate = new Coordinate(SerNetCDFUtils.getDataSym(latitudeArray, j, i),
		    				SerNetCDFUtils.getDataSym(longitudeArray, j, i),SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
	    		}
	    		Point observation = geometryFactory.createPoint(coordinate);
	    		String userData = "";
	    		for(int k=0;k<dataVariableList.length;k++)
	    		{
	    			userData+=SerNetCDFUtils.getDataAsym(dataArrayList[k], j, i, offset, increment);
	    		}
	    		observation.setUserData(userData);
	    		hdfData.add(observation);
	    	}
	    }
		return hdfData.iterator();
	}

}
