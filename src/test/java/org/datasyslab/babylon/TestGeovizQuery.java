/**
 * FILE: TestGeovizQuery.java
 * PATH: org.datasyslab.babylon.TestGeovizQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon;

import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.GenericVizEffect;
import org.datasyslab.babylon.core.ImageGenerator;
import org.datasyslab.babylon.core.ImageStitcher;
import org.datasyslab.babylon.core.enumerator.ImageType;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.queryoperator.CRScoverter;
import org.datasyslab.babylon.example.JoinViz;
import org.datasyslab.babylon.example.MultiRangeViz;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class TestGeovizQuery.
 */
public class TestGeovizQuery {



    /**
     * Test multi range viz scatter plot.
     */
    @org.junit.Test
    public void testMultiRangeVizScatterPlot() {
        setLogLevel();
        SparkConf conf = new SparkConf().setAppName("testMultiRangeVizScatterPlot").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"arealm.csv";
        String outputLocation = "target/demo/arealm-multirange-scatterplot";
        PointRDD testRDD = new PointRDD(sc, inputLocation,0, FileDataSplitter.CSV,false,4);
        System.setProperty("org.geotools.referencing.forceXY", "true");

        testRDD.CRSTransform("epsg:4326","epsg:3857");
        List<Polygon> queryWindows = new ArrayList<Polygon>();
             
        try {
			queryWindows.add((Polygon) CRScoverter.TransformCRS(getUSMainLandBoundary(true), "epsg:4326", "epsg:3857"));
			queryWindows.add((Polygon) CRScoverter.TransformCRS(getArizona(true), "epsg:4326", "epsg:3857"));
		} catch (MismatchedDimensionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        GlobalParameter globalParameter = GlobalParameter.getGlobalParameter();
        globalParameter.set("minTreeLevel:4");
        globalParameter.set("maxTreeLevel:6");
        GenericVizEffect multiRangeViz = new MultiRangeViz(globalParameter,queryWindows,outputLocation);

        multiRangeViz.Visualize(sc,testRDD);
        sc.stop();
    }

    /**
     * Test multi range viz heat map.
     */
    @org.junit.Test
    public void testMultiRangeVizHeatMap() {
        setLogLevel();
        SparkConf conf = new SparkConf().setAppName("testMultiRangeVizScatterPlot").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"arealm.csv";
        String outputLocation = "target/demo/arealm-multirange-heatmap";
        PointRDD testRDD = new PointRDD(sc, inputLocation,0, FileDataSplitter.CSV,false,4);
        System.setProperty("org.geotools.referencing.forceXY", "true");

        testRDD.CRSTransform("epsg:4326","epsg:3857");
        List<Polygon> queryWindows = new ArrayList<Polygon>();
             
        try {
			queryWindows.add((Polygon) CRScoverter.TransformCRS(getUSMainLandBoundary(true), "epsg:4326", "epsg:3857"));
			queryWindows.add((Polygon) CRScoverter.TransformCRS(getArizona(true), "epsg:4326", "epsg:3857"));
		} catch (MismatchedDimensionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        GlobalParameter globalParameter = GlobalParameter.getGlobalParameter();
        globalParameter.set("minTreeLevel:4");
        globalParameter.set("maxTreeLevel:6");
        globalParameter.set("aggregation:count");
        globalParameter.set("coloringRule:piecewise");
        //globalParameter.set("maxPixelWeight:1");
        GenericVizEffect multiRangeViz = new MultiRangeViz(globalParameter,queryWindows,outputLocation);

        multiRangeViz.Visualize(sc,testRDD);
        sc.stop();
        }
    
    /**
     * Test join viz scatter plot.
     */
    @org.junit.Test
    public void testJoinVizScatterPlot() {
        setLogLevel();
        SparkConf conf = new SparkConf().setAppName("testJoinVizScatterPlot").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"arealm.csv";
        String outputLocation = "target/demo/arealm-joinviz-scatterplot";
        PointRDD testRDD = new PointRDD(sc, inputLocation,0, FileDataSplitter.CSV,false,4);
        System.setProperty("org.geotools.referencing.forceXY", "true");

        testRDD.CRSTransform("epsg:4326","epsg:3857");

        String windowRDDLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"real-state.csv";
        PolygonRDD windowRDD = new PolygonRDD(sc,windowRDDLocation,0,-1,FileDataSplitter.WKT,false,6);
        windowRDD.CRSTransform("epsg:4326","epsg:3857");
        
        GlobalParameter globalParameter = GlobalParameter.getGlobalParameter();
        globalParameter.set("minTreeLevel:4");
        globalParameter.set("maxTreeLevel:6");
        GenericVizEffect joinViz = new JoinViz(globalParameter,windowRDD);
        joinViz.Visualize(sc,testRDD);
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(joinViz.getDistributedRasterImage(),outputLocation, ImageType.PNG,globalParameter);
        ImageStitcher.stitchImagePartitionsFromLocalFile(outputLocation,globalParameter);
        sc.stop();
    }

    /**
     * Test join viz heat map.
     */
    @org.junit.Test
    public void testJoinVizHeatMap() {
        setLogLevel();
        SparkConf conf = new SparkConf().setAppName("testJoinVizHeatMap").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"arealm.csv";
        String outputLocation = "target/demo/arealm-joinviz-heatmap";
        PointRDD testRDD = new PointRDD(sc, inputLocation,0, FileDataSplitter.CSV,false,4);
        System.setProperty("org.geotools.referencing.forceXY", "true");

        testRDD.CRSTransform("epsg:4326","epsg:3857");

        String windowRDDLocation = "file://"+System.getProperty("user.dir")+"/src/test/resources/"+"real-state.csv";
        PolygonRDD windowRDD = new PolygonRDD(sc,windowRDDLocation,0,-1,FileDataSplitter.WKT,false,6);
        windowRDD.CRSTransform("epsg:4326","epsg:3857");
        
        GlobalParameter globalParameter = GlobalParameter.getGlobalParameter();
        globalParameter.set("minTreeLevel:4");
        globalParameter.set("maxTreeLevel:6");
        globalParameter.set("aggregation:count");
        globalParameter.set("coloringRule:piecewise");
        
        GenericVizEffect joinViz = new JoinViz(globalParameter,windowRDD);
        joinViz.Visualize(sc,testRDD);
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(joinViz.getDistributedRasterImage(),outputLocation, ImageType.PNG,globalParameter);
        ImageStitcher.stitchImagePartitionsFromLocalFile(outputLocation,globalParameter);
        sc.stop();
        }
    
    
    /**
     * Sets the log level.
     */
    public static void setLogLevel()
    {
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        Logger.getLogger("com").setLevel(Level.WARN);
    }
    
    /**
     * Gets the arizona.
     *
     * @param swapXY the swap XY
     * @return the arizona
     */
    public static Polygon getArizona(boolean swapXY)
    {
        Envelope ArizonaBoundary = new Envelope(37.153,31.567,-114.923,-109.172);
        if (swapXY)
        {
            ArizonaBoundary = new Envelope(-114.923,-109.172,37.153,31.567);
        }
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(ArizonaBoundary.getMinX(),ArizonaBoundary.getMinY());
        coordinates[1] = new Coordinate(ArizonaBoundary.getMaxX(),ArizonaBoundary.getMinY());
        coordinates[2] = new Coordinate(ArizonaBoundary.getMaxX(),ArizonaBoundary.getMaxY());
        coordinates[3] = new Coordinate(ArizonaBoundary.getMinX(),ArizonaBoundary.getMaxY());
        coordinates[4] = coordinates[0];
        Polygon polygon = geometryFactory.createPolygon(coordinates);
        return polygon;
    }

    /**
     * Gets the manhattan.
     *
     * @param swapXY the swap XY
     * @return the manhattan
     */
    public static Polygon getManhattan(boolean swapXY)
    {
        Envelope ManhattanBoundary = new Envelope(40.7,40.911,-74.008,-73.879);
        if (swapXY)
        {
            ManhattanBoundary = new Envelope(-74.008,-73.879,40.7,40.911);
        }
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(ManhattanBoundary.getMinX(),ManhattanBoundary.getMinY());
        coordinates[1] = new Coordinate(ManhattanBoundary.getMaxX(),ManhattanBoundary.getMinY());
        coordinates[2] = new Coordinate(ManhattanBoundary.getMaxX(),ManhattanBoundary.getMaxY());
        coordinates[3] = new Coordinate(ManhattanBoundary.getMinX(),ManhattanBoundary.getMaxY());
        coordinates[4] = coordinates[0];
        Polygon polygon = geometryFactory.createPolygon(coordinates);
        return polygon;
    }
    
    /**
     * Gets the US main land boundary.
     *
     * @param swapXY the swap XY
     * @return the US main land boundary
     */
    public static Polygon getUSMainLandBoundary(boolean swapXY)
    {
        Envelope USMainLandBoundary = new Envelope(24.863836,50.000, -126.790180,-64.630926);
        if (swapXY)
        {
        	USMainLandBoundary = new Envelope(-126.790180,-64.630926,24.863836,50.000);
        }
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(USMainLandBoundary.getMinX(),USMainLandBoundary.getMinY());
        coordinates[1] = new Coordinate(USMainLandBoundary.getMaxX(),USMainLandBoundary.getMinY());
        coordinates[2] = new Coordinate(USMainLandBoundary.getMaxX(),USMainLandBoundary.getMaxY());
        coordinates[3] = new Coordinate(USMainLandBoundary.getMinX(),USMainLandBoundary.getMaxY());
        coordinates[4] = coordinates[0];
        LinearRing linearRing = geometryFactory.createLinearRing(coordinates);
        Polygon polygon = geometryFactory.createPolygon(linearRing);
        return polygon;
    }

}

