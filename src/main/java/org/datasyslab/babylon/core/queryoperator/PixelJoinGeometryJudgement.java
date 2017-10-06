package org.datasyslab.babylon.core.queryoperator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.babylon.core.internalobject.Pixel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PixelJoinGeometryJudgement<T extends Geometry> implements FlatMapFunction2<Iterator<Tuple2<Pixel,Double>>, Iterator<T>, Tuple2<Pixel,Double>>, Serializable {

    @Override
    public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> tuple2Iterator, Iterator<T> geometryIterator) throws Exception {
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        List<Geometry> windowObjects = new ArrayList<Geometry>();

        GeometryFactory geometryFactory = new GeometryFactory();

        while(geometryIterator.hasNext())
        {
        	windowObjects.add(geometryIterator.next());
        }
        while (tuple2Iterator.hasNext())
        {
        	Tuple2<Pixel, Double> queryObject = tuple2Iterator.next();
        	for (int i=0;i<windowObjects.size();i++)
        	{
        		if(windowObjects.get(i).contains(geometryFactory.createPoint(new Coordinate(queryObject._1.getX(),queryObject._1.getY()))))
        		{
        			result.add(queryObject);
        			break;
        		}
        	}
        }
        return result.iterator();
    }
}
