# Babylon 
[![Build Status](https://travis-ci.org/DataSystemsLab/Babylon.svg?branch=master)](https://travis-ci.org/DataSystemsLab/Babylon)
### Massive-Scale GeoSpatial Visual Analytics System

## Introduction
GeoVisual analytics, abbr. **GeoViz**, is the science of analytical reasoning assisted by GeoVisual map interfaces.

* Phase I: Spatial Data Preparation: In this phase, the system ﬁrst loads the designated spatial data from the database (e.g., Shape ﬁles, PostGIS, HDFS). Based on the application, the system may then need to perform a data processing operation (e.g., spatial range query, spatial join) on the loaded spatial data to return the set of spatial objects to be visualized.

* Phase II: Map Visualization (MapViz): In this phase, the system applies the map visualization effect, e.g., Heatmap, on the spatial objects produced in Phase I. The system ﬁrst pixelizes the spatial objects, calculates the intensity of each pixel, and ﬁnally renders a geospatial map tile(s).

**Babylon** is a full-ﬂedged system that allows the user to load, prepare, integrate and execute GeoViz tasks on spatial data at scale. Most importantly, Babylon co-optimizes the spatial query operators (e.g., spatial join) and MapViz operators (e.g., pixelization) side by side. This prototype is implemented in Apache Spark, SparkSQL.

## Components
### Declarative GeoViz
To combine both the map visualization and data preparation phases, Babylon allows users to declaratively deﬁne a geospatial visual analytics (GeoViz) task using a SQL-like language.
### MapViz operator
Babylon encapsulates the main steps of the geospatial map visualization process, e.g., pixelize spatial objects, aggregate overlapped pixels, render map tiles, into a set of massively parallelized Map visualization operators. Such MapViz operators provide out-of-the-box support for the user to declaratively generate a variety of map visualization effects, e.g., scatter plot, heat map.

### Query operator
Babylon combines spatial query operators (from existings systems, such as GeoSpark or others) with MapViz operators to assemble an execution plan. Currently, Babylon supports spatial range query and spatial join query operators.

### GeoViz-aware spatial data partitioner
Babylon employs a partitioner operator that fragments a given pixel dataset across the cluster. The partitioner accommodates map visual constraints and also balances the load among the cluster nodes when processing skewed geospatial data. Therefore, Babylon can easily render a map image tile using pixels on the same data partition.

### GeoViz optimizer
The optimizer takes as input a GeoViz query and ﬁgures out an execution plan that co-optimizes the map visualization operators and spatial query operators (i.e., used for data preparation).

## How to get started

### Compile and play

1. Git clone ```git clone https://github.com/DataSystemsLab/Babylon.git```
2. Compile ```mvn clean install -DskipTests```
3. Play with example queries: [GeoViz query example](https://github.com/DataSystemsLab/Babylon/blob/master/src/test/java/org/datasyslab/babylon/TestGeovizQuery.java)

### Assemble your optimized visual analytics execution plan
We are still working on the stable release of Babylon GeoViz query optimizer with SparkSQL support. But, for now, you still can manually assemble an optimized execution plan which fits in your scenario. In the example mentioned above, we provide two optimized GeoViz query:

* **Optimized GeoViz query 1**: Visualize multiple range queries, each with an arbitrary query window, to multiple maps
* **Optimized GeoViz query 2**: Visualize spatial join query to a map

## Contact


### Contact
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2 at asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat at asu.edu)


### Data Systems Lab
Babylon is one of the projects initiated by [Data Systems Lab](http://www.datasyslab.org/) at Arizona State University. The mission of Data Systems Lab is designing and developing experimental data management systems (e.g., database systems).
