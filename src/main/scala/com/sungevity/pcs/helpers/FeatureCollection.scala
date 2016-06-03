package com.sungevity.pcs.helpers

import java.io.OutputStream
import java.net.URI
import java.util.UUID

import FSUtils._
import com.vividsolutions.jts.geom._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.data.{DataStoreFinder, DataStore, DefaultTransaction}
import org.geotools.feature.FeatureIterator
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.referencing.CRS
import org.opengis.coverage.grid.GridCoverage
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

/**
  * A class that represents collection of features. The collection can be read from a GeoJSON or Shapefile, or passed
  * as a parameter.
  *
  * @param crs a coordinate reference system
  * @param geometries list
  * @param attributes
  */
case class FeatureCollection(crs: CoordinateReferenceSystem, schema: Option[Map[String, Class[_]]] = None, geometries: Seq[Geometry] = Seq.empty, attributes: Seq[Map[String, AnyRef]] = Seq.empty) {

  private val GEOM: String = "geom"

  private val THE_GEOM: String = "the_" + GEOM

  private implicit def simpleFeatureIterator[F <: Feature](it: FeatureIterator[F]) = new Iterator[F] {

    override def hasNext: Boolean = if (!it.hasNext) {
      it.close()
      false
    } else true

    override def next(): F = it.next()

  }

  /**
    * List features
    *
    * @return an instance of [[SimpleFeatureCollection]]
    */
  def features(collectionName: String = UUID.randomUUID().toString): SimpleFeatureCollection = {
    val featureType = createLocalFeatureType(crs, classOf[Polygon], collectionName)

    val featureCollection = createFeatureCollection(featureType, { (builder: SimpleFeatureBuilder, geom: Geometry, attributes: Map[String, AnyRef]) =>
      builder.set(THE_GEOM, geom)
      attributes.foreach{
        attr => builder.set(attr._1, attr._2)

      }
    })

    featureCollection
  }

  /**
    * Output content of collection to a GeoJSON file
    *
    * @param out output stream to a file
    */
  def writeGeoJSON(out: OutputStream): Unit ={
    new FeatureJSON().writeFeatureCollection(features(), out)
  }

  def writePostGIS(host: String, port: Int, user: String, password: String, database: String, schema: String, tableName: String): Unit = {
    val params = Map(
      "dbtype" -> "postgis",
      "host" -> host,
      "port" -> port,
      "schema" -> schema,
      "database" -> database,
      "user" -> user,
      "passwd" -> password
    )

    val dataStore = DataStoreFinder.getDataStore(params)

    val collection = features(tableName)

    if(!dataStore.getTypeNames.exists(t => t == tableName)){
      dataStore.createSchema(collection.getSchema)
    }

    writeFeatures(dataStore, collection, Some(crs))

    dataStore.dispose()

  }

  /**
    * Add geometries to the collection.
    *
    * @param polygons a sequence of new polygons to add
    * @param attributes attributes of polygons
    * @return
    */
  def ++(polygons: Seq[Geometry], attributes: Seq[Map[String, AnyRef]] = Seq.empty): FeatureCollection = {
    copy(
      geometries = this.geometries ++ polygons,
      attributes = this.attributes ++ attributes
    )
  }

  private def createFeatureCollection(featureType: SimpleFeatureType, featureSetter: (SimpleFeatureBuilder, Geometry, Map[String, AnyRef]) => Unit): SimpleFeatureCollection = {
    val result = new ListFeatureCollection(featureType)

    val featureBuilder = new SimpleFeatureBuilder(featureType)

    for (((geom, attrs), idx) <- geometries.zip(attributes).zipWithIndex) {

      featureSetter(featureBuilder, geom, attrs)

      val f = featureBuilder.buildFeature(idx.toString)

      f.getAttributes.iterator().foreach {
        case g: Geometry => g.setSRID(srid(crs))
        case _ =>
      }

      result.add(f)
    }

    result
  }

  private def writeFeatures(dataStore: DataStore, featureCollection: SimpleFeatureCollection, coordinateReferenceSystem: Option[CoordinateReferenceSystem]): Unit = {

    val transaction = new DefaultTransaction("import")

    try {
      val featureSource = dataStore.getFeatureSource(featureCollection.getSchema.getTypeName)

      featureSource match {
        case featureStore: SimpleFeatureStore =>
          featureStore.setTransaction(transaction)
          featureStore.addFeatures(featureCollection)
          transaction.commit()
        case _ =>
          throw new RuntimeException(s"${featureCollection.getSchema.getTypeName} does not support read/write access")
      }
    } finally {
      transaction.close()
    }
  }

  private def createLocalFeatureType(crs: CoordinateReferenceSystem, geometryClass: Class[_ <: Geometry], name: String): SimpleFeatureType = {
    require(geometryClass != null && name != null)

    val builder = new SimpleFeatureTypeBuilder

    builder.setName(name)
    builder.setNamespaceURI("http://sungevity.com/")
    builder.setCRS(crs)
    builder.setDefaultGeometry(THE_GEOM)
    builder.add(THE_GEOM, geometryClass)
    schema.map {
      s =>
        s.foreach {
          case (name, t) => builder.add(name, t)
        }
    }

    builder.buildFeatureType
  }

  private def srid(crs: CoordinateReferenceSystem) = Try {
    val e = CRS.lookupEpsgCode(crs, false)
    e.intValue()
  }.getOrElse(0)

}

object FeatureCollection {

  private implicit def simpleFeatureIterator[F <: Feature](it: FeatureIterator[F]) = new Iterator[F] {

    override def hasNext: Boolean = if (!it.hasNext) {
      it.close()
      false
    } else true

    override def next(): F = it.next()

  }

  /**
    * Load collection of features from a GeoJSON file
    *
    * @param source a link to a GeoJSON file
    * @return a instance of [[FeatureCollection]]
    */
  def apply(source: URI): FeatureCollection = {

    val reader = new FeatureJSON()

    val crs = source.open{
      stream =>
        reader.readCRS(stream)
    }

    val features = source.open{
      stream =>
        reader.readFeatureCollection(stream).features().map(v => v.asInstanceOf[SimpleFeature])
    } toArray

    val geoms = features.map(_.getDefaultGeometry.asInstanceOf[Geometry]).toSeq

    val schema = features.headOption map {
      f =>
        val schemaMap = mutable.Map.empty[String, Class[_]]
        for(t <- f.getFeatureType.getTypes){
          schemaMap(t.getName.toString) = t.getBinding
        }
        schemaMap.toMap
    }

    new FeatureCollection(crs, schema, geoms)

  }

  /**
    * Instantiate an empty instance of [[FeatureCollection]]
    *
    * @param coverage an instance of [[GridCoverage]] that will be used to get coordinate reference system
    * @return a instance of [[FeatureCollection]]
    */
  def apply(coverage: GridCoverage, schema: Map[String, Class[_]]): FeatureCollection = {

    new FeatureCollection(coverage.getCoordinateReferenceSystem, Some(schema))

  }
}
