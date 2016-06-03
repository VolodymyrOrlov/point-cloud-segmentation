package com.sungevity.pcs

import java.awt.Font
import java.awt.geom.AffineTransform
import java.io.File
import java.net.URI
import javax.swing.{JComponent, JPanel, JTabbedPane, JFrame}

import breeze.linalg._
import com.sungevity.pcs.helpers.FSUtils._
import com.sungevity.analytics.imageio.Image
import com.sungevity.analytics.utils.Configuration._
import com.sungevity.analytics.utils.Reflection._
import com.sungevity.analytics.utils.IOUtils._
import com.sungevity.pcs.helpers.FeatureCollection
import com.sungevity.pcs.segmentation.Segmentation
import com.vividsolutions.jts.geom.{Polygon, Geometry}
import com.vividsolutions.jts.geom.util.AffineTransformation
import it.geosolutions.imageioimpl.plugins.tiff.{TIFFImageWriterSpi, TIFFImageReaderSpi}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.geotiff.GeoTiffReader
import org.opengis.metadata.spatial.PixelOrientation

object Main extends App {

  if (args.size < 1) {
    sys.error("You forgot to specify a config file.")
    sys.exit(1)
  }

  implicit val config = configuration(args).resolve()

  val sparkConf = this.getClass.jarFile map {
    jar =>
      val conf = new SparkConf().setAppName(s"Plane Growth Algorithm.").
        setJars(Seq(jar.getAbsolutePath))
      config.getOptionalString("com.sungevity.smt.facets.master-url") match {
        case Some(value) => conf.setMaster(value)
        case _ => conf
      }
  } getOrElse {
    new SparkConf().setAppName(s"Image Classifier: ${args(0)}").setMaster("local[4]")
  }

  implicit val sc = new SparkContext(sparkConf)

  val inputs = recursiveListFiles(config.getString("com.sungevity.pcs.input.elevation-maps-path")).filter(_.getName.endsWith(".tif"))

  val outlines = recursiveListFiles(config.getString("com.sungevity.pcs.input.outline-filter.path")).filter(_.getName.endsWith(".tif"))

  val color = recursiveListFiles(config.getString("com.sungevity.pcs.input.raster.path")).filter(_.getName.endsWith(".tif"))

  val segmentation = new Segmentation()

  val debugWindow = config.getOptionalBoolean("com.sungevity.pcs.debug").getOrElse(false) match {

    case true => Some {
      val frame = new JFrame("Debug")
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
      frame.setSize(frame.getMaximumSize)
      val tabbedPane = new JTabbedPane()
      frame.add(tabbedPane)
      (frame, tabbedPane)
    }

    case false => None

  }

  inputs.foreach {
    input =>

      println(s"Processing $input")

      val inputImage = Image(input.getAbsolutePath)(new TIFFImageReaderSpi)

      val elevationMap = inputImage.pixels.map{
        v => (v & 0x00ffffff).toDouble
      }

      val name =  FilenameUtils.getBaseName(input.getAbsolutePath)

      val debugPanel = debugWindow.map {
        case (_, pane) =>
          val panel = new JPanel()
          pane.addTab(name, panel)
          panel
      }

      val color = outlines.find(_.getName == input.getName) map {
        f =>
          val im = Image(f.getAbsolutePath)(new TIFFImageReaderSpi)
          im.intensities
      } getOrElse(DenseMatrix.zeros[Double](elevationMap.rows, elevationMap.cols))

      outlines.find(_.getName == input.getName) map {
        f =>
          val filterImage = Image(f.getAbsolutePath)(new TIFFImageReaderSpi)
          filterImage.pixels.foreachPair{
            case ((x, y), v) if v != 0xffff1111 =>
              elevationMap.update(x, y, 0)
            case _ =>
          }
      }

      val normalizedElevationMap = normalize(elevationMap)

      heatmap(normalizedElevationMap, debugPanel)

      val result = segmentation.segment(name, normalizedElevationMap, color, debugPanel)

      config.getOptionalString("com.sungevity.pcs.output.geotiff-path").map {
        path =>
          (new URI(path) + s"$name.tif").create(true) {
            stream =>
              Image.fromPixels(colorOutput(result)).write(stream)(new TIFFImageWriterSpi)
          }

      }

      config.getOptionalString("com.sungevity.pcs.output.geojson-path").map {
        path =>

          val coverage = readCoverage(input)

          val k = max(result)

          val resultImage = Image.fromPixels(result)

          var features = FeatureCollection(coverage, Map.empty[String, Class[_]])

          for {
            color <- (1 to k + 1)
            extracted = extractBand(resultImage, color)
            polygons = extracted.vectorize(filterThreshold = 100).map(transformPolygon(coverage))
          } yield {
            features = features ++(polygons, Seq.fill(polygons.size)(Map.empty[String, AnyRef]))
          }

          (new URI(path) + s"$name.geojson").create(true) {
            stream =>
              features.writeGeoJSON(stream)
          }
      }

  }

  debugWindow.foreach(_._1.setVisible(true))

  private def transformPolygon(coverage: GridCoverage2D)(polygon: Geometry): Geometry = {

    val jtsTransformation = {
      val mt2D = coverage.getGridGeometry.getGridToCRS2D(PixelOrientation.UPPER_LEFT).asInstanceOf[AffineTransform]

      new AffineTransformation(
        mt2D.getScaleX,
        mt2D.getShearX,
        mt2D.getTranslateX,
        mt2D.getShearY,
        mt2D.getScaleY,
        mt2D.getTranslateY)
    }

    val p = polygon.clone.asInstanceOf[Polygon]

    p.apply(jtsTransformation)

    p
  }

  private def normalize(data: DenseMatrix[Double]): DenseMatrix[Double] = {
    ((data - min(data)) / max(data)) :* 1000.0
  }

  private def colorOutput(output: DenseMatrix[Int]): DenseMatrix[Int] = {
    val maxLabel = max(output)
    val maxColor = 255

    val colorEdge = if(maxLabel > 0) maxColor / maxLabel else 0xff0000

    output.map {
      v =>
        if(v > 0){
          val c = (colorEdge * v) & 0xff
          0xff000000 | (c << 16) | (c << 8) | c
        } else 0
    }


  }

  private def readCoverage(file: File): GridCoverage2D = new GeoTiffReader(file, null).read(null)

  private def extractBand(img: Image, band: Int): Image = {

    img.withPixels {
      img.pixels.map {
        p =>
          if ((0x00ffffff & p) == band) 0xffffffff
          else 0
      }
    }

  }

  private def heatmap(data: DenseMatrix[Double], dst: Option[JComponent]) = dst.foreach {
    jComponent =>

      import smile.plot._

      val plotData = Array.fill(data.cols)(Array.fill(data.rows)(0.0))
      data.iterator.foreach{
        case ((x, y), v) =>
          plotData(y)(x) = v
      }

      val chart = Heatmap.plot(plotData, Palette.heat(256, 1.0f))
      chart.setTitle("height map")
      chart.setVisible(true)
      chart.setTitleFont(new Font("Default", Font.PLAIN, 10))

      jComponent.add(chart)
  }

}
