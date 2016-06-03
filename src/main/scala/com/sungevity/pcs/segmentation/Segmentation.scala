package com.sungevity.pcs.segmentation

import java.awt._
import javax.swing.{JComponent, JPanel, JFrame}

import breeze.linalg._
import breeze.numerics._
import com.sungevity.analytics.imageio.utils.KDTree
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import smile.clustering.GMeans
import smile.plot.Palette

class Segmentation {

  def segment(id: String, input: DenseMatrix[Double], color: DenseMatrix[Double], debug: Option[JComponent])(implicit sc: SparkContext, config: Config) = {

    val seedFilter = DenseMatrix.fill(input.rows, input.cols)(true)

    val lPlanes = localPlanes(50, input, seedFilter)

    val result = DenseMatrix.zeros[Int](input.rows, input.cols)

    custer(lPlanes, color, result, debug)

    result

  }

  private def custer(lPlanes: DenseMatrix[DenseVector[Double]], color: DenseMatrix[Double], output: DenseMatrix[Int], debug: Option[JComponent])(implicit sc: SparkContext) = {

    val ff = lPlanes.iterator.filter(_._2.length > 0).map(s => s._2(0 to 2).toArray ++ Array(s._2(3) / 1000.0, s._2(7), color(s._1._1, s._1._2))).toArray

    val pp = lPlanes.valuesIterator.filter(_.length > 0).map(s => DenseVector(s(4 to 6).toArray)).toArray

    debug.foreach {
      frame =>
        frame.setLayout(new GridLayout(5,6))
    }

    val models = for (i <- 2 to 30) yield {

      val model = new GMeans(ff, i)

      val c4Predictions = ff.map(model.predict)

      val clusters = c4Predictions.zip(pp).groupBy(_._1).map {
        case (i, g) =>
          DenseVector(g.map(_._2))
      }.toArray

      val totalPoints = ff.size.toDouble

      val scores = for (sPoints <- clusters) yield {
        if (sPoints.length > 250) {
          val plane = fitPlane(sPoints)
          val planeTotalPoints = sPoints.length.toDouble
          val goodPlanePoints = sPoints.valuesIterator.foldLeft(0.0)((s, p) => s + (if (distance(plane, p) > 3.0) 0 else 1))
          (planeTotalPoints / totalPoints) * (goodPlanePoints / planeTotalPoints)
        } else {
          0.0
        }
      }

      val score = scores.sum

      plot(clusters, debug, s"$i: $score")

      score -> model
    }

    val bestModel = models.maxBy(_._1)._2

    val c4Predictions = ff.map(bestModel.predict)

    val clusters = c4Predictions.zip(pp).groupBy(_._1).map {
      case (i, g) =>
        DenseVector(g.map(_._2))
    }.toArray

    for ((c, i) <- clusters.zipWithIndex) {
      c.foreach {
        v => output.update(v(0).toInt, v(1).toInt, i + 1)
      }
    }

    output

  }

  def distance(plane: DenseVector[Double], point: DenseVector[Double]) = {
    math.abs(plane(0) * point(0) + plane(1) * point(1) + plane(2) * point(2) + plane(3)) / math.sqrt(sum(pow(plane(0 to 2), 2)))
  }

  private def fitPlane(points: DenseVector[DenseVector[Double]]): DenseVector[Double] = {
    val p = points.valuesIterator.map(_(0)) ++ points.valuesIterator.map(_(1)) ++ points.valuesIterator.map(_(2))
    val m = new DenseMatrix(points.size, 3, p.toArray)
    fitPlane(m)
  }

  private def fitPlane(points: DenseMatrix[Double]): DenseVector[Double] = {
    val pca = princomp(points)
    val mean = pca.center
    val norm = {
      val norm = pca.loadings(2, ::).inner
      if(norm(2) < 0) -norm
      else norm
    }
    val d = - norm(0) * mean(0) - norm(1) * mean(1) - norm(2) * mean(2)
    DenseVector(norm(0), norm(1), norm(2), d)
  }

  private def localPlanes(k: Int, intensities: DenseMatrix[Double], filter: DenseMatrix[Boolean]): DenseMatrix[DenseVector[Double]] = {

    val tree = KDTree.fromSeq {

      intensities.iterator.filter(_._2 > 0).filter(v => filter(v._1._1, v._1._2)).map {
        case ((x, y), _) => (x, y)
      } toSeq

    }

    intensities.mapPairs {
      case ((x, y), v) if v > 0 && filter(x, y) =>
          val nearest = tree.findNearest((x, y), k)
          val n = nearest.map(_._1.toDouble) ++ nearest.map(_._2.toDouble) ++ nearest.map(v => intensities(v._1.toInt, v._2.toInt))
          val nn = new DenseMatrix(nearest.size, 3, n.toArray)
          val pca = princomp(nn)
          val mean = pca.center
          val norm = {
            val norm = pca.loadings(2, ::).inner
            if(norm(2) < 0) -norm
            else norm
          }
          val curvature = pca.eigenvalues(2) / sum(pca.eigenvalues)
          val d = - norm(0) * mean(0) - norm(1) * mean(1) - norm(2) * mean(2)
          DenseVector(norm(0), norm(1), norm(2),  d, x.toDouble, y.toDouble, intensities(x, y), curvature)
      case _ => DenseVector.zeros[Double](0)
    }

  }

  private def plot(clusters: Array[DenseVector[DenseVector[Double]]], debug: Option[JComponent], title: String): Unit = {

    import smile.plot.ScatterPlot

    debug.map {
      frame =>
        val maxX = clusters.flatMap(_.valuesIterator.map(_(0))).max.toInt
        val maxY = clusters.flatMap(_.valuesIterator.map(_(1))).max.toInt

        val points = clusters.flatMap(_.valuesIterator.map(v => Array(v(0), v(1))))
        val labels = clusters.zipWithIndex.flatMap(v => v._1.valuesIterator.map(_ => v._2))

        val canvas = ScatterPlot.plot(points, labels, '.', Palette.COLORS)
        canvas.setSize(new Dimension(maxX, maxY))
        canvas.setMinimumSize(new Dimension(maxX, maxY))
        canvas.setTitle(title)
        canvas.setVisible(true)
        canvas.setAxisLabels("", "")
        canvas.setTitleFont(new Font("Default", Font.PLAIN, 10))

        frame.add(canvas)

    }

  }

}
