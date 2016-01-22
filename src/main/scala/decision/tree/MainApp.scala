package decision.tree

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by inakov on 16-1-22.
 */
object MainApp {


  def entropy(data: RDD[Vector[String]], targetAttr: Int): Double = {
    def dataEntropy(frequency: Double, dataSize: Long): Double = {
      def log2(x: Double) = scala.math.log(x)/scala.math.log(2)
      ((-frequency/dataSize) * log2(frequency/dataSize))
    }
    val recordsCount = data.count()
    val frequencyCount = data.map(record => (record(targetAttr), 1))
      .reduceByKey((k, v) => k + v).collect()

    frequencyCount.foldLeft(0.0)((e, b) => e + dataEntropy(b._2, recordsCount))
  }

  def gain(data: RDD[Vector[String]], attr: String, targetAttr: String, attributesWithIndex: Map[String, Int]): Double = {
    val attrIndex = attributesWithIndex(attr)
    val targetAttrIndex = attributesWithIndex(targetAttr)

    val frequencyCount = data.map(record => (record(attrIndex), 1))
      .reduceByKey((k, v) => k + v).collect()

    val valuesCount = frequencyCount.foldLeft(0.0)((e, b) => e + b._2)

    val subsetEntropy = frequencyCount.foldLeft(0.0)((se, i) => {
      val valueProb = i._2 / valuesCount
      val dataSubset = data.filter(v=> v(attrIndex) != i._1)

      se + (valueProb * entropy(dataSubset, targetAttrIndex))
    })

    (entropy(data, targetAttrIndex) - subsetEntropy)
  }


  def chooseAttribute(data: RDD[Vector[String]], attributes: List[String], targetAttr: String,
                      attributesWithIndex: Map[String, Int]): String = {
    val attributesWithGain = attributes.filterNot(_ == targetAttr).map(attribute =>
      (attribute, gain(data, attribute, targetAttr, attributesWithIndex)))

    if(attributesWithGain.isEmpty) ""
    else attributesWithGain.maxBy(_._2)._1
  }

  def getUniqueValues(data: RDD[Vector[String]], attribute: String,
                      attributesWithIndex:Map[String, Int]): List[String] = {
    data.map(v => v(attributesWithIndex(attribute))).distinct().collect().toList
  }

  def getExamples(data: RDD[Vector[String]], attrIndex: Int, value: String): RDD[Vector[String]] = {
    data.filter(v => v(attrIndex) == value)
  }

  def createDecisionTree(data: RDD[Vector[String]], attributes: List[String], targetAttr: String,
                         attributesWithIndex: Map[String, Int]): Any = {

    val values = data.map(record => record(attributesWithIndex(targetAttr)))
    val majorityValue = data.map(record => (record(attributesWithIndex(targetAttr)), 1)).reduceByKey((a, b) => a + b)
      .reduce((x, y) => if (x._2 > y._2) x else y)._1

    if(data.isEmpty() || attributes.size-1 <= 0)
      majorityValue
    else if(values.distinct().count() == 1) {
      values.first()
    }else{
      val best: String = chooseAttribute(data, attributes, targetAttr, attributesWithIndex)
      var tree: Map[String, Any] = Map(best -> Map())

      //Create a new decision tree/sub-node for each of the values in the
      //best attribute field
      for(value <- getUniqueValues(data, best, attributesWithIndex)){
        val subtree = createDecisionTree(getExamples(data, attributesWithIndex(best), value),
          attributes.filterNot(_ == best), targetAttr, attributesWithIndex)

        val subtreeMap = tree(best) match {
          case st: Map[String, Any] => st + (value -> subtree)
          case _ => throw new RuntimeException()
        }
        tree += (best -> subtreeMap)
      }

      tree
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("breast-cancer")
    val data = lines.map(line => line.split(","))
      .map(cancerRecord => Vector(cancerRecord(0), cancerRecord(1), cancerRecord(2),
          cancerRecord(3), cancerRecord(4), cancerRecord(5),cancerRecord(6),
          cancerRecord(7), cancerRecord(8),cancerRecord(9)))

    val noHeader = data.mapPartitionsWithIndex(
      (i, iterator) =>
        if (i == 0 && iterator.hasNext) {
          iterator.next
          iterator
        } else iterator).cache()

    val attributesWithIndex = data.take(1)(0).iterator.toList.foldLeft(Map[String, Int]()){
      case (seed,(k,v)) => {
        seed.updated(v,k)
      }
    }

    val attributes = data.take(1)(0).iterator.toList.map(_._2)
    val targetAttr = attributes.last

    val tree = createDecisionTree(noHeader, attributes, targetAttr, attributesWithIndex).asInstanceOf[Map[String, Any]]

    println(tree.prettyPrint)
  }

  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "\n" + valuesString + "\n"
    }

    def toStringLines = {
      map
        .flatMap{ case (k, v) => keyValueToString(k, v)}
        .map(indentLine(_))
    }

    def keyValueToString(key: K, value: V): Iterable[String] = {
      value match {
        case v: Map[_, _] => Iterable(key + " -> ") ++ v.prettyPrint.toStringLines
        case x => Iterable(key + " -> " + x.toString)
      }
    }

    def indentLine(line: String): String = {
      "\t" + line
    }
  }

}
