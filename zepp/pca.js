//import matrix
val data = sc.textFile("/Users/sureshkumarmukhiya/Downloads/spark-1.5.0-bin-hadoop2.6/user2.txt",4).cache


/**
 * Create vector
 */
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.RowMatrix

case class Log(a:String, b:Double, c:Double, d:Double, e:Double)

def process(str:String) = str.replaceAll("\\[", "").replaceAll("\\]","")

val inputData = data.map(s => Vectors.dense(s.split(',').map(r =>
    process(r).toDouble)))
    
inputData.setName("Source Vector")


/**
 * initial stat
 */

val testMatrix = new RowMatrix(inputData)
val matrixSummary1 =  testMatrix.computeColumnSummaryStatistics()

println("Log factors mean: " + matrixSummary1.mean)
println("Log factors variance: " + matrixSummary1.variance)


/**
 * Transform
 */

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler

inputData.cache

val scaler = new StandardScaler (withMean = true, withStd = true).fit(inputData)
   
val scaledVectors = inputData.map(v => scaler.transform(v))


/**
 * Create Row Matrix
 */
val matrix = new RowMatrix(scaledVectors)

matrix.numRows()


/**
 * New stat
 */

val matrixSummary =  matrix.computeColumnSummaryStatistics()

println("Log factors mean: " + matrixSummary.mean)
println("Log factors variance: " + matrixSummary.variance)

println(matrixSummary.min)
println(matrixSummary.max)
println(matrixSummary.numNonzeros)


/**
 * Calculate PCA
 */
val K =2
val pc = matrix.computePrincipalComponents(K)

val projected: RowMatrix = matrix.multiply(pc)

case class Result(a:Double, b:Double)
val pro= projected.rows.map{
    row => Result(row(0), row(1))
}


val result = pro.toDF.cache
result.registerTempTable("tpca")


%sql 
select a, b from tpca