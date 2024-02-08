import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

object LinearRegressionWithSimpleData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithSimpleData")
      .config("spark.master", "local")
      .getOrCreate()

    // initial Data
    val df = spark.createDataFrame(Seq(
      (0, 59),
      (0, 56),
      (0, 54),
      (0, 62),
      (0, 61),
      (0, 53),
      (0, 55),
      (0, 62),
      (0, 64),
      (1, 73),
      (1, 78),
      (1, 67),
      (1, 68),
      (1, 78)
    )).toDF("fail", "temperature")

    // Additional Data
    val df2 = spark.createDataFrame(Seq(
      (1,64),
      (1,62),
      (0,73)
    )).toDF("fail", "temperature")

    // Defining features
    val features = new VectorAssembler()
      .setInputCols(Array("temperature"))
      .setOutputCol("features")

    // Define model to use
    val lr = new LinearRegression().setLabelCol("fail")

    // Create a pipeline that associates the model with the data processing sequence
    val pipeline = new Pipeline().setStages(Array(features, lr))

    // Run the Model
    val model = pipeline.fit(df)

    var linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]

    println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linRegModel.summary.r2}")
    println(s"Model: Y = ${linRegModel.coefficients(0)} * X + ${linRegModel.intercept}")

    linRegModel.summary.residuals.show()
    var result = model.transform(df).select("temperature", "fail", "prediction")
    result.show()

    // After Additional Data
    var model2 = pipeline.fit(df2)

    linRegModel = model2.stages(1).asInstanceOf[LinearRegressionModel]

    println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linRegModel.summary.r2}")
    println(s"Model: Y = ${linRegModel.coefficients(0)} * X + ${linRegModel.intercept}")

    linRegModel.summary.residuals.show()
    result = model2.transform(df).select("temperature", "fail", "prediction")
    result.show()
  }
}