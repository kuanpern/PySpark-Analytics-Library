
class RunRDF:

	def main(self):
		spark = SparkSession.builder().getOrCreate()

		dataWithoutHeader = spark.read.
			option("inferSchema", true).
			option("header", false).
			csv("hdfs://user/ds/covtype.data")

		colNames = Seq(
				"Elevation", "Aspect", "Slope",
				"Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
				"Horizontal_Distance_To_Roadways",
				"Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
				"Horizontal_Distance_To_Fire_Points"
			) ++ (
				(0 until 4).map(i : s"Wilderness_Area_$i")
			) ++ (
				(0 until 40).map(i : s"Soil_Type_$i")
			) ++ Seq("Cover_Type")

		data = dataWithoutHeader.toDF(colNames:_*).
			withColumn("Cover_Type", $"Cover_Type".cast("double"))

		data.show()
		data.head

		# Split into 90% train (+ CV), 10% test
		Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		testData.cache()

		runRDF = RunRDF(spark)

		runRDF.simpleDecisionTree(trainData, testData)
		runRDF.randomClassifier(trainData, testData)
		runRDF.evaluate(trainData, testData)
		runRDF.evaluateCategorical(trainData, testData)
		runRDF.evaluateForest(trainData, testData)

		trainData.unpersist()
		testData.unpersist()
	# end def

# end class

class RunRDF:

	def __init__(self):
		self.spark = spark
	# end def

	def simpleDecisionTree(trainData, testData):

		inputCols = trainData.columns.filter(_ != "Cover_Type")
		assembler = VectorAssembler().
			setInputCols(inputCols).
			setOutputCol("featureVector")

		assembledTrainData = assembler.transform(trainData)
		assembledTrainData.select("featureVector").show(truncate = false)

		classifier = DecisionTreeClassifier().
			setSeed(Random.nextLong()).
			setLabelCol("Cover_Type").
			setFeaturesCol("featureVector").
			setPredictionCol("prediction")

		model = classifier.fit(assembledTrainData)
		print(model.toDebugString)

		model.featureImportances.toArray.zip(inputCols).
			sorted.reverse.foreach(print)

		predictions = model.transform(assembledTrainData)

		predictions.select("Cover_Type", "prediction", "probability").
			show(truncate = false)

		evaluator = MulticlassClassificationEvaluator().
			setLabelCol("Cover_Type").
			setPredictionCol("prediction")

		accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
		f1 = evaluator.setMetricName("f1").evaluate(predictions)
		print(accuracy)
		print(f1)

		predictionRDD = predictions.
			select("prediction", "Cover_Type").
			as[(Double,Double)].rdd
		multiclassMetrics = MulticlassMetrics(predictionRDD)
		print(multiclassMetrics.confusionMatrix)

		confusionMatrix = predictions.
			groupBy("Cover_Type").
			pivot("prediction", (1 to 7)).
			count().
			na.fill(0.0).
			orderBy("Cover_Type")

		confusionMatrix.show()
	# end def

	def classProbabilities(data): Array[Double]:
		total = data.count()
		data.groupBy("Cover_Type").count().
			orderBy("Cover_Type").
			select("count").as[Double].
			map(_ / total).
			collect()
	# end def

	def randomClassifier(trainData, testData):
		trainPriorProbabilities = classProbabilities(trainData)
		testPriorProbabilities = classProbabilities(testData)
		accuracy = trainPriorProbabilities.zip(testPriorProbabilities).map {
			case (trainProb, cvProb) : trainProb * cvProb
		# end def.sum
		print(accuracy)
	# end def

	def evaluate(trainData, testData):

		inputCols = trainData.columns.filter(_ != "Cover_Type")
		assembler = VectorAssembler().
			setInputCols(inputCols).
			setOutputCol("featureVector")

		classifier = DecisionTreeClassifier().
			setSeed(Random.nextLong()).
			setLabelCol("Cover_Type").
			setFeaturesCol("featureVector").
			setPredictionCol("prediction")

		pipeline = Pipeline().setStages(Array(assembler, classifier))

		paramGrid = ParamGridBuilder().
			addGrid(classifier.impurity, Seq("gini", "entropy")).
			addGrid(classifier.maxDepth, Seq(1, 20)).
			addGrid(classifier.maxBins, Seq(40, 300)).
			addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
			build()

		multiclassE= MulticlassClassificationEvaluator().
			setLabelCol("Cover_Type").
			setPredictionCol("prediction").
			setMetricName("accuracy")

		validator = TrainValidationSplit().
			setSeed(Random.nextLong()).
			setEstimator(pipeline).
			setEvaluator(multiclassEval).
			setEstimatorParamMaps(paramGrid).
			setTrainRatio(0.9)

		validatorModel = validator.fit(trainData)

		paramsAndMetrics = validatorModel.validationMetrics.
			zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

		paramsAndMetrics.foreach { case (metric, params) :
				print(metric)
				print(params)
				print()
		# end def

		bestModel = validatorModel.bestModel

		print(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

		print(validatorModel.validationMetrics.max)

		testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
		print(testAccuracy)

		trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
		print(trainAccuracy)
	# end def

	def unencodeOneHot(data):
		wildernessCols = (0 until 4).map(i : s"Wilderness_Area_$i").toArray

		wildernessAssembler = VectorAssembler().
			setInputCols(wildernessCols).
			setOutputCol("wilderness")

		unhotUDF = udf((vec: Vector) : vec.toArray.indexOf(1.0).toDouble)

		withWilderness = wildernessAssembler.transform(data).
			drop(wildernessCols:_*).
			withColumn("wilderness", unhotUDF($"wilderness"))

		soilCols = (0 until 40).map(i : s"Soil_Type_$i").toArray

		soilAssembler = VectorAssembler().
			setInputCols(soilCols).
			setOutputCol("soil")

		soilAssembler.transform(withWilderness).
			drop(soilCols:_*).
			withColumn("soil", unhotUDF($"soil"))
	# end def

	def evaluateCategorical(trainData, testData):
		unencTrainData = unencodeOneHot(trainData)
		unencTestData = unencodeOneHot(testData)

		inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
		assembler = VectorAssembler().
			setInputCols(inputCols).
			setOutputCol("featureVector")

		indexer = VectorIndexer().
			setMaxCategories(40).
			setInputCol("featureVector").
			setOutputCol("indexedVector")

		classifier = DecisionTreeClassifier().
			setSeed(Random.nextLong()).
			setLabelCol("Cover_Type").
			setFeaturesCol("indexedVector").
			setPredictionCol("prediction")

		pipeline = Pipeline().setStages(Array(assembler, indexer, classifier))

		paramGrid = ParamGridBuilder().
			addGrid(classifier.impurity, Seq("gini", "entropy")).
			addGrid(classifier.maxDepth, Seq(1, 20)).
			addGrid(classifier.maxBins, Seq(40, 300)).
			addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
			build()

		multiclassE= MulticlassClassificationEvaluator().
			setLabelCol("Cover_Type").
			setPredictionCol("prediction").
			setMetricName("accuracy")

		validator = TrainValidationSplit().
			setSeed(Random.nextLong()).
			setEstimator(pipeline).
			setEvaluator(multiclassEval).
			setEstimatorParamMaps(paramGrid).
			setTrainRatio(0.9)

		validatorModel = validator.fit(unencTrainData)

		bestModel = validatorModel.bestModel

		print(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

		testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
		print(testAccuracy)
	# end def

	def evaluateForest(trainData, testData):
		unencTrainData = unencodeOneHot(trainData)
		unencTestData = unencodeOneHot(testData)

		inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
		assembler = VectorAssembler().
			setInputCols(inputCols).
			setOutputCol("featureVector")

		indexer = VectorIndexer().
			setMaxCategories(40).
			setInputCol("featureVector").
			setOutputCol("indexedVector")

		classifier = RandomForestClassifier().
			setSeed(Random.nextLong()).
			setLabelCol("Cover_Type").
			setFeaturesCol("indexedVector").
			setPredictionCol("prediction").
			setImpurity("entropy").
			setMaxDepth(20).
			setMaxBins(300)

		pipeline = Pipeline().setStages(Array(assembler, indexer, classifier))

		paramGrid = ParamGridBuilder().
			addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
			addGrid(classifier.numTrees, Seq(1, 10)).
			build()

		multiclassE= MulticlassClassificationEvaluator().
			setLabelCol("Cover_Type").
			setPredictionCol("prediction").
			setMetricName("accuracy")

		validator = TrainValidationSplit().
			setSeed(Random.nextLong()).
			setEstimator(pipeline).
			setEvaluator(multiclassEval).
			setEstimatorParamMaps(paramGrid).
			setTrainRatio(0.9)

		validatorModel = validator.fit(unencTrainData)

		bestModel = validatorModel.bestModel

		forestModel = bestModel.asInstanceOf[PipelineModel].
			stages.last.asInstanceOf[RandomForestClassificationModel]

		print(forestModel.extractParamMap)
		print(forestModel.getNumTrees)
		forestModel.featureImportances.toArray.zip(inputCols).
			sorted.reverse.foreach(print)

		testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
		print(testAccuracy)

		bestModel.transform(unencTestData.drop("Cover_Type")).select("prediction").show()
	# end def

# end class


if __name__ == '__main__':
	app = RunRDF()
	app.main()
# end if
