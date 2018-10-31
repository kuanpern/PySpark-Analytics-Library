

class RunKMeans {

	def main(self):
		spark = SparkSession.builder().getOrCreate()

		data = spark.read.
			option("inferSchema", true).
			option("header", false).
			csv("hdfs://user/ds/kddcup.data").
			toDF(
				"duration", "protocol_type", "service", "flag",
				"src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
				"hot", "num_failed_logins", "logged_in", "num_compromised",
				"root_shell", "su_attempted", "num_root", "num_file_creations",
				"num_shells", "num_access_files", "num_outbound_cmds",
				"is_host_login", "is_guest_login", "count", "srv_count",
				"serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
				"same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
				"dst_host_count", "dst_host_srv_count",
				"dst_host_same_srv_rate", "dst_host_diff_srv_rate",
				"dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
				"dst_host_serror_rate", "dst_host_srv_serror_rate",
				"dst_host_rerror_rate", "dst_host_srv_rerror_rate",
				"label")

		data.cache()

		runKMeans = RunKMeans(spark)

		runKMeans.clusteringTake0(data)
		runKMeans.clusteringTake1(data)
		runKMeans.clusteringTake2(data)
		runKMeans.clusteringTake3(data)
		runKMeans.clusteringTake4(data)
		runKMeans.buildAnomalyDetector(data)

		data.unpersist()
	# end def

# end def

class RunKMeans(private spark: SparkSession) {


	# Clustering, Take 0

	def clusteringTake0(data):

		data.select("label").groupBy("label").count().orderBy($"count".desc).show(25)

		numericOnly = data.drop("protocol_type", "service", "flag").cache()

		assembler = VectorAssembler().
			setInputCols(numericOnly.columns.filter(_ != "label")).
			setOutputCol("featureVector")

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setPredictionCol("cluster").
			setFeaturesCol("featureVector")

		pipeline = Pipeline().setStages(Array(assembler, kmeans))
		pipelineModel = pipeline.fit(numericOnly)
		kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

		kmeansModel.clusterCenters.foreach(print)

		withCluster = pipelineModel.transform(numericOnly)

		withCluster.select("cluster", "label").
			groupBy("cluster", "label").count().
			orderBy($"cluster", $"count".desc).
			show(25)

		numericOnly.unpersist()
	# end def

	# Clustering, Take 1

	def clusteringScore0(data, k):
		assembler = VectorAssembler().
			setInputCols(data.columns.filter(_ != "label")).
			setOutputCol("featureVector")

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setK(k).
			setPredictionCol("cluster").
			setFeaturesCol("featureVector")

		pipeline = Pipeline().setStages(Array(assembler, kmeans))

		kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
		kmeansModel.computeCost(assembler.transform(data)) / data.count()
	# end def

	def clusteringScore1(data, k):
		assembler = VectorAssembler().
			setInputCols(data.columns.filter(_ != "label")).
			setOutputCol("featureVector")

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setK(k).
			setPredictionCol("cluster").
			setFeaturesCol("featureVector").
			setMaxIter(40).
			setTol(1.0e-5)

		pipeline = Pipeline().setStages(Array(assembler, kmeans))

		kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
		kmeansModel.computeCost(assembler.transform(data)) / data.count()
	# end def

	def clusteringTake1(data):
		numericOnly = data.drop("protocol_type", "service", "flag").cache()
		(20 to 100 by 20).map(k : (k, clusteringScore0(numericOnly, k))).foreach(print)
		(20 to 100 by 20).map(k : (k, clusteringScore1(numericOnly, k))).foreach(print)
		numericOnly.unpersist()
	# end def

	# Clustering, Take 2

	def clusteringScore2(data, k):
		assembler = VectorAssembler().
			setInputCols(data.columns.filter(_ != "label")).
			setOutputCol("featureVector")

		scaler = StandardScaler()
			.setInputCol("featureVector")
			.setOutputCol("scaledFeatureVector")
			.setWithStd(true)
			.setWithMean(false)

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setK(k).
			setPredictionCol("cluster").
			setFeaturesCol("scaledFeatureVector").
			setMaxIter(40).
			setTol(1.0e-5)

		pipeline = Pipeline().setStages(Array(assembler, scaler, kmeans))
		pipelineModel = pipeline.fit(data)

		kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
		kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
	# end def

	def clusteringTake2(data):
		numericOnly = data.drop("protocol_type", "service", "flag").cache()
		(60 to 270 by 30).map(k : (k, clusteringScore2(numericOnly, k))).foreach(print)
		numericOnly.unpersist()
	# end def

	# Clustering, Take 3

	def oneHotPipeline(inputCol): (Pipeline, String):
		indexer = StringIndexer().
			setInputCol(inputCol).
			setOutputCol(inputCol + "_indexed")
		encoder = OneHotEncoder().
			setInputCol(inputCol + "_indexed").
			setOutputCol(inputCol + "_vec")
		pipeline = Pipeline().setStages(Array(indexer, encoder))
		(pipeline, inputCol + "_vec")
	# end def

	def clusteringScore3(data, k):
		(protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
		(serviceEncoder, serviceVecCol) = oneHotPipeline("service")
		(flagEncoder, flagVecCol) = oneHotPipeline("flag")

		# Original columns, without label / string columns, but with vector encoded cols
		assembleCols = Set(data.columns: _*) --
			Seq("label", "protocol_type", "service", "flag") ++
			Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
		assembler = VectorAssembler().
			setInputCols(assembleCols.toArray).
			setOutputCol("featureVector")

		scaler = StandardScaler()
			.setInputCol("featureVector")
			.setOutputCol("scaledFeatureVector")
			.setWithStd(true)
			.setWithMean(false)

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setK(k).
			setPredictionCol("cluster").
			setFeaturesCol("scaledFeatureVector").
			setMaxIter(40).
			setTol(1.0e-5)

		pipeline = Pipeline().setStages(
			Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
		pipelineModel = pipeline.fit(data)

		kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
		kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
	# end def

	def clusteringTake3(data):
		(60 to 270 by 30).map(k : (k, clusteringScore3(data, k))).foreach(print)
	# end def

	# Clustering, Take 4

	def entropy(counts: Iterable[Int]):
		values = counts.filter(_ > 0)
		n = values.map(_.toDouble).sum
		values.map { v :
			p = v / n
			-p * math.log(p)
		# end def.sum
	# end def

	def fitPipeline4(data, k): PipelineModel:
		(protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
		(serviceEncoder, serviceVecCol) = oneHotPipeline("service")
		(flagEncoder, flagVecCol) = oneHotPipeline("flag")

		# Original columns, without label / string columns, but with vector encoded cols
		assembleCols = Set(data.columns: _*) --
			Seq("label", "protocol_type", "service", "flag") ++
			Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
		assembler = VectorAssembler().
			setInputCols(assembleCols.toArray).
			setOutputCol("featureVector")

		scaler = StandardScaler()
			.setInputCol("featureVector")
			.setOutputCol("scaledFeatureVector")
			.setWithStd(true)
			.setWithMean(false)

		kmeans = KMeans().
			setSeed(Random.nextLong()).
			setK(k).
			setPredictionCol("cluster").
			setFeaturesCol("scaledFeatureVector").
			setMaxIter(40).
			setTol(1.0e-5)

		pipeline = Pipeline().setStages(
			Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
		pipeline.fit(data)
	# end def

	def clusteringScore4(data, k):
		pipelineModel = fitPipeline4(data, k)

		# Predict cluster for each datum
		clusterLabel = pipelineModel.transform(data).
			select("cluster", "label").as[(Int, String)]
		weightedClusterEntropy = clusterLabel.
			# Extract collections of labels, per cluster
			groupByKey { case (cluster, _) : cluster # end def.
			mapGroups { case (_, clusterLabels) :
				labels = clusterLabels.map { case (_, label) : label # end def.toSeq
				# Count labels in collections
				labelCounts = labels.groupBy(identity).values.map(_.size)
				labels.size * entropy(labelCounts)
			# end def.collect()

		# Average entropy weighted by cluster size
		weightedClusterEntropy.sum / data.count()
	# end def

	def clusteringTake4(data):
		(60 to 270 by 30).map(k : (k, clusteringScore4(data, k))).foreach(print)

		pipelineModel = fitPipeline4(data, 180)
		countByClusterLabel = pipelineModel.transform(data).
			select("cluster", "label").
			groupBy("cluster", "label").count().
			orderBy("cluster", "label")
		countByClusterLabel.show()
	# end def

	# Detect anomalies

	def buildAnomalyDetector(data):
		pipelineModel = fitPipeline4(data, 180)

		kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
		centroids = kMeansModel.clusterCenters

		clustered = pipelineModel.transform(data)
		threshold = clustered.
			select("cluster", "scaledFeatureVector").as[(Int, Vector)].
			map { case (cluster, vec) : Vectors.sqdist(centroids(cluster), vec) # end def.
			orderBy($"value".desc).take(100).last

		originalCols = data.columns
		anomalies = clustered.filter { row :
			cluster = row.getAs[Int]("cluster")
			vec = row.getAs[Vector]("scaledFeatureVector")
			Vectors.sqdist(centroids(cluster), vec) >= threshold
		# end def.select(originalCols.head, originalCols.tail:_*)

		print(anomalies.first())
	# end def

# end def

if __name__ == '__main__':
	app = RunKMeans()
	app.main()
# end if
