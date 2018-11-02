import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class RunRecommender:

	def main(self):
		spark = SparkSession.builder().getOrCreate()
		# Optional, but may help avoid errors due to long lineage
		self.spark.sparkContext.setCheckpointDir("hdfs://tmp/")

		base = "hdfs://user/ds/"
		rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
		rawArtistData = spark.read.textFile(base + "artist_data.txt")
		rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

		runRecommender = RunRecommender(spark)
		runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
		runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
		runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
		runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)
	# end def

# end class

class RunRecommender:

	def __init__(spark):
		self.spark = spark
	# end def

	def preparation(
			rawUserArtistData,
			rawArtistData,
			rawArtistAlias):

		rawUserArtistData.take(5).foreach(print)

		userArtistDF = rawUserArtistData.map { line :
			Array(user, artist, _*) = line.split(' ')
			(user.toInt, artist.toInt)
		# end def.toDF("user", "artist")

		userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

		artistByID = buildArtistByID(rawArtistData)
		artistAlias = buildArtistAlias(rawArtistAlias)

		(badID, goodID) = artistAlias.head
		artistByID.filter($"id" isin (badID, goodID)).show()
	# end def

	def model(rawUserArtistData, rawArtistData, rawArtistAlias):

		bArtistAlias = self.spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

		trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()

		model = ALS().
			setSeed(Random.nextLong()).
			setImplicitPrefs(true).
			setRank(10).
			setRegParam(0.01).
			setAlpha(1.0).
			setMaxIter(5).
			setUserCol("user").
			setItemCol("artist").
			setRatingCol("count").
			setPredictionCol("prediction").
			fit(trainData)

		trainData.unpersist()

		model.userFactors.select("features").show(truncate = false)

		userID = 2093760

		existingArtistIDs = trainData.
			filter($"user" === userID).
			select("artist").as[Int].collect()

		artistByID = buildArtistByID(rawArtistData)

		artistByID.filter($"id" isin (existingArtistIDs:_*)).show()

		topRecommendations = makeRecommendations(model, userID, 5)
		topRecommendations.show()

		recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()

		artistByID.filter($"id" isin (recommendedArtistIDs:_*)).show()

		model.userFactors.unpersist()
		model.itemFactors.unpersist()
	# end def

	def evaluate(
			rawUserArtistData,
			rawArtistAlias):

		bArtistAlias = self.spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

		allData = buildCounts(rawUserArtistData, bArtistAlias)
		Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		cvData.cache()

		allArtistIDs = allData.select("artist").as[Int].distinct().collect()
		bAllArtistIDs = self.spark.sparkContext.broadcast(allArtistIDs)

		mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
		print(mostListenedAUC)

		evaluations =
			for (rank		 <- Seq(5,	30);
					 regParam <- Seq(1.0, 0.0001);
					 alpha		<- Seq(1.0, 40.0))
			yield {
				model = ALS().
					setSeed(Random.nextLong()).
					setImplicitPrefs(true).
					setRank(rank).setRegParam(regParam).
					setAlpha(alpha).setMaxIter(20).
					setUserCol("user").setItemCol("artist").
					setRatingCol("count").setPredictionCol("prediction").
					fit(trainData)

				auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)

				model.userFactors.unpersist()
				model.itemFactors.unpersist()

				(auc, (rank, regParam, alpha))
			# end def

		evaluations.sorted.reverse.foreach(print)

		trainData.unpersist()
		cvData.unpersist()
	# end def

	def recommend(
			rawUserArtistData,
			rawArtistData,
			rawArtistAlias):

		bArtistAlias = self.spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
		allData = buildCounts(rawUserArtistData, bArtistAlias).cache()
		model = ALS().
			setSeed(Random.nextLong()).
			setImplicitPrefs(true).
			setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(20).
			setUserCol("user").setItemCol("artist").
			setRatingCol("count").setPredictionCol("prediction").
			fit(allData)
		allData.unpersist()

		userID = 2093760
		topRecommendations = makeRecommendations(model, userID, 5)

		recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
		artistByID = buildArtistByID(rawArtistData)
		artistByID.join(self.spark.createDataset(recommendedArtistIDs).toDF("id"), "id").
			select("name").show()

		model.userFactors.unpersist()
		model.itemFactors.unpersist()
	# end def

	def buildArtistByID(rawArtistData):
		rawArtistData.flatMap { line :
			(id, name) = line.span(_ != '\t')
			if (name.isEmpty) {
				None
			# end def else {
				try {
					Some((id.toInt, name.trim))
				# end def catch {
					case _: NumberFormatException : None
				# end def
			# end def
		# end def.toDF("id", "name")
	# end def

	def buildArtistAlias(rawArtistAlias): Map[Int,Int]:
		rawArtistAlias.flatMap { line :
			Array(artist, alias) = line.split('\t')
			if (artist.isEmpty) {
				None
			# end def else {
				Some((artist.toInt, alias.toInt))
			# end def
		# end def.collect().toMap
	# end def

	def buildCounts(
			rawUserArtistData,
			bArtistAlias: Broadcast[Map[Int,Int]]):
		rawUserArtistData.map { line :
			Array(userID, artistID, count) = line.split(' ').map(_.toInt)
			finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
			(userID, finalArtistID, count)
		# end def.toDF("user", "artist", "count")
	# end def

	def makeRecommendations(model: ALSModel, userID, howMany):
		toRecommend = model.itemFactors.
			select($"id".as("artist")).
			withColumn("user", lit(userID))
		model.transform(toRecommend).
			select("artist", "prediction").
			orderBy($"prediction".desc).
			limit(howMany)
	# end def

	def areaUnderCurve(
			positiveData,
			bAllArtistIDs: Broadcast[Array[Int]],
			predictFunction: (DataFrame : DataFrame)):

		# What this actually computes is AUC, per user. The result is actually something
		# that might be called "mean AUC".

		# Take held-out data as the "positive".
		# Make predictions for each of them, including a numeric score
		positivePredictions = predictFunction(positiveData.select("user", "artist")).
			withColumnRenamed("prediction", "positivePrediction")

		# BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
		# small AUC problems, and it would be inefficient, when a direct computation is available.

		# Create a set of "negative" products for each user. These are randomly chosen
		# from among all of the other artists, excluding those that are "positive" for the user.
		negativeData = positiveData.select("user", "artist").as[(Int,Int)].
			groupByKey { case (user, _) : user # end def.
			flatMapGroups { case (userID, userIDAndPosArtistIDs) :
				random = Random()
				posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) : artist # end def.toSet
				negative = ArrayBuffer[Int]()
				allArtistIDs = bAllArtistIDs.value
				var i = 0
				# Make at most one pass over all artists to avoid an infinite loop.
				# Also stop when number of negative equals positive set size
				while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
					artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
					# Only add distinct IDs
					if (!posItemIDSet.contains(artistID)) {
						negative += artistID
					# end def
					i += 1
				# end def
				# Return the set with user ID added back
				negative.map(artistID : (userID, artistID))
			# end def.toDF("user", "artist")

		# Make predictions on the rest:
		negativePredictions = predictFunction(negativeData).
			withColumnRenamed("prediction", "negativePrediction")

		# Join positive predictions to negative predictions by user, only.
		# This will result in a row for every possible pairing of positive and negative
		# predictions within each user.
		joinedPredictions = positivePredictions.join(negativePredictions, "user").
			select("user", "positivePrediction", "negativePrediction").cache()

		# Count the number of pairs per user
		allCounts = joinedPredictions.
			groupBy("user").agg(count(lit("1")).as("total")).
			select("user", "total")
		# Count the number of correctly ordered pairs per user
		correctCounts = joinedPredictions.
			filter($"positivePrediction" > $"negativePrediction").
			groupBy("user").agg(count("user").as("correct")).
			select("user", "correct")

		# Combine these, compute their ratio, and average over all users
		meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
			select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
			agg(mean("auc")).
			as[Double].first()

		joinedPredictions.unpersist()

		meanAUC
	# end def

	def predictMostListened(train)(allData):
		listenCounts = train.groupBy("artist").
			agg(sum("count").as("prediction")).
			select("artist", "prediction")
		allData.
			join(listenCounts, Seq("artist"), "left_outer").
			select("user", "artist", "prediction")
	# end def

# end class

if __name__ == '__main__':
	app = RunRecommender()
	app.main()
# end if
