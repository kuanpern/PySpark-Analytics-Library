import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class RunGraph:

	def main(self):
		spark = SparkSession.builder().getOrCreate()

		medlineRaw: Dataset[String] = loadMedline(spark, "hdfs://user/ds/medline")
		medline: Dataset[Seq[String]] = medlineRaw.map(majorTopics).cache()

		topics = medline.flatMap(mesh : mesh).toDF("topic")
		topics.createOrReplaceTempView("topics")
		topicDist = spark.sql("SELECT topic, COUNT(*) cnt FROM topics GROUP BY topic ORDER BY cnt DESC")
		topicDist.show()
		topicDist.createOrReplaceTempView("topic_dist")
		spark.sql("SELECT cnt, COUNT(*) dist FROM topic_dist GROUP BY cnt ORDER BY dist DESC LIMIT 10").show()

		topicPairs = medline.flatMap(_.sorted.combinations(2)).toDF("pairs")
		topicPairs.createOrReplaceTempView("topic_pairs")
		cooccurs = spark.sql("SELECT pairs, COUNT(*) cnt FROM topic_pairs GROUP BY pairs")
		cooccurs.cache()

		cooccurs.createOrReplaceTempView("cooccurs")
		print("Number of unique co-occurrence pairs: " + cooccurs.count())
		spark.sql("SELECT pairs, cnt FROM cooccurs ORDER BY cnt DESC LIMIT 10").show()

		vertices = topics.map { case Row(topic) : (hashId(topic), topic) # end def.toDF("hash", "topic")
		edges = cooccurs.map { case Row(topics: Seq[_], cnt: Long) :
			 ids = topics.map(_.toString).map(hashId).sorted
			 Edge(ids(0), ids(1), cnt)
		# end def
		vertexRDD = vertices.rdd.map{ case Row(hash: Long, topic) : (hash, topic) # end def
		topicGraph = Graph(vertexRDD, edges.rdd)
		topicGraph.cache()

		connectedComponentGraph = topicGraph.connectedComponents()
		componentDF = connectedComponentGraph.vertices.toDF("vid", "cid")
		componentCounts = componentDF.groupBy("cid").count()
		componentCounts.orderBy(desc("count")).show()

		topicComponentDF = topicGraph.vertices.innerJoin(
			connectedComponentGraph.vertices) {
			(topicId, name, componentId) : (name, componentId.toLong)
		# end def.values.toDF("topic", "cid")
		topicComponentDF.where("cid = -2062883918534425492").show()

		campy = spark.sql("SELECT * FROM topic_dist WHERE topic LIKE '%ampylobacter%'")
		campy.show()

		degrees: VertexRDD[Int] = topicGraph.degrees.cache()
		degrees.map(_._2).stats()
		degrees.innerJoin(topicGraph.vertices) {
			(topicId, degree, name) : (name, degree.toInt)
		# end def.values.toDF("topic", "degree").orderBy(desc("degree")).show()

		T = medline.count()
		topicDistRdd = topicDist.map { case Row(topic, cnt: Long) : (hashId(topic), cnt) # end def.rdd
		topicDistGraph = Graph(topicDistRdd, topicGraph.edges)
		chiSquaredGraph = topicDistGraph.mapTriplets(triplet :
			chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
		)
		chiSquaredGraph.edges.map(x : x.attr).stats()

		interesting = chiSquaredGraph.subgraph(triplet : triplet.attr > 19.5)
		interestingComponentGraph = interesting.connectedComponents()
		icDF = interestingComponentGraph.vertices.toDF("vid", "cid")
		icCountDF = icDF.groupBy("cid").count()
		icCountDF.count()
		icCountDF.orderBy(desc("count")).show()

		interestingDegrees = interesting.degrees.cache()
		interestingDegrees.map(_._2).stats()
		interestingDegrees.innerJoin(topicGraph.vertices) {
			(topicId, degree, name) : (name, degree)
		# end def.toDF("topic", "degree").orderBy(desc("degree")).show()

		avgCC = avgClusteringCoef(interesting)

		paths = samplePathLengths(interesting)
		paths.map(_._3).filter(_ > 0).stats()

		hist = paths.map(_._3).countByValue()
		hist.toSeq.sorted.foreach(print)
	# end def

	def avgClusteringCoef(graph: Graph[_, _]):
		triCountGraph = graph.triangleCount()
		maxTrisGraph = graph.degrees.mapValues(d : d * (d - 1) / 2.0)
		clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
			(vertexId, triCount, maxTris) : if (maxTris == 0) 0 else triCount / maxTris
		# end def
		clusterCoefGraph.map(_._2).sum() / graph.vertices.count()
	# end def

	def samplePathLengths[V, E](graph: Graph[V, E], fraction = 0.02)
		: RDD[(VertexId, VertexId, Int)]:
		replacement = false
		sample = graph.vertices.map(v : v._1).sample(
			replacement, fraction, 1729L)
		ids = sample.collect().toSet

		mapGraph = graph.mapVertices((id, v) : {
			if (ids.contains(id)) {
				Map(id -> 0)
			# end def else {
				Map[VertexId, Int]()
			# end def
		# end def)

		start = Map[VertexId, Int]()
		res = mapGraph.ops.pregel(start)(update, iterate, mergeMaps)
		res.vertices.flatMap { case (id, m) :
			m.map { case (k, v) :
				if (id < k) {
					(id, k, v)
				# end def else {
					(k, id, v)
				# end def
			# end def
		# end def.distinct().cache()
	# end def

	def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int]:
		def minThatExists(k: VertexId):
			math.min(
				m1.getOrElse(k, Int.MaxValue),
				m2.getOrElse(k, Int.MaxValue))
		# end def

		(m1.keySet ++ m2.keySet).map(k : (k, minThatExists(k))).toMap
	# end def

	def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int])
		: Map[VertexId, Int]:
		mergeMaps(state, msg)
	# end def

	def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
		: Iterator[(VertexId, Map[VertexId, Int])]:
		aplus = a.map { case (v, d) : v -> (d + 1) # end def
		if (b != mergeMaps(aplus, b)) {
			Iterator((bid, aplus))
		# end def else {
			Iterator.empty
		# end def
	# end def

	def iterate(e: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])]:
		checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
		checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
	# end def

	def loadMedline(spark: SparkSession, path): Dataset[String]:
		conf = Configuration()
		conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
		conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")
		sc = spark.sparkContext
		in = sc.newAPIHadoopFile(path, classOf[XMLInputFormat],
			classOf[LongWritable], classOf[Text], conf)
		in.map(line : line._2.toString).toDS()
	# end def

	def majorTopics(record): Seq[String]:
		elem = XML.loadString(record)
		dn = elem \\ "DescriptorName"
		mt = dn.filter(n : (n \ "@MajorTopicYN").text == "Y")
		mt.map(n : n.text)
	# end def

	def hashId(): Long:
		# This is effectively the same implementation as in Guava's Hashing, but 'inlined'
		# to avoid a dependency on Guava just for this. It creates a long from the first 8 bytes
		# of the (16 byte) MD5 hash, with first byte as least-significant byte in the long.
		bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
		(bytes(0) & 0xFFL) |
		((bytes(1) & 0xFFL) << 8) |
		((bytes(2) & 0xFFL) << 16) |
		((bytes(3) & 0xFFL) << 24) |
		((bytes(4) & 0xFFL) << 32) |
		((bytes(5) & 0xFFL) << 40) |
		((bytes(6) & 0xFFL) << 48) |
		((bytes(7) & 0xFFL) << 56)
	# end def

	def chiSq(YY: Long, YB: Long, YA: Long, T: Long):
		NB = T - YB
		NA = T - YA
		YN = YA - YY
		NY = YB - YY
		NN = T - NY - YN - YY
		inner = math.abs(YY * NN - YN * NY) - T / 2.0
		T * math.pow(inner, 2) / (YA * NA * YB * NB)
	# end def
# end def

if __name__ == '__main__':
	app = RunGraph()
	app.main()
# end if
