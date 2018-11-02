import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class RunLSA:
	def main(self):
		k = if (args.length > 0) args(0).toInt else 100
		numTerms = if (args.length > 1) args(1).toInt else 20000

		spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()
		assembleMatrix = AssembleDocumentTermMatrix(spark)

		docTexts: Dataset[(String, String)] = parseWikipediaDump("hdfs://user/ds/Wikipedia/")

		(docTermMatrix, termIds, docIds, termIdfs) = documentTermMatrix(docTexts, "stopwords.txt", numTerms)

		docTermMatrix.cache()

		vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row :
			Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
		# end def

		vecRdd.cache()
		mat = RowMatrix(vecRdd)
		svd = mat.computeSVD(k, computeU=true)

		print("Singular values: " + svd.s)
		topConceptTerms = topTermsInTopConcepts(svd, 10, 10, termIds)
		topConceptDocs = topDocsInTopConcepts(svd, 10, 10, docIds)
		for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
			print("Concept terms: " + terms.map(_._1).mkString(", "))
			print("Concept docs: " + docs.map(_._1).mkString(", "))
			print()
		# end def

		queryEngine = LSAQueryEngine(svd, termIds, docIds, termIdfs)
		queryEngine.printTopTermsForTerm("algorithm")
		queryEngine.printTopTermsForTerm("radiohead")
		queryEngine.printTopTermsForTerm("tarantino")

		queryEngine.printTopDocsForTerm("fir")
		queryEngine.printTopDocsForTerm("graph")

		queryEngine.printTopDocsForDoc("Romania")
		queryEngine.printTopDocsForDoc("Brad Pitt")
		queryEngine.printTopDocsForDoc("Radiohead")

		queryEngine.printTopDocsForTermQuery(Seq("factorization", "decomposition"))
	# end def

	'''
	 * The top concepts are the concepts that explain the most variance in the dataset.
	 * For each top concept, finds the terms that are most relevant to the concept.
	 *
	 * @param svd A singular value decomposition.
	 * @param numConcepts The number of concepts to look at.
	 * @param numTerms The number of terms to look at within each concept.
	 * @param termIds The mapping of term IDs to terms.
	 * @return A Seq of top concepts, in order, each with a Seq of top terms, in order.
	'''
	def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts,
			numTerms, termIds: Array[String]): Seq[Seq[(String, Double)]]:
		v = svd.V
		topTerms = ArrayBuffer[Seq[(String, Double)]]()
		arr = v.toArray
		for (i <- 0 until numConcepts) {
			offs = i * v.numRows
			termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
			sorted = termWeights.sortBy(-_._1)
			topTerms += sorted.take(numTerms).map {case (score, id) : (termIds(id), score) # end def
		# end def
		topTerms
	# end def

	'''
	 * The top concepts are the concepts that explain the most variance in the dataset.
	 * For each top concept, finds the documents that are most relevant to the concept.
	 *
	 * @param svd A singular value decomposition.
	 * @param numConcepts The number of concepts to look at.
	 * @param numDocs The number of documents to look at within each concept.
	 * @param docIds The mapping of document IDs to terms.
	 * @return A Seq of top concepts, in order, each with a Seq of top terms, in order.
	'''
	def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts,
			numDocs, docIds: Map[Long, String]): Seq[Seq[(String, Double)]]:
		u	= svd.U
		topDocs = ArrayBuffer[Seq[(String, Double)]]()
		for (i <- 0 until numConcepts) {
			docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
			topDocs += docWeights.top(numDocs).map { case (score, id) : (docIds(id), score) # end def
		# end def
		topDocs
	# end def
# end def

class LSAQueryEngine:
		svd: SingularValueDecomposition[RowMatrix, Matrix],
		termIds: Array[String],
		docIds: Map[Long, String],
		termIdfs: Array[Double]) {

	VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
	normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
	US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
	normalizedUS: RowMatrix = distributedRowsNormalized(US)

	idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
	idDocs: Map[String, Long] = docIds.map(_.swap)

	'''
	 * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
	 * Breeze doesn't support efficient diagonal representations, so multiply manually.
	'''
	def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double]:
		sArr = diag.toArray
		BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
			.mapPairs { case ((r, c), v) : v * sArr(c) # end def
	# end def

	'''
	 * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
	'''
	def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix:
		sArr = diag.toArray
		RowMatrix(mat.rows.map { vec :
			vecArr = vec.toArray
			newArr = (0 until vec.size).toArray.map(i : vecArr(i) * sArr(i))
			Vectors.dense(newArr)
		# end def)
	# end def

	'''
	 * Returns a matrix where each row is divided by its length.
	'''
	def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double]:
		newMat = BDenseMatrix[Double](mat.rows, mat.cols)
		for (r <- 0 until mat.rows) {
			length = math.sqrt((0 until mat.cols).map(c : mat(r, c) * mat(r, c)).sum)
			(0 until mat.cols).foreach(c : newMat.update(r, c, mat(r, c) / length))
		# end def
		newMat
	# end def

	'''
	 * Returns a distributed matrix where each row is divided by its length.
	'''
	def distributedRowsNormalized(mat: RowMatrix): RowMatrix:
		RowMatrix(mat.rows.map { vec :
			array = vec.toArray
			length = math.sqrt(array.map(x : x * x).sum)
			Vectors.dense(array.map(_ / length))
		# end def)
	# end def

	'''
	 * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
	 * relevance scores to the given term.
	'''
	def topDocsForTerm(termId): Seq[(Double, Long)]:
		rowArr = (0 until svd.V.numCols).map(i : svd.V(termId, i)).toArray
		rowVec = Matrices.dense(rowArr.length, 1, rowArr)

		# Compute scores against every doc
		docScores = US.multiply(rowVec)

		# Find the docs with the highest scores
		allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
		allDocWeights.top(10)
	# end def

	'''
	 * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
	 * relevance scores to the given term.
	'''
	def topTermsForTerm(termId): Seq[(Double, Int)]:
		# Look up the row in VS corresponding to the given term ID.
		rowVec = normalizedVS(termId, :).t

		# Compute scores against every term
		termScores = (normalizedVS * rowVec).toArray.zipWithIndex

		# Find the terms with the highest scores
		termScores.sortBy(-_._1).take(10)
	# end def

	'''
	 * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
	 * relevance scores to the given doc.
	'''
	def topDocsForDoc(docId: Long): Seq[(Double, Long)]:
		# Look up the row in US corresponding to the given doc ID.
		docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
		docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

		# Compute scores against every doc
		docScores = normalizedUS.multiply(docRowVec)

		# Find the docs with the highest scores
		allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

		# Docs can end up with NaN score if their row in U is all zeros.	Filter these out.
		allDocWeights.filter(!_._1.isNaN).top(10)
	# end def

	'''
		* Builds a term query vector from a set of terms.
	 '''
	def termsToQueryVector(terms: Seq[String]): BSparseVector[Double]:
		indices = terms.map(idTerms(_)).toArray
		values = indices.map(termIdfs(_))
		BSparseVector[Double](indices, values, idTerms.size)
	# end def

	'''
		* Finds docs relevant to a term query, represented as a vector with non-zero weights for the
		* terms in the query.
	 '''
	def topDocsForTermQuery(query: BSparseVector[Double]): Seq[(Double, Long)]:
		breezeV = BDenseMatrix[Double](svd.V.numRows, svd.V.numCols, svd.V.toArray)
		termRowArr = (breezeV.t * query).toArray

		termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

		# Compute scores against every doc
		docScores = US.multiply(termRowVec)

		# Find the docs with the highest scores
		allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
		allDocWeights.top(10)
	# end def

	def printTopTermsForTerm(term):
		idWeights = topTermsForTerm(idTerms(term))
		print(idWeights.map { case (score, id) : (termIds(id), score) # end def.mkString(", "))
	# end def

	def printTopDocsForDoc(doc):
		idWeights = topDocsForDoc(idDocs(doc))
		print(idWeights.map { case (score, id) : (docIds(id), score) # end def.mkString(", "))
	# end def

	def printTopDocsForTerm(term):
		idWeights = topDocsForTerm(idTerms(term))
		print(idWeights.map { case (score, id) : (docIds(id), score) # end def.mkString(", "))
	# end def

	def printTopDocsForTermQuery(terms: Seq[String]):
		queryVec = termsToQueryVector(terms)
		idWeights = topDocsForTermQuery(queryVec)
		print(idWeights.map { case (score, id) : (docIds(id), score) # end def.mkString(", "))
	# end def
# end def

if __name__ == '__main__':
	app = RunLSA()
	app.main()
# end if
