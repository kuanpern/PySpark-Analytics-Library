import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class AssembleDocumentTermMatrix(private spark: SparkSession):

	'''
	 * Returns a (title, content) pair.
	'''
	def wikiXmlToPlainText(pageXml): Option[(String, String)]:
		page = EnglishWikipediaPage()

		# Wikipedia has updated their dumps slightly since Cloud9 was written, so this hacky replacement is sometimes
		# required to get parsing to work.
		hackedPageXml = pageXml.replaceFirst(
			"<text xml:space=\"preserve\" bytes=\"\\d+\">", "<text xml:space=\"preserve\">")

		WikipediaPage.readPage(page, hackedPageXml)
		if (page.isEmpty || !page.isArticle || page.isRedirect || page.isDisambiguation ||
				page.getTitle.contains("(disambiguation)")) {
			None
		# end def else {
			Some((page.getTitle, page.getContent))
		# end def
	# end def

	def parseWikipediaDump(path): Dataset[(String, String)]:
		conf = Configuration()
		conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
		conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
		kvs = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable],
			classOf[Text], conf)
		rawXmls = kvs.map(_._2.toString).toDS()

		rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)
	# end def

	'''
	 * Create a StanfordCoreNLP pipeline class to lemmatize documents.
	'''
	def createNLPPipeline(): StanfordCoreNLP:
		props = Properties()
		props.put("annotators", "tokenize, ssplit, pos, lemma")
		StanfordCoreNLP(props)
	# end def

	def isOnlyLetters(): Boolean:
		str.forall(c : Character.isLetter(c))
	# end def

	def plainTextToLemmas(text, stopWords: Set[String], pipeline: StanfordCoreNLP)
		: Seq[String]:
		doc = Annotation(text)
		pipeline.annotate(doc)
		lemmas = ArrayBuffer[String]()
		sentences = doc.get(classOf[SentencesAnnotation])
		for (sentence <- sentences.asScala;
				 token <- sentence.get(classOf[TokensAnnotation]).asScala) {
			lemma = token.get(classOf[LemmaAnnotation])
			if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
				lemmas += lemma.toLowerCase
			# end def
		# end def
		lemmas
	# end def

	def contentsToTerms(docs: Dataset[(String, String)], stopWordsFile): Dataset[(String, Seq[String])]:
		stopWords = scala.io.Source.fromFile(stopWordsFile).getLines().toSet
		bStopWords = spark.sparkContext.broadcast(stopWords)

		docs.mapPartitions { iter :
			pipeline = createNLPPipeline()
			iter.map { case (title, contents) : (title, plainTextToLemmas(contents, bStopWords.value, pipeline)) # end def
		# end def
	# end def

	def loadStopWords(path): Set[String]:
		scala.io.Source.fromFile(path).getLines().toSet
	# end def

	'''
	 * Returns a document-term matrix where each element is the TF-IDF of the row's document and
	 * the column's term.
	 *
	 * @param docTexts a DF with two columns: title and text
	'''
	def documentTermMatrix(docTexts: Dataset[(String, String)], stopWordsFile, numTerms)
		: (DataFrame, Array[String], Map[Long, String], Array[Double]):
		terms = contentsToTerms(docTexts, stopWordsFile)

		termsDF = terms.toDF("title", "terms")
		filtered = termsDF.where(size($"terms") > 1)

		countVectorizer = CountVectorizer()
			.setInputCol("terms").setOutputCol("termFreqs").setVocabSize(numTerms)
		vocabModel = countVectorizer.fit(filtered)
		docTermFreqs = vocabModel.transform(filtered)

		termIds = vocabModel.vocabulary

		docTermFreqs.cache()

		docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap

		idf = IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
		idfModel = idf.fit(docTermFreqs)
		docTermMatrix = idfModel.transform(docTermFreqs).select("title", "tfidfVec")

		(docTermMatrix, termIds, docIds, idfModel.idf.toArray)
	# end def
# end def
