
class RunRisk {
	def main(self):
		spark = SparkSession.builder().getOrCreate()
		runRisk = RunRisk(spark)

		(stocksReturns, factorsReturns) = runRisk.readStocksAndFactors()
		runRisk.plotDistribution(factorsReturns(2))
		runRisk.plotDistribution(factorsReturns(3))

		numTrials = 10000000
		parallelism = 1000
		baseSeed = 1001L
		trials = runRisk.computeTrialReturns(stocksReturns, factorsReturns, baseSeed, numTrials,
			parallelism)
		trials.cache()
		valueAtRisk = runRisk.fivePercentVaR(trials)
		conditionalValueAtRisk = runRisk.fivePercentCVaR(trials)
		print("VaR 5%: " + valueAtRisk)
		print("CVaR 5%: " + conditionalValueAtRisk)
		varConfidenceInter= runRisk.bootstrappedConfidenceInterval(trials,
			runRisk.fivePercentVaR, 100, .05)
		cvarConfidenceInter= runRisk.bootstrappedConfidenceInterval(trials,
			runRisk.fivePercentCVaR, 100, .05)
		print("VaR confidence interval: " + varConfidenceInterval)
		print("CVaR confidence interval: " + cvarConfidenceInterval)
		print("Kupiec test p-value: " + runRisk.kupiecTestPValue(stocksReturns, valueAtRisk, 0.05))
		runRisk.plotDistribution(trials)
	# end def
# end def

class RunRisk(private spark: SparkSession) {

	'''
	 * Reads a history in the Google format
	'''
	def readGoogleHistory(file: File): Array[(LocalDate, Double)]:
		formatter = DateTimeFormatter.ofPattern("d-MMM-yy")
		lines = scala.io.Source.fromFile(file).getLines().toSeq
		lines.tail.map { line :
			cols = line.split(',')
			date = LocalDate.parse(cols(0), formatter)
			value = cols(4).toDouble
			(date, value)
		# end def.reverse.toArray
	# end def

	def trimToRegion(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
		: Array[(LocalDate, Double)]:
		var trimmed = history.dropWhile(_._1.isBefore(start)).
			takeWhile(x : x._1.isBefore(end) || x._1.isEqual(end))
		if (trimmed.head._1 != start) {
			trimmed = Array((start, trimmed.head._2)) ++ trimmed
		# end def
		if (trimmed.last._1 != end) {
			trimmed = trimmed ++ Array((end, trimmed.last._2))
		# end def
		trimmed
	# end def

	'''
	 * Given a timeseries of values of an instruments, returns a timeseries between the given
	 * start and end dates with all missing weekdays imputed. Values are imputed as the value on the
	 * most recent previous given day.
	'''
	def fillInHistory(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
		: Array[(LocalDate, Double)]:
		var cur = history
		filled = ArrayBuffer[(LocalDate, Double)]()
		var curDate = start
		while (curDate.isBefore(end)) {
			if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
				cur = cur.tail
			# end def

			filled += ((curDate, cur.head._2))

			curDate = curDate.plusDays(1)
			# Skip weekends
			if (curDate.getDayOfWeek.getValue > 5) {
				curDate = curDate.plusDays(2)
			# end def
		# end def
		filled.toArray
	# end def

	def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double]:
		history.sliding(10).map { window :
			next = window.last._2
			prev = window.head._2
			(next - prev) / prev
		# end def.toArray
	# end def

	def readStocksAndFactors(): (Seq[Array[Double]], Seq[Array[Double]]):
		start = LocalDate.of(2009, 10, 23)
		end = LocalDate.of(2014, 10, 23)

		stocksDir = File("stocks/")
		files = stocksDir.listFiles()
		allStocks = files.iterator.flatMap { file :
			try {
				Some(readGoogleHistory(file))
			# end def catch {
				case e: Exception : None
			# end def
		# end def
		rawStocks = allStocks.filter(_.size >= 260 * 5 + 10)

		factorsPrefix = "factors/"
		rawFactors = Array("NYSEARCA%3AGLD.csv", "NASDAQ%3ATLT.csv", "NYSEARCA%3ACRED.csv").
			map(x : File(factorsPrefix + x)).
			map(readGoogleHistory)

		stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

		factors = rawFactors.
			map(trimToRegion(_, start, end)).
			map(fillInHistory(_, start, end))

		stocksReturns = stocks.map(twoWeekReturns).toArray.toSeq
		factorsReturns = factors.map(twoWeekReturns)
		(stocksReturns, factorsReturns)
	# end def

	def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]]:
		mat = Array[Array[Double]](histories.head.length)
		for (i <- histories.head.indices) {
			mat(i) = histories.map(_(i)).toArray
		# end def
		mat
	# end def

	def featurize(factorReturns: Array[Double]): Array[Double]:
		squaredReturns = factorReturns.map(x : math.signum(x) * x * x)
		squareRootedReturns = factorReturns.map(x : math.signum(x) * math.sqrt(math.abs(x)))
		squaredReturns ++ squareRootedReturns ++ factorReturns
	# end def

	def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])
		: OLSMultipleLinearRegression:
		regression = OLSMultipleLinearRegression()
		regression.newSampleData(instrument, factorMatrix)
		regression
	# end def

	def computeFactorWeights(
		stocksReturns: Seq[Array[Double]],
		factorFeatures: Array[Array[Double]]): Array[Array[Double]]:
		stocksReturns.map(linearModel(_, factorFeatures)).map(_.estimateRegressionParameters()).toArray
	# end def

	def trialReturns(
			seed: Long,
			numTrials,
			instruments: Seq[Array[Double]],
			factorMeans: Array[Double],
			factorCovariances: Array[Array[Double]]): Seq[Double]:
		rand = MersenneTwister(seed)
		multivariateNormal = MultivariateNormalDistribution(rand, factorMeans,
			factorCovariances)

		trialReturns = Array[Double](numTrials)
		for (i <- 0 until numTrials) {
			trialFactorReturns = multivariateNormal.sample()
			trialFeatures = featurize(trialFactorReturns)
			trialReturns(i) = trialReturn(trialFeatures, instruments)
		# end def
		trialReturns
	# end def

	'''
	 * Calculate the full return of the portfolio under particular trial conditions.
	'''
	def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]):
		var totalReturn = 0.0
		for (instrument <- instruments) {
			totalReturn += instrumentTrialReturn(instrument, trial)
		# end def
		totalReturn / instruments.size
	# end def

	'''
	 * Calculate the return of a particular instrument under particular trial conditions.
	'''
	def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]):
		var instrumentTrialReturn = instrument(0)
		var i = 0
		while (i < trial.length) {
			instrumentTrialReturn += trial(i) * instrument(i+1)
			i += 1
		# end def
		instrumentTrialReturn
	# end def

	def plotDistribution(samples: Array[Double]): Figure:
		min = samples.min
		max = samples.max
		stddev = StatCounter(samples).stdev
		bandwidth = 1.06 * stddev * math.pow(samples.size, -.2)

		# Using toList before toArray avoids a Scala bug
		domain = Range.Double(min, max, (max - min) / 100).toList.toArray
		kd = KernelDensity().
			setSample(samples.toSeq.toDS.rdd).
			setBandwidth(bandwidth)
		densities = kd.estimate(domain)
		f = Figure()
		p = f.subplot(0)
		p += plot(domain, densities)
		p.xlabel = "Two Week Return ($)"
		p.ylabel = "Density"
		f
	# end def

	def plotDistribution(samples: Dataset[Double]): Figure:
		(min, max, count, stddev) = samples.agg(
			functions.min($"value"),
			functions.max($"value"),
			functions.count($"value"),
			functions.stddev_pop($"value")
		).as[(Double, Double, Long, Double)].first()
		bandwidth = 1.06 * stddev * math.pow(count, -.2)

		# Using toList before toArray avoids a Scala bug
		domain = Range.Double(min, max, (max - min) / 100).toList.toArray
		kd = KernelDensity().
			setSample(samples.rdd).
			setBandwidth(bandwidth)
		densities = kd.estimate(domain)
		f = Figure()
		p = f.subplot(0)
		p += plot(domain, densities)
		p.xlabel = "Two Week Return ($)"
		p.ylabel = "Density"
		f
	# end def

	def fivePercentVaR(trials: Dataset[Double]):
		quantiles = trials.stat.approxQuantile("value", Array(0.05), 0.0)
		quantiles.head
	# end def

	def fivePercentCVaR(trials: Dataset[Double]):
		topLosses = trials.orderBy("value").limit(math.max(trials.count().toInt / 20, 1))
		topLosses.agg("value" -> "avg").first()(0).asInstanceOf[Double]
	# end def

	def bootstrappedConfidenceInterval(
			trials: Dataset[Double],
			computeStatistic: Dataset[Double] ,
			numResamples,
			probability): (Double, Double):
		stats = (0 until numResamples).map { i :
			resample = trials.sample(true, 1.0)
			computeStatistic(resample)
		# end def.sorted
		lowerIndex = (numResamples * probability / 2 - 1).toInt
		upperIndex = math.ceil(numResamples * (1 - probability / 2)).toInt
		(stats(lowerIndex), stats(upperIndex))
	# end def

	def countFailures(stocksReturns: Seq[Array[Double]], valueAtRisk):
		var failures = 0
		for (i <- stocksReturns.head.indices) {
			loss = stocksReturns.map(_(i)).sum
			if (loss < valueAtRisk) {
				failures += 1
			# end def
		# end def
		failures
	# end def

	def kupiecTestStatistic(total, failures, confidenceLevel):
		failureRatio = failures.toDouble / total
		logNumer = (total - failures) * math.log1p(-confidenceLevel) +
			failures * math.log(confidenceLevel)
		logDenom = (total - failures) * math.log1p(-failureRatio) +
			failures * math.log(failureRatio)
		-2 * (logNumer - logDenom)
	# end def

	def kupiecTestPValue(
			stocksReturns: Seq[Array[Double]],
			valueAtRisk,
			confidenceLevel):
		failures = countFailures(stocksReturns, valueAtRisk)
		total = stocksReturns.head.length
		testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
		1 - ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
	# end def

	def computeTrialReturns(
			stocksReturns: Seq[Array[Double]],
			factorsReturns: Seq[Array[Double]],
			baseSeed: Long,
			numTrials,
			parallelism): Dataset[Double]:
		factorMat = factorMatrix(factorsReturns)
		factorCov = Covariance(factorMat).getCovarianceMatrix().getData()
		factorMeans = factorsReturns.map(factor : factor.sum / factor.size).toArray
		factorFeatures = factorMat.map(featurize)
		factorWeights = computeFactorWeights(stocksReturns, factorFeatures)

		# Generate different seeds so that our simulations don't all end up with the same results
		seeds = (baseSeed until baseSeed + parallelism)
		seedDS = seeds.toDS().repartition(parallelism)

		# Main computation: run simulations and compute aggregate return for each
		seedDS.flatMap(trialReturns(_, numTrials / parallelism, factorWeights, factorMeans, factorCov))
	# end def
# end def

if __name__ == '__main__':
	app = RunRisk()
	app.main()
# end if
