##
# Stats.R
#
# Authors: Jeff Leek & Ben Langmead
#
# Load one or more gene's worth of alignments and calculate a statistic
# on each gene's worth, and output a per-gene record.
#

# Stats.pl loads this when invoking Rscript
#library(lmtest)
#library(MASS)

##
# Increment given Hadoop counter by given amount.  Hadoop interprets
# lines of stderr output with format "reporter:counter:<name>:<amt>" as
# a request to atomically increment the global counter called <name> by
# amount <amt>.
#
counter <- function(names, amts) {
	sink(stderr())
	cat(paste("reporter:counter:Stats,", names, ",", amts, sep="", collapse="\n"), "\n")
	flush(stderr())
	sink()
}

##
# Write something to stderr with a newline
#
msg <- function(...) {
	sink(stderr())
	cat("Stats.R [")
	cat(format(Sys.time(), "%H:%M:%S"))
	cat("]: ")
	cat(...)
	cat('\n')
	sink()
}

##
# Get number of rows for an object that might be a vector or a matrix.
# Vectors have 1 row.
#
rows <- function(vecOrMat) {
	nr <- nrow(vecOrMat)
	if(is.null(nr) && length(vecOrMat) > 0) {
		1
	} else if(is.null(nr)) {
		0
	} else {
		nr
	}
}

##
# Wrap glm and check arguments
#
glmWrap <- function(dat, grps, family, offset) {
	if(length(dat) != length(as.factor(grps))) {
		msg(c("Warning: data vector (",dat,") has length",length(dat),"but groups vector (",grps,") has length",length(grps),"; offsets=(",offset,")"))
		q(status=1)
	}
	if(length(dat) != length(offset)) {
		msg(c("Warning: data vector (",dat,") has length",length(dat),"but offset vector (",offset,") has length",length(offset),"; grps=(",grps,")"))
		q(status=1)
	}
	dat.l <- if(family == "gaussian") { log(dat+1) } else { dat }
	glm(dat.l ~ as.factor(grps) + log(offset+1), family=family)
}

##
# Wrap paired glm and check arguments
#
glmWrapPaired <- function(dat, grps, pairs, family, offset) {
	if(length(dat) != length(as.factor(grps))) {
		msg(c("Warning: data vector (",dat,") has length",length(dat),"but groups vector (",grps,") has length",length(grps),"; offsets=(",offset,")"))
		q(status=1)
	}
	if(length(dat) != length(offset)) {
		msg(c("Warning: data vector (",dat,") has length",length(dat),"but offset vector (",offset,") has length",length(offset),"; grps=(",grps,")"))
		q(status=1)
	}
	if(length(dat) != length(pairs)) {
		msg(c("Warning: data vector (",dat,") has length",length(dat),"but pairs vector (",pairs,") has length",length(offset),"; grps=(",grps,")"))
		q(status=1)
	}
	dat.l <- if(family == "gaussian") { log(dat+1) } else { dat }
	glm(dat.l ~ as.factor(grps) + as.factor(pairs) + log(offset+1), family=family)
}

##
# Calculate Pvals for one row of data, assuming there are two groups
# and they're matched in alphabetical order.
#
calcPairedPvalRow <- function(dat, grps.row, pairs, ltots, family) {
	glm1 <- glmWrapPaired(dat, grps.row, pairs, family, ltots)
	# Note: Unlike with lrtest(), [2,5] is not necessarily >= 0 here
	summary(glm1)$coeff[2,4]
}

##
# Calculate Pvals for one row of data and one grouping.
#
calcPvalRow <- function(dat, grps.row, ltots, family, stats) {
	glm1 <- glmWrap(dat, grps.row, family=family, offset=ltots)
	dat.l <- if(family == "gaussian") { log(dat+1) } else { dat }
	glm0 <- glm(dat.l ~ log(1+ltots), family=family)
	lrtest(glm0, glm1)[2,if(stats) {4} else {5}]
}

##
# Calculate Pvals for one row of data and a matrix of groupings.
#
calcPval <- function(dat, grps.mat, ltots, family, stats) {
	apply(grps.mat, 1, calcPvalRow, dat=dat, ltots=ltots, family=family, stats=stats)
}

##
# Calculate null Pvals for a matrix of data with given grouping.
#
calcNullPvals <- function(nulls.per.unit, dat.mat, grps, ltots, family) {
	grps.mat <- matrix(replicate(nulls.per.unit, sample(grps, length(grps))), nulls.per.unit, byrow=T)
	counter("Null Pvals calculated", nulls.per.unit * rows(dat.mat))
	counter("Null Pval batches calculated", 1)
	apply(dat.mat, 1, calcPval, grps.mat=grps.mat, ltots=ltots, family=family, stats=T)
}

##
# Calculate observed Pvals for a matrix of data with given grouping.
#
calcObsPvals <- function(dat.mat, grps, ltots, family, stats) {
	counter("Observed Pval batches calculated", 1)
	if(is.null(nrow(dat.mat)) && length(dat.mat) > 0) {
		counter("Observed Pvals calculated", 1)
		calcPvalRow(dat.mat, grps, ltots, family)
	} else {
		counter("Observed Pvals calculated", nrow(dat.mat))
		apply(dat.mat, 1, calcPvalRow, grps.row=grps, ltots=ltots, family=family, stats=stats)
	}
}

##
# Calculate observed Pvals for a matrix of data with given grouping,
# assuming the groups match up.
#
calcObsPairedPvals <- function(dat.mat, grps, ltots, family) {
	counter("Observed Pval batches calculated", 1)
	pairs <- rep(1:(length(grps)/2), 2)
	if(is.null(nrow(dat.mat)) && length(dat.mat) > 0) {
		counter("Observed Pvals calculated", 1)
		calcPairedPvalRow(dat.mat, grps, pairs, ltots, family)
	} else {
		counter("Observed Pvals calculated", nrow(dat.mat))
		apply(dat.mat, 1, calcPairedPvalRow, grps.row=grps, pairs=pairs, ltots=ltots, family=family)
	}
}

##
# Calculate fake Pvals (all 1) for a matrix of data.
#
calcFakePvals <- function(dat.mat) {
	counter("Fake Pval batches calculated", 1)
	if(is.null(nrow(dat.mat)) && length(dat.mat) > 0) {
		counter("Fake Pvals calculated", 1)
		1
	} else {
		counter("Fake Pvals calculated", nrow(dat.mat))
		rep(1, nrow(dat.mat))
	}
}

##
# Convert labels to group names by leading - and everything after.  If
# there's no dash, leave it alone. 
#
labToGroup <- function(labs) { sub("-.*$", "", labs) }

##
# Format a list of statistics or P-values so that they sort properly.
# This involves clamping Inf and extremely large numbers down to about
# 10^10.
#
formatVals <- function(vals, digits, width) {
	formatC(pmin(vals, 999999999), flag="0", format="f", digits=digits, width=width)
}

##
#
#
deTest <- function(fn, all.labels.str, famstr, nulls.per.unit, bypass.pvals, add.fudge, paired) {
	
	msg(c("Called deTest(", fn, ", ", all.labels.str, ", ", famstr, ", ", bypass.pvals, ", ", add.fudge, ", ", paired, ")"), sep="")

	# Read in the alignments
	reads <- scan(
		fn, sep='\t', quote="", comment.char="",
		allowEscapes=F, multi.line=F, what=list(
			"character",  # Name
			"integer",    # Offset
			"character",  # Strand
			"integer",    # SeqLen
			"character",  # Ignore
			"character",  # Mis
			"character",  # Strat
			"character",  # Label (e.g. "H-1", "T-19")
			"integer"))   # Norm (any type of normalization factor)
	
	names(reads) <- c("gene", "offset", "strand", "seqlen", "oms",
	                  "cigar", "strat", "label", "norm")
	
	nreads <- length(reads$gene)
	msg(c("Processing batch of", nreads, "alignments"))

	# Get the unit of analysis (genes, exons, etc. by strand)
	
	units <- table(reads$gene)
	units.vals <- names(units)
	nunits <- length(units.vals) # nunits = # of different genes in this batch

	msg(c("Alignment batch has", nunits, "genes"))
	msg(c("Genes:", paste(units.vals, sep=",")))

	# Get the labels in this batch
	labels <- table(reads$label)
	labels.vals <- names(labels)
	nsamples <- length(labels.vals) # nsamples = # of different labels in batch

	msg(c("Alignment batch has", nsamples, "distinct labels"))
	msg(c("Labels:", paste(labels.vals, sep=",")))

	# Get the labels in the whole dataset (passed in from wrapper)
	all.labels <- unlist(strsplit(all.labels.str, split=",", fixed=TRUE))
	if(length(all.labels) != length(unique(all.labels))) {
		msg(c("Warning! labels list has non-unique elements"))
		q(status=1)
	}
	all.nsamples <- length(all.labels)

	msg(c("Whole dataset has", all.nsamples, "distinct labels"))
	msg(c("Labels:", paste(all.labels, sep=",")))

	# Ensure all the batch's labels are legit
	if(!all(labels.vals %in% all.labels)) {
		msg(c("ERROR: one or more batch labels were not in the whole-dataset list of labels"))
		q(status=1)
	}

	# Get the groups in this batch
	grp <- table(labToGroup(reads$label))
	grp.vals <- names(grp)
	ngrps <- length(grp.vals)

	msg(c("Batch has", ngrps, "distinct groups"))
	msg(c("Groups:", paste(grp.vals, sep=",")))

	# Get the groups in the whole dataset
	all.grp <- labToGroup(all.labels)
	all.grp.vals <- unique(all.grp)

	msg(c("Whole dataset has", length(all.grp.vals), "distinct groups"))
	msg(c("Groups:", paste(all.grp.vals, sep=",")))
	
	if(paired) {
		nulls.per.unit <- 0
		all.grp.tab <- table(all.grp)
		if(dim(all.grp.tab) != 2) {
			msg(c("Error: Stats.R deTest() called with paired=True, but there are",dim(all.grp.tab),"distinct groups in the input"))
			q(status=1);
		}
		g1 <- all.grp.tab[1]
		g2 <- all.grp.tab[2]
		if(g1 != g2) {
			msg(c("Error: Stats.R deTest() called with paired=True, but the",
			      "number of samples per group is different:",
			      g1, "in group", all.grp[1], "and", g2, "in", all.grp[2]))
			q(status=1);
		}
	}

	if(!all(grp.vals %in% all.grp.vals)) {
		msg(c("ERROR: one or more batch groups were not in the whole-dataset list of groups"))
		q(status=1)
	}

	msg("Factorizing label column")
	reads$label <- factor(reads$label, levels=all.labels)

	msg("Getting per-sample normalization factors")
	lab.by.norm.tab <- table(reads$label, reads$norm)
	totals <- apply(lab.by.norm.tab, 1, which.max)
	totals <- colnames(lab.by.norm.tab)[totals]
	totals <- as.integer(totals) + add.fudge
	names(totals) <- rownames(lab.by.norm.tab)

	# Set up the data matrix
	msg("Setting up data matrix")
	dat <- as.matrix(table(reads$gene, reads$label)) + add.fudge
	
	if(length(names(totals)) != length(colnames(dat)) ||
	   any(names(totals) != colnames(dat)))
	{
		msg(c("Names of the normalization factors (", names(totals),
		      ") did not match with column names of the dat matrix (",
		      colnames(dat), ")"))
		q(status=1)
	}
	
	msg("Setting up output strings")
	datstr <- apply(dat, 1, paste, sep=",", collapse=",")
	
	digits <- if (nulls.per.unit > 0) { 10 } else { 10 }
	width <- if (nulls.per.unit > 0) { 10+digits } else { 10+digits }
	pvals <- if(bypass.pvals || length(all.grp) < 2) {
		msg("Calculating batch of fake P-values")
		calcFakePvals(dat)
	} else {
		msg("Calculating batch of",rows(dat),"observed P-values")
		pvals <- if(paired) {
			calcObsPairedPvals(dat, all.grp, totals, famstr)
		} else {
			calcObsPvals(dat, all.grp, totals, famstr, nulls.per.unit > 0)
		}
		if(!all(pvals >= 0)) {
			msg("Some calculated observed P values were < 0")
			sink(stderr())
			print(table(pvals[pvals < 0]))
			q(status=1);
		}
		if(nulls.per.unit == 0) {
			if(!all(pvals <= 1)) {
				msg("Some calculated observed P values were > 1")
				sink(stderr())
				print(table(pvals[pvals > 1]))
				q(status=1);
			}
			abs(-log(pvals))
		} else { pvals }
	}
	if(!all(pvals >= 0)) {
		msg("Some calculated statistics/P-vals were < 0")
		sink(stderr())
		print(pvals)
		q(status=1);
	}
	msg("Outputting observed P-values")
	sink("/dev/stdout")
	cat(paste("1", formatVals(pvals, digits, width), "O", units.vals, sep="\t"), sep="\n")
	if(nulls.per.unit > 0 && !bypass.pvals) {
		msg(c("Calculating a batch of",(nulls.per.unit*rows(dat)),"null P-values for",rows(dat),"genes"))
		nvals <- calcNullPvals(nulls.per.unit, dat, all.grp, totals, famstr)
		msg(c("Outputting batch of null P-values"))
		cat(paste("1", formatVals(nvals, digits, width), "N", sep="\t"), sep="\n")
	}
	sink()
	msg("Finished call to deTest")
}

args <- commandArgs(T)
alnFile <- args[2]
allgrp.str <- args[3]
famstr <- args[4]
nulls.per.unit <- as.integer(args[5])
seed <- as.integer(args[6])
bypass.pvals <- as.logical(as.integer(args[7]))
do.profile <- as.logical(as.integer(args[8]))
add.fudge <- as.integer(args[9])
paired <- as.logical(as.integer(args[10]))

if(do.profile) {
	Rprof(filename = paste("RProfile.Stats.R", Sys.getpid(), sep="."))
}

if(seed >= 0) { set.seed(seed) }

deTest(alnFile, allgrp.str, famstr, nulls.per.unit, bypass.pvals, add.fudge, paired)
