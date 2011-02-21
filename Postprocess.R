##
# Postprocess.R
#
# Authors: Jeff Leek & Ben Langmead
# 'qvalue' function borrowed from 'qvalue' package by John D. Storey
#
# Postprocess Myrna results into a series of R plots.
#

##
# Write something to stderr with a newline
#
msg <- function(...) {
	sink(stderr())
	cat("Postprocess.R: ")
	cat(...)
	cat(" at ")
	cat(format(Sys.time(), "%H:%M:%S"))
	cat('\n')
	flush(stderr())
	sink()
}

##
# Convert labels to group names by removing trailing - and everything
# after.  If there's no dash, leave it alone. 
#
labToGroup <- function(labs) { sub("-.*$", "", labs) }

##
# Convert labels to samples by removing everything up to and including
# the first dash.  If there's no dash, leave it alone.
#
labToSample <- function(labs) { sub("^[^-]*-", "", labs) }

##
# Plot coverage for one gene.
#
plotGene <- function(inaln, exons) {
	msg("Alignments: ", inaln)
	genename <- sub(".txt$", "", inaln)
	msg("Gene name: ", genename)
	inaln <- paste("alignments/", inaln, sep="")
	msg("Alignments (adjusted): ", inaln)
	
	# Read alignments
	reads <- read.table(
		inaln, sep='\t', as.is=T, quote="", header=F,
		comment.char="", colClasses=c(
			"character",  # Sample
			"integer",    # Offset
			"character",  # Strand
			"integer",    # SeqLen
			"character",  # Oms
			"character",  # CIGAR
			"character")) # Mate
	colnames(reads) <-
		c("sample", "offset", "strand", "seqlen", "oms", "cigar", "mate")

 	if(length(reads$sample) == 0) {
 		msg("Read 0 alignments")
		q(status=1);
	}
	readlens <- reads$seqlen

	# Get the annotation information
	sexons <- if(is.null(exons)) { list() } else { exons[(exons$gene == genename),] }
	bb <- as.numeric(as.vector(sexons$start))
	ee <- as.numeric(as.vector(sexons$end))
	nexons <- length(bb)

	# Get the sample labels
	slabels <- reads$sample
	slabels.vals <- sort(unique(slabels))
	nslabels <- length(slabels.vals)

	# Get the groups
	grp <- labToGroup(slabels)
	samp <- labToSample(slabels)

	# Get the colors
	ntot <- sum(table(grp, samp) > 0)
	cols <- as.numeric(as.factor(unlist(strsplit(unique(reads$sample),"-"))[1:ntot*2-1]))

	# Find the length of the reads
	start <- reads$offset
	end <- reads$offset + readlens
	nreads <- dim(reads)[1]

	# Get the positions
	pos <- min(reads$offset):(max(reads$offset) + readlens[nreads])

	# Make the individual plot
	pdf(file=paste(paste("alignments/", genename, sep=""), "replicates.pdf", sep="_"))
	plot(pos,pos,ylim=c(-1,ntot),type="n",xlab="Genomic Position",ylab="",yaxt="n")
 	axis(side=2,at=c(-0.5,1:ntot),labels=c("Exons",unique(reads$sample)),las=2)
	msg("Drawing",nexons,"exons for per-replicate plot")
	if(nexons > 0) {
		for(i in 1:nexons){
			polygon(c(bb[i],bb[i],ee[i],ee[i]),c(-0.60,-0.40,-0.40,-0.60),lwd=2,col="green")
		}
	}

	# Get the max
	mx <- 0
	for(i in 1:length(slabels.vals)){
		ss <- c(min(reads$offset), reads$offset[slabels==slabels.vals[i]], max(reads$offset))
		ww <- c(2,nchar(readlens[slabels==slabels.vals[i]]),100)
		ir <- IRanges(ss, (ss + ww - 1))
		cc <- as.vector(coverage(ir,shift=(-min(ss)  + 1),weight=c(0,rep(1,length(ss)-2),0)))
		mx <- max(c(mx,cc))
		#pos <- min(ss):(max(ss) + ww[length(ww)] - 1)
	}

	# Do the plot
	for(i in 1:length(slabels.vals)){
		ss <- c(min(reads$offset),reads$offset[slabels==slabels.vals[i]],max(reads$offset))
		ww <- c(2,nchar(readlens[slabels==slabels.vals[i]]),100)
		ir <- IRanges(ss, (ss + ww - 1))
		cc <- as.vector(coverage(ir,shift=(-min(ss)  + 1),weight=c(0,rep(1,length(ss)-2),0)))/mx
		pos <- min(ss):(max(ss) + ww[length(ww)] - 1)
		lines(pos,cc+i,col=cols[i])
		xx <- c(pos,rev(pos))
		yy <- c(rep(i,length(pos)),rev(cc + i))
		polygon(xx,yy,col=cols[i],border=cols[i])
	}
	invisible(dev.off())

	# Make the average plot
	pdf(file=paste(paste("alignments/", genename, sep=""), "groups.pdf", sep="_"))
	plot(pos,pos,ylim=c(-1,(length(unique(grp))+1)),type="n",xlab="Genomic Position",ylab="",yaxt="n")
	axis(side=2,at=c(-0.5,1:length(unique(grp))),labels=c("Exons",unique(grp)),las=2)
	msg("Drawing",nexons,"exons for grouped plot")
	if(nexons > 0) {
		for(i in 1:nexons){ 
			polygon(c(bb[i],bb[i],ee[i],ee[i]),c(-0.60,-0.40,-0.40,-0.60),lwd=2,col="green")
		}
	}

	# Get the max
	mx <- 0
	for(i in 1:length(unique(grp))){
		ss <- c(min(reads$offset),reads$offset[grp==unique(grp)[i]], max(reads$offset))
		ww <- c(2,nchar(readlens[grp==unique(grp)[i]]),100)
		ir <- IRanges(ss, (ss + ww - 1))
		cc <- as.vector(coverage(ir,shift=(-min(ss)  + 1),weight=c(0,rep(1,length(ss)-2),0)))
		mx <- max(c(mx,cc))
		#pos <- min(ss):(max(ss) + ww[length(ww)] - 1)
	}

	for(i in 1:length(unique(grp))){
		ss <- c(min(reads$offset),reads$offset[grp==unique(grp)[i]],max(reads$offset))
		ww <- c(2,nchar(readlens[grp==unique(grp)[i]]),100)
		ir <- IRanges(ss, (ss + ww - 1))
		cc <- as.vector(coverage(ir,shift=(-min(ss)  + 1),weight=c(0,rep(1,length(ss)-2),0)))/mx
		pos <- min(ss):(max(ss) + ww[length(ww)] - 1)
		lines(pos,cc+i,col=cols[i])
		xx <- c(pos,rev(pos))
		yy <- c(rep(i,length(pos)),rev(cc + i))
		polygon(xx,yy,col=i,border=i)
	}
	invisible(dev.off())
}

##
# Borrowed (with GUI stuff removed) from John Storey's qvalue package.
#
qvalue <- function(p = NULL, lambda = seq(0, 0.9, 0.05), pi0.method = "smoother",
                   fdr.level = NULL, robust = FALSE, gui = FALSE, smooth.df = 3,
                   smooth.log.pi0 = FALSE)
{
	if (min(p) < 0 || max(p) > 1) {
		msg("ERROR: p-values not in valid range.")
		return(0)
	}
	if (length(lambda) > 1 && length(lambda) < 4) {
		msg("ERROR: If length of lambda greater than 1, you need at least 4 values.")
		return(0)
	}
	if (length(lambda) > 1 && (min(lambda) < 0 || max(lambda) >= 1)) {
		msg("ERROR: Lambda must be within [0, 1).")
		return(0)
	}
	m <- length(p)
	if (length(lambda) == 1) {
		if (lambda < 0 || lambda >= 1) {
			msg("ERROR: Lambda must be within [0, 1).")
			return(0)
		}
		pi0 <- mean(p >= lambda)/(1 - lambda)
		pi0 <- min(pi0, 1)
	}
	else {
		pi0 <- rep(0, length(lambda))
		for (i in 1:length(lambda)) {
			pi0[i] <- mean(p >= lambda[i])/(1 - lambda[i])
		}
		if (pi0.method == "smoother") {
			if (smooth.log.pi0) 
				pi0 <- log(pi0)
			spi0 <- smooth.spline(lambda, pi0, df = smooth.df)
			pi0 <- predict(spi0, x = max(lambda))$y
			if (smooth.log.pi0) 
				pi0 <- exp(pi0)
			pi0 <- min(pi0, 1)
		}
		else if (pi0.method == "bootstrap") {
			minpi0 <- min(pi0)
			mse <- rep(0, length(lambda))
			pi0.boot <- rep(0, length(lambda))
			for (i in 1:100) {
				p.boot <- sample(p, size = m, replace = TRUE)
				for (i in 1:length(lambda)) {
					pi0.boot[i] <- mean(p.boot > lambda[i])/(1 - lambda[i])
				}
				mse <- mse + (pi0.boot - minpi0)^2
			}
			pi0 <- min(pi0[mse == min(mse)])
			pi0 <- min(pi0, 1)
		}
		else {
			msg("ERROR: 'pi0.method' must be one of 'smoother' or 'bootstrap'.")
			return(0)
		}
	}
	if (pi0 <= 0) {
		msg("Warning: The estimated pi0 <= 0. Check that you have valid p-values or use another lambda method.")
		return(1)
	}
	if (!is.null(fdr.level) && (fdr.level <= 0 || fdr.level > 1)) {
		msg("ERROR: 'fdr.level' must be within (0, 1].")
		return(0)
	}
	u <- order(p)
	qvalue.rank <- function(x) {
		idx <- sort.list(x)
		fc <- factor(x)
		nl <- length(levels(fc))
		bin <- as.integer(fc)
		tbl <- tabulate(bin)
		cs <- cumsum(tbl)
		tbl <- rep(cs, tbl)
		tbl[idx] <- tbl
		return(tbl)
	}
	v <- qvalue.rank(p)
	qvalue <- pi0 * m * p/v
	if (robust) {
		qvalue <- pi0 * m * p/(v * (1 - (1 - p)^m))
	}
	qvalue[u[m]] <- min(qvalue[u[m]], 1)
	for (i in (m - 1):1) {
		qvalue[u[i]] <- min(qvalue[u[i]], qvalue[u[i + 1]], 1)
	}
	if (!is.null(fdr.level)) {
		retval <- list(call = match.call(), pi0 = pi0, qvalues = qvalue, 
			pvalues = p, fdr.level = fdr.level, significant = (qvalue <= 
				fdr.level), lambda = lambda)
	}
	else {
		retval <- list(call = match.call(), pi0 = pi0, qvalues = qvalue, 
			pvalues = p, lambda = lambda)
	}
	class(retval) <- "qvalue"
	return(retval)
}

##
# A wrapper for qvalue that returns just the qvalues and handles the
# case where it complains about the pi0 estimate.
#
qvalue2 <- function(p) {
	qvals <- qvalue(p)
	qvals <- if(class(qvals) == "numeric" && qvals == 1) {
		msg("Warning: Very low p-values detected, perhaps due to",
		    "very small sample size.  All q-values will be set to 0.")
		rep(0.0, length(p))
	} else {
		qvals$qvalues
	}
}

##
# Plot p-value histogram for p-values in 'pvals' and put the pdf result in 'dest'
#
processPvalues <- function(pvals) {
	msg("PValue file: ", pvals)
	# Read in the alignments
	pv <- read.table(pvals, as.is=T, quote="", header=T)
	if(length(pv[,1]) == 0) {
		msg("Read 0 p-values")
		q(status=1)
	}
	msg("Read ", length(pv[,1]), " p-values")
	qvals <- qvalue2(pv[,2])
	qv <- cbind(pv[,1], format(qvals, scientific=T))
	colnames(qv) <- c("ensembl_gene_id", "q_value")
	write.table(qv, "qvals.txt", row.names=F, col.names=T, sep="\t", quote=F)

	# Make the p-value plot
	pdf(file="pval_hist.pdf")
	hist(as.numeric(pv[,2]), main="p-value histogram", xlab="p-value", col="dodgerblue", xlim=c(0, 1))
	invisible(dev.off())
	pdf(file="pval_hist_dense.pdf")
	hist(as.numeric(pv[,2]), main="p-value histogram", xlab="p-value", col="dodgerblue", xlim=c(0, 1), breaks=80)
	invisible(dev.off())

	# Make the q-value plot
	pdf(file="qval_hist.pdf")
	hist(qvals, main="q-value histogram", xlab="q-value", col="dodgerblue", xlim=c(0, 1))
	invisible(dev.off())
	pdf(file="qval_hist_dense.pdf")
	hist(qvals, main="q-value histogram", xlab="q-value", col="dodgerblue", xlim=c(0, 1), breaks=80)
	invisible(dev.off())

	# Make the p-value versus log-count smoothScatter plot
	counts <- read.table("count_table.txt", as.is=T, quote="", header=T)
	rsum <- rowSums(counts)
	counts.sum <- rsum[rsum > 0]
	if(length(counts.sum) != length(pv[,2])) {
		msg("Warning: length of the non-zero row-sum vector is",length(counts.sum),
		    "but length of p-value vector is",length(pv[,2]))
	} else {
		pdf(file="pval_scatter.pdf")
		smoothScatter(log(counts.sum+1), pv[,2],
		              xlab="Log(sum of all counts+1) per gene", ylab="p-value", main="p-value scatter")
		invisible(dev.off())
	 }
}

exonsFn <- "exons.txt";
exonsFh <- file(exonsFn, open = "r");
exonsFirst <- readLines(exonsFh, n = 1, warn = FALSE);
exonsHead <- unlist(strsplit(exonsFirst, "\t", fixed=TRUE));
# Load exons
cls <- c(
		"character",  # Gene id
		"character",  # Transcript id
		"character",  # Exon id
		"character",  # Chr name
		"integer",    # Exon start
		"integer");   # Exon end
if(length(exonsHead) == 8) {
	# Includes is_constitutive
	cls <- c(cls, "integer"); # Constitutive?
} else {
	# Does not include is_constitutive
	if(length(exonsHead) != 7) {
		# Unexpected number of columns in exons.txt
		msg("Expected exons.txt header line to contain 7 or 8 columns; got:\n", exonsHead);
		q(status=1)
	}
}
cls <- c(cls, "character"); # Biotype

exons <- if(file.exists("exons.txt")) {
	read.table("exons.txt", sep='\t', as.is=T, quote="", header=T, comment.char="", colClasses=cls)
} else { NULL }

if(!is.null(exons)) {
	if(length(exonsHead) == 7) {
		colnames(exons) <- c("gene", "transcript", "exon", "chr", "start", "end", "biotype");
	} else {
		colnames(exons) <- c("gene", "transcript", "exon", "chr", "start", "end", "const", "biotype");
	}
}

args <- commandArgs(T)
cores <- args[2]

if(file_test("-d", "alignments")) {
	mclapply(list.files("alignments", pattern="[.]txt$"), plotGene, exons=exons, mc.cores=cores)
}

processPvalues("pvals.txt")
