##
# Assign.R
#
# Authors: Ben Langmead and Hector Corrada Bravo
#    Date: November 5, 2009
#
# Load a chunk of alignment tuples from a file, overlap them with a set
# of relevant genomic ranges, then output a new stream of tuples for
# every overlap.  Also, for every read label involved in any overlap,
# output a set of per-interval subtotals which can be used for
# normalization across replicates later on.
#

# Assign.pl loads this when invoking Rscript
# library(IRanges)

##
# Write something to stderr with a newline
#
msg <- function(...) {
	sink(stderr())
	cat("Assign.R [")
	cat(format(Sys.time(), "%H:%M:%S"))
	cat("]: ")
	cat(...)
	cat('\n')
	flush(stderr())
	sink()
}

args <- commandArgs(T)
# Get alignment file from first arg after "--"
alnFile          <- args[2]
maxAlgnInfluence <- as.integer(args[3])
ivalsFromDir     <- args[4]
ivalsDir         <- args[5]
binWidth         <- as.integer(args[6])
from3prime       <- args[7]
fromMiddle       <- args[8]

ivalsFromDir <- (ivalsFromDir == "0")
from3prime <- (from3prime == "1")
fromMiddle <- (fromMiddle == "1")

msg("Alignment file:", alnFile)
msg("Influence:", maxAlgnInfluence)
msg("Ivals from dir?:", ivalsFromDir)
msg("Ivals dir:", ivalsDir)
msg("Bin width:", binWidth)
msg("From 3':", from3prime)
msg("From middle:", fromMiddle)

##
# Increment given Hadoop counter by given amount.  Hadoop interprets
# lines of stderr output with format "reporter:counter:<name>:<amt>" as
# a request to atomically increment the global counter called <name> by
# amount <amt>.
#
counter <- function(names, amts) {
	sink(stderr())
	cat(paste("reporter:counter:Assign,", names, ",", amts, sep="", collapse="\n"), "\n")
	flush(stderr())
	sink()
}

##
# Calculate overlaps given arbitrary intervals of interest.
#
calcOlap <- function(ranges1, ranges2) {
	matchMatrix(findOverlaps(ranges2, ranges1))
}

##
# Calculate overlaps given that intervals of interest are bins.
#
calcBinOlap <- function(ranges1, bwidth) {
	cbind(as.integer(start(ranges1)/bwidth)+1, seq(from=1, to=length(ranges1)));
}

##
# Load ranges from an interval file
#
rangesFromIvalFile <- function(dir, chromo) {
	# First try raw chromosome id, then try "chr" followed by id
	fname1 <- paste(dir, "/", chromo, ".ivals", sep="")
	fname2 <- paste(dir, "/chr", chromo, ".ivals", sep="")
	fname3 <- paste(dir, "/other.ivals", sep="")
	what <- list("", # Chr
	             "", # Gene,
	             integer(0),  # Start
	             integer(0))  # End
	ivs <- if(file.exists(fname1)) {
		msg("Reading intervals from", fname1)
		scan(fname1, what=what, sep='\t', quote="")
	} else if(file.exists(fname2)) {
		msg("Reading intervals from", fname2)
		scan(fname2, what=what, sep='\t', quote="")
	} else if(file.exists(fname3)) {
		msg("Reading intervals from", fname3)
		counter("Had to use other.ivals", 1)
		scan(fname3, what=what, sep='\t', quote="")
	} else {
		counter("No ival file", 1)
		list( as.character(c()), as.character(c()),
		      as.integer(c()),   as.integer(c()) )
	}
	niv <- length(ivs[[1]])
	msg(c("Read", niv, "intervals"))
	# Give columns reasonable labels
	names(ivs) <- c('chromo', 'name', 'start', 'end')
	class(ivs) <- "data.frame"
	attr(ivs, "row.names") <- as.character(seq(len=niv))
	ivs
}

##
# Create ranges given a bin width
#
rangesFromBins <- function(bwidth, chromo) {
	l <- list()
	starts <- seq(from=0, to=300000000, by=bwidth)
	ends <- seq(from=bwidth-1, by=bwidth, length.out=length(starts))
	l[[1]] <- rep(chromo, length(starts))
	l[[2]] <- paste(chromo, "_", format(starts, scientific=F, trim=T), sep="");
	l[[3]] <- starts
	l[[4]] <- ends
	names(l) <- c('chromo', 'name', 'start', 'end')
	class(l) <- "data.frame"
	attr(l, "row.names") <- as.character(seq(len=length(l[[1]])))
	l
}

# Read partition alignments from file
msg("Reading in alignments")
what <- list("",          # Chr
             integer(0),  # Part
             integer(0),  # ChrOff
             "",          # Orient
             integer(0),  # Seq length
             integer(0),  # Oms
             "",          # CIGAR
             "",          # Mate
             "")          # Lab
#what <- list("",          # Chr
#             integer(0),  # Part
#             integer(0),  # ChrOff
#             "",          # Orient
#             "",          # Seq
#             "",          # Qual
#             integer(0),  # Oms
#             "",          # CIGAR
#             "",          # Mate
#             "")          # Lab
# Give columns reasonable labels
alns <- scan(alnFile, what=what, sep='\t', quote="",
             allowEscapes=F, multi.line=F)
names(alns) <- c('Chr', 'Part', 'ChrOff', 'Orient', 'SeqLen',
                 'Oms', 'CIGAR', 'Mate', 'Lab')
class(alns) <- "data.frame"
nread <- length(alns$Chr)
attr(alns, "row.names") <- as.character(seq(len=nread))
counter("Alignments read by Assign.R", nread)

msg(c("Read", nread, "alignments from", alnFile))
# Abort if there were no alignments
if(nread == 0) {
	counter("0-read inputs", 1)
	q(status=1);
}

alns.split <- split(alns, alns$Chr)

##
# Given a bundle of alignments falling into the same chromosome, read
# the corresponding ivals file, calculate overlaps, and emit overlaps.
#
handleChr <- function(alns) {
	nread <- length(alns$Chr)
	if(nread == 0) {
		msg("Error, no reads in call to handleChr")
		q(status=1)
	}
	
	# Extract chromosome name from first alignment (all alignments in alns
	# are in the same partition, and so should have same chromosome name)
	chromo <- alns$Chr[1]
	
	# Load gene annotations from files; only load genes and only load
	# annotations from our chromosome
	msg(c("Reading interval file for chromosome", chromo, "in dir", ivalsDir))
	ivals = if(ivalsFromDir) {
		 rangesFromIvalFile(ivalsDir, chromo)
	} else {
		 rangesFromBins(binWidth, chromo)
	}
	attach(ivals, name="ivals")
	# If there aren't any intervals to overlap, bail
	if(length(ivals[[1]]) == 0) {
		msg("Aborting because there are no ranges to overlap")
		detach("ivals")
		return(NULL)
	}
	ranges <- IRanges(start, end)
	
	# Extract read intervals from reads
	msg("Converting reads to ranges")
	readlens <- alns$SeqLen
	widths <- readlens
	widths <- pmin(readlens, maxAlgnInfluence)
	ori <- if(from3prime) { "+" } else { "-" }
	readbegins <- alns$ChrOff + if(fromMiddle) {
		# From the middle
		floor((readlens - widths + as.integer(alns$Orient == "-")) / 2)
	} else {
		# From either the 3' or the 5' end
		as.integer(alns$Orient == ori) * (readlens - widths)
	}
	readranges <- IRanges(readbegins, width=widths)
	
	# Update counters since we may abort soon if there are no overlaps
	counter("Alignments handled by Assign.R", nread)
	counter("Chromosome bins handled by Assign.R", 1)

	# Overlap read intervals with genomic intervals
	# Query = genomic intervals, Subject = reads
	msg("Finding overlaps")
	olaps <- if(ivalsFromDir || maxAlgnInfluence > 1) {
		counter("Calls to calcOlap", 1)
		calcOlap(readranges, ranges)
	} else {
		counter("Calls to calcBinOlap", 1)
		calcBinOlap(readranges, binWidth)
	}
	if(length(olaps) == 0 | nrow(olaps) == 0) {
		msg("Aborting because there are no overlaps")
		counter("No-overlap chromosome bins", 1)
		detach("ivals")
		return(NULL)
	}
	msg(c("Overlap matrix is", nrow(olaps), "rows by", ncol(olaps),"cols"))
	
	# Check each possible overlap and, where one exists, print an alignment
	# tuple; pri key = Lab, sec key = 1
	# Add 100,000 to the IvalOffs so that they're all positive; we have to
	# keep them positive and pad them with 0s in order for Hadoop to sort
	# them properly.
	msg("Outputting overlapping alignments")
	outdf <- cbind(Lab=alns$Lab[olaps[,2]],
	               Part="999999999",
	               alns[olaps[,2],4:8],
	               Ival=name[olaps[,1]],
	               IvalOff=formatC(readbegins[olaps[,2]], 10, flag="0"))
	write.table(outdf,
	            col.names=FALSE, row.names=FALSE, quote=FALSE, sep='\t')
	msg("Outputting per-label subtotals")
	
	# Grab just the labels for the overlapped reads
	labtab <- table(outdf$Lab)
	labtab <- cbind(row.names(labtab), labtab)
	
	# Make a table counting overlaps per-label-per-interval
	labIvalTab <- table(paste(outdf$Lab, "   ", outdf$Ival))
	
	# Remove the interval name from the key and pad the count with 0's
	# so that it can be sorted lexicographically by Hadoop
	labIvalTabRows <- row.names(labIvalTab)
	labIvalTab <- cbind(sub("   .*", "", labIvalTabRows), # just label
	                    formatC(labIvalTab, 8, flag="0"),
	                    sub(".*   ", "", labIvalTabRows)) # just gene id
	
	# Output a label subtotal for each label that appeared in an overlap
	# A secondary key of 0 is given so that the subtotals are
	# guaranteed to arrive at the next Reduce step before any of the
	# alignments (alignments have secondary key = 1)
	write.table(labIvalTab,
	            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="\t")
	
	msg("Finished outputting per-label subtotals")

	# Also update global counters
	counter(paste("Overlaps for label ", labtab[,1], sep=""), labtab[,2])
	
	# Print Hadoop counter updates
	counter("Overlaps output by Assign.R", nrow(olaps))
	detach("ivals")
}

# Split by chromosome, dispatch each chromosome bin to handleChr
invisible(lapply(alns.split, handleChr))
counter("Invocations of Assign.R", 1)
