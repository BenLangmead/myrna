##
# Ensembl.R
#
# Author: Ben Langmead
#   Date: December 15, 2009
#
# Get an Ensembl table via BioMart encoding exons of the target
# species, then perform set operations to condition the per-gene
# information as desired.
#
#library(biomaRt)
#library(IRanges)

# Print command-line args
args <- commandArgs(T)
mart       <- args[2]
organism   <- args[3]
dataset    <- args[4]
ftp.base   <- args[5]
mask.fasta <- as.logical(as.integer(args[6]))
rep.mask   <- as.logical(as.integer(args[7]))
incl.chrs  <- args[8]
excl.chrs  <- args[9]

# Helper to write a message to stderr
log <- function(...) {
	sink(stderr())
	cat("Ensembl.R [")
	cat(format(Sys.time(), "%Hh:%Mm:%Ss"))
	cat("]: ")
	cat(...)
	cat('\n')
	flush(stderr())
	sink()
}

log(c("organism:",        organism))

log("1/10. Connecting to Ensembl via biomaRt")
ensembl = useMart(mart, dataset=dataset)
if(!exists("ensembl")) {
	log("Bad organism:", organism) ; q('no')
}

log("2/10. Getting table of exons (somewhat slow)")
exons <- getBM(attributes=c(
	"ensembl_gene_id",
	"ensembl_transcript_id",
	"ensembl_exon_id",
	"chromosome_name",
	"exon_chrom_start",
	"exon_chrom_end",
	"gene_biotype"), mart=ensembl)

# Write exons
write.table(exons, file="ivals/exons.txt",
col.names=TRUE, row.names=FALSE, quote=FALSE, sep="\t")

##
# Courtesy of:
# http://www.mail-archive.com/bioc-sig-sequencing@r-project.org/msg01135.html
#
writeFASTA <- function(x, file="", desc=NULL, append=FALSE, width=80)
{
	if (!isTRUEorFALSE(append))
		stop("'append' must be TRUE or FALSE")
	if (isSingleString(file)) {
		if (file == "") {
			file <- stdout()
		} else {
			file <- file(file, ifelse(append, "a", "w"))
			on.exit(close(file))
		}
	} else if (inherits(file, "connection")) {
		if (!isOpen(file)) {
			file <- file(file, ifelse(append, "a", "w"))
			on.exit(close(file))
		}
	} else {
		stop("'file' must be a single string or connection")
	}
	if (!isSingleNumber(width))
		stop("'width' must be an integer >= 1")
	if (!is.integer(width))
		width <- as.integer(width)
	if (width < 1L)
		stop("'width' must be an integer >= 1")
	if(!(is.character(x) || is(x, "XString") || is(x, "XStringSet") ||
		 is(x, "BSgenome") || (is.list(x) && "seq" %in% names(x))))
		stop("'x' does not have the appropriate type")
	#browser()
	if(is.character(x))
	x <- BStringSet(x, use.names = TRUE)
	if(is.list(x)) {
		if(is.null(desc))
		desc <- x$desc
		x <- BStringSet(x$seq)
	}
	if(is(x, "XString")) {
		nLengths <- length(x)
	}
	if(is(x, "XStringSet")) {
		nLengths <- width(x)
	}
	if(is(x, "BSgenome")) {
		nLengths <- seqlengths(x)
	}
	if (!is.null(desc) && !(is.character(desc) && length(desc) == length(nLengths)))
		stop("when specified, 'desc' must be a character vector of the same length as the 'x' object")
	if(is.null(desc))
		desc <- names(x)
	if(is.null(desc))
		desc <- rep("", length(nLengths))
	if(length(nLengths) != length(desc))
		stop("wrong length of 'desc'")
	writeBString <- function(bstring)
	{
		if (length(bstring) == 0L)
			return()
		nlines <- (length(bstring) - 1L) %/% width + 1L
		lineIdx <- seq_len(nlines)
		start <- (lineIdx - 1L) * width + 1L
		end <- start + width - 1L
		if (end[nlines] > length(bstring))
			end[nlines] <- length(bstring)
		bigstring <- paste(
			as.character(Views(bstring, start = start, end = end)),
			collapse="\n")
		cat(bigstring, "\n", file=file, sep="")
	}
	if(is(x, "XString")) {
		cat(">", desc, "\n", file = file, sep = "")
		writeBString(x)
	} else {
		for (ii in seq_len(length(nLengths))) {
			cat(">", desc[ii], "\n", file = file, sep = "")
			writeBString(x[[ii]])
		}
	}
}

log("3/10. Getting, masking, writing FASTA")
keyizeFastaSeqs <- function(x) {
	res <- list()
	for(i in 1:length(x)) {
		name <- sub(" .*", "", x[[i]]$desc)
		res[[i]] <- x[[i]]$seq
		names(res)[i] <- name
	}
	res
}
keyizeFastaNames <- function(x) {
	res <- list()
	for(i in 1:length(x)) {
		name <- sub(" .*", "", x[[i]]$desc)
		res[[i]] <- x[[i]]$desc
		names(res)[i] <- name
	}
	res
}

dna.base <- if(rep.mask) {"dna_rm"} else {"dna"}
non.chromosomal.pattern = "GL.*";
to.index <- NULL
got.non.chromosomal <- F
if(mask.fasta) {
	for(i in unique(exons$chromosome_name)) {
		append <- F
		ofn <- paste("/tmp/", organism, "_", i, ".fa.gz", sep="")
		if(grepl(non.chromosomal.pattern, i)) {
			ofn <- paste("/tmp/", organism, "_nonchromosomal.fa.gz", sep="")
			if(!got.non.chromosomal) {
				ifn <- paste(ftp.base, dna.base, ".nonchromosomal.fa.gz", sep="")
				got.non.chromosomal <- T
				download.file(ifn, ofn, "auto")
				# Truncate
			} else {
				append <- T
			}
		} else {
			ifn <- paste(ftp.base, dna.base, ".chromosome.", i, ".fa.gz", sep="")
			download.file(ifn, ofn, "auto")
			# Truncate
		}
		fa <- readFASTA(gzfile(ofn), strip.descs=T)
		fa.seq <- keyizeFastaSeqs(fa)
		fa.name <- keyizeFastaNames(fa)
		if(mask.fasta) {
			pexons <- exons[exons$gene_biotype == "pseudogene" & exons$chromosome_name == i,]
			widths <- pexons$exon_chrom_end - pexons$exon_chrom_start + 1
			if(length(widths) > 0) {
				for(j in 1:length(widths)) {
					if(widths[j] <= 0) {
						log(c("Warning, found widths of ",widths[j]))
						continue
					}
					# Sanity check to make sure our range doesnt fall off the end
					subseq(fa.seq[[i]], start=pexons$exon_chrom_start[j], end=pexons$exon_chrom_end[j]) = paste(rep("N", widths[j]), collapse="")
				}
			}
		}
		ofn2 <- sub(".fa.gz", ".post.fa", ofn)
		if(!is.null(to.index)) {
			to.index <- paste(to.index, ofn2, sep=",")
		} else {
			to.index <- ofn2
		}
		writeFASTA(fa.seq[[i]], desc=fa.name[[i]], file=ofn2, width=60, append=append)
		log(c(ifelse(append, "      Appended to ", "      Wrote "), ofn2))
	}
}

exons$ensembl_exon_id <- NULL

log("4/10. Getting table of genes")
genes <- getBM(attributes=c(
	"ensembl_gene_id",
	"external_gene_id",
	"chromosome_name",
	"start_position",
	"end_position",
	"strand",
	"gene_biotype"), mart=ensembl)

# Write genes
write.table(genes, file="ivals/genes.txt",
            col.names=TRUE, row.names=FALSE, quote=FALSE, sep="\t")

log("5/10. Getting tables of go terms (somewhat slow)")
genes.go <- getBM(attributes=c(
	"ensembl_gene_id",
	"go_biological_process_id"), mart=ensembl)

# Write genes-to-GO for biological process
write.table(genes.go, file="ivals/genes_go_bproc.txt",
            col.names=TRUE, row.names=FALSE, quote=FALSE, sep="\t")

genes.go <- getBM(attributes=c(
	"ensembl_gene_id",
	"go_cellular_component_id"), mart=ensembl)

# Write genes-to-GO for cellular component
write.table(genes.go, file="ivals/genes_go_ccomp.txt",
            col.names=TRUE, row.names=FALSE, quote=FALSE, sep="\t")

genes.go <- getBM(attributes=c(
	"ensembl_gene_id",
	"go_molecular_function_id"), mart=ensembl)

# Write genes-to-GO for molecular function
write.table(genes.go, file="ivals/genes_go_mfunc.txt",
            col.names=TRUE, row.names=FALSE, quote=FALSE, sep="\t")

##
# Take a data frame of exons from Ensembl (must have $ensembl_gene_id,
# $ensembl_transcript_id, $exon_chrom_start, $exon_chrom_end) and
# return a matrix with columns for chromosome name, gene id, start pos,
# end pos.
#
unExonByGene <- function(exons.by.gene, omit.by.chr) {
	unGeneList <- function(gene, omit.by.chr) {
		if(length(gene$chromosome_name) == 0) {
			return(NULL)
		}
		unn <- reduce(IRanges(gene$exon_chrom_start, gene$exon_chrom_end))
		chr <- gene$chromosome_name[1]
		unn.filt <- if(length(omit.by.chr) > 0 && length(omit.by.chr[[chr]]) > 0) {
			setdiff(unn, omit.by.chr[[chr]])
		} else { unn }
		if(length(unn.filt) > 0) {
			cbind(gene$chromosome_name[1], gene$ensembl_gene_id[1],
			      start(unn.filt), end(unn.filt))
		} else { NULL }
	}
	un <- do.call(rbind, lapply(exons.by.gene, function(x) { unGeneList(x, omit.by.chr) }))
	colnames(un) <- c("chr", "gene", "start", "end")
	un
}

exons.pcode <- subset(exons, exons$gene_biotype == "protein_coding")
exons.pcode.by.gene <- split(exons.pcode, exons.pcode$ensembl_gene_id)

log("6/10. Calculating Union interval models (slow)")
exons.union <- unExonByGene(exons.pcode.by.gene, list())

log("7/10. Calculating gene overlaps")
overlapChr <- function(ranges) {
	ir <- IRanges(ranges$start, ranges$end)
	IRanges(coverage(ir) > 1)
}

exons.union.fr <- data.frame(
	exons.union[,1], as.integer(exons.union[,3]), as.integer(exons.union[,4]))
colnames(exons.union.fr) <- c("chr", "start", "end")
olap.by.chr <- lapply(split(exons.union.fr, exons.union.fr $chr), overlapChr)
olap.frame <- data.frame(
	do.call(rbind, lapply(names(olap.by.chr), function(x) {
		if(length(olap.by.chr[[x]]) > 0) {
			cbind(x, start(olap.by.chr[[x]]), end(olap.by.chr[[x]]))
		} else { NULL }
	})))
colnames(olap.frame) <- c("chr", "start", "end")

write.table(cbind("gene_olaps", olap.frame),
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="\t")

log("8/10. Calculating filtered Union interval models (slow)")
exons.union.filt <- unExonByGene(exons.pcode.by.gene, olap.by.chr)
write.table(cbind("un", exons.union.filt),
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="\t")

##
# Input: a list mapping gene ids data.frames of exon records from Ensembl.
# Assumes Ensembl column names ($ensembl_gene_id, $ensembl_transcript_id,
# $exon_chrom_start, $exon_chrom_end).  Returns: a matrix with columns for
# chromosome name, gene id, start pos, end pos.
#
uiExonByGene <- function(exons.by.gene, omit.by.chr) {
	uiGeneList <- function(gene) {
		if(length(gene$chromosome_name) == 0) { return(NULL) }
		tr <- split(gene, gene$ensembl_transcript_id)
		tr.ir <- lapply(tr, function(x) { IRanges(x$exon_chrom_start, x$exon_chrom_end) })
		ui.ranges <- Reduce(intersect, tr.ir)
		chr <- gene$chromosome_name[1]
		ui.ranges.filt <- if(length(omit.by.chr) > 0 && length(omit.by.chr[[chr]]) > 0) {
			setdiff(ui.ranges, omit.by.chr[[chr]])
		} else { ui.ranges }
		if(length(ui.ranges.filt) > 0) {
			cbind(gene$chromosome_name[1], gene$ensembl_gene_id[1],
			      start(ui.ranges.filt), end(ui.ranges.filt))
		} else { NULL }
	}
	ui.by.gene <- lapply(exons.by.gene, uiGeneList)
	ui <- do.call(rbind, ui.by.gene)
	colnames(ui) <- c("chr", "gene", "start", "end")
	ui
}

# Union-intersection of all protein-coding exons
log("9/10. Calculating filtered Union-intersection interval models (slow)")
exons.ui.filt <- uiExonByGene(exons.pcode.by.gene, olap.by.chr)
write.table(cbind("ui", exons.ui.filt),
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="\t")

log("10/10. Done")
log(c("bowtie-build ",to.index))
