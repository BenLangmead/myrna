#!/usr/bin/perl -w

##
# Postprocess.pl
#
# Author: Ben Langmead
#   Date: December 29, 2009
#
# Postprocess the status and call files for a cb-rna run.
#

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin); 
use lib $Bin;
use Counters;
use Get;
use Util;
use Tools;
use AWS;
use File::Path qw(mkpath);
use File::Basename;

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

sub msg($) {
	my $m = shift;
	print STDERR "PostprocessReduce.pl: $m\n";
}

# We want to manipulate counters before opening stdin, but Hadoop seems
# to freak out when counter updates come before the first <STDIN>.  So
# instead, we append counter updates to this list.
my @counterUpdates = ();

sub counter($) {
	my $c = shift;
	print STDERR "reporter:counter:$c\n";
}

sub flushCounters() {
	for my $c (@counterUpdates) { counter($c); }
	@counterUpdates = ();
}

my $ivalsjar = "";
my $dest_dir = "";
my $status = "";
my $calls = "";
my $exons = "";
my $output = "";
my $r = "";
my $r_arg = "";
my $Rfetch = "";
my $ivals = "";
my $counts = "";
my $resultsFn = "";
my $r_args = "--default-packages=grDevices,graphics,methods,utils,stats,IRanges,multicore,geneplotter";
my $minusLog = 0;
my $noGenes = 0;
my $noAlignments = 0;
my $cores = 1;
my $keep = 0;
my $cntfn = "";

if(defined($ENV{R_HOME})) {
	$r = "$ENV{R_HOME}/bin/Rscript";
	unless(-x $r) { $r = "" };
}
if($r eq "") {
	$r = `which Rscript 2>/dev/null`;
	chomp($r);
	unless(-x $r) { $r = "" };
}
if($r eq "" && -x "Rscript") {
	$r = "Rscript";
}

Tools::initTools();

my %args = (
	"ivalsjar:s"    => \$ivalsjar,
	"ivaljar:s"     => \$ivalsjar,
	"no-genes"      => \$noGenes,
	"minus-log"     => \$minusLog,
	"no-alignments" => \$noAlignments,
	"destdir:s"     => \$dest_dir,
	"s3cmd:s"       => \$Tools::s3cmd_arg,
	"s3cfg:s"       => \$Tools::s3cfg,
	"jar:s"         => \$Tools::jar_arg,
	"accessid:s"    => \$AWS::accessKey,
	"secretid:s"    => \$AWS::secretKey,
	"hadoop:s"      => \$Tools::hadoop_arg,
	"wget:s"        => \$Tools::wget_arg,
	"R:s"           => \$r_arg,
	"Rfetch:s"      => \$Rfetch,
	"ivals:s"       => \$ivals,
	"calls:s"       => \$calls,
	"exons:s"       => \$exons,
	"output:s"      => \$output,
	"counts:s"      => \$counts,
	"counters:s"    => \$cntfn,
	"cores:i"       => \$cores,
	"keep"          => \$keep,
	"status:s"      => \$status
);

my @args2 = ();
for my $k (keys %args) {
	push @args2, $k;
	my $nk = $k;
	$nk =~ s/[:=].*$//;
	my $val = $args{$k};
	defined($val) || die;
	delete $args{$k};
	$args{$nk} = $val;
}

GetOptions (\%args, @args2) || die "Bad option\n";

for my $k (sort keys %args) {
	defined($args{$k}) || die "Bad key: $k";
	msg("$k: ".${$args{$k}});
}

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

$resultsFn = "results.tar.gz" if $resultsFn eq "";

msg("ls -al");
msg(`ls -al`);

$output ne "" || die "Must specify -output\n";
$counts ne "" || die "Must specify -counts\n";
$ivals eq "" || -d $ivals || die "No such directory as -ivals \"$ivals\"\n";
$cores > 0 || die "-cores must be > 0 (was $cores)\n";

if($Rfetch ne "") {
	mkpath($dest_dir);
	(-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";
	msg("Ensuring R is installed");
	my $r_dir = "R-2.10.0";
	Get::ensureFetched($Rfetch, $dest_dir, \@counterUpdates, $r_dir);
	$ENV{RHOME} = "$dest_dir/$r_dir";
	if($r_arg ne "") {
		msg("Overriding old r_arg = $r_arg");
		msg("  with $dest_dir/R-2.10.0/bin/Rscript");
	}
	$r = "$dest_dir/R-2.10.0/bin/Rscript";
	(-x $r) || die "Did not extract an executable $r\n";
} else {	
	$r = $r_arg if $r_arg ne "";
	if(! -x $r) {
		$r = `which $r`;
		chomp($r);
		if(! -x $r) {
			if($r_arg ne "") {
				die "-R argument \"$r_arg\" doesn't exist or isn't executable\n";
			} else {
				die "R could not be found in R_HOME or PATH; please specify -R\n";
			}
		}
	}
}

$output =~ s/^S3N/s3n/;
$output =~ s/^S3/s3/;
$output =~ s/^HDFS/hdfs/;

if($ivalsjar ne "" && $exons eq "") {
	mkpath($dest_dir);
	(-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";
	msg("Ensuring reference jar is installed");
	Get::ensureFetched($ivalsjar, $dest_dir, \@counterUpdates);
	if($ivals ne "") {
		msg("Overriding old ivals = $ivals");
		msg("  with $dest_dir/ivals");
	}
	$ivals = "$dest_dir/ivals";
	(-d $ivals) || die "Postprocess.pl: Interval dir $ivals doesn't exist\n";
	#(-f "Postprocess.R" || die "Postprocess.pl: Postprocess.R doesn't exist or isn't readable");
	(-f "$ivals/exons.txt") || die "Postprocess.pl: $ivals/exons.txt doesn't exist\n";
	(-f "$ivals/genes.txt") || die "Postprocess.pl: $ivals/genes.txt doesn't exist\n";
	$exons = "$ivals/exons.txt";
} elsif($exons eq "") {
	$exons = "$ivals/exons.txt";
}

my $first = 1;
my $firstLine = <STDIN>;
unless(defined($firstLine)) {
	msg("No input, exiting gracefully");
	exit 0;
}

unless($noGenes) {
	system("cp $ivals/exons.txt .") == 0 || die "Could not copy '$ivals/exons.txt' to current directory";
	system("cp $ivals/genes*.txt .") == 0 || die "Could not copy '$ivals/genes*.txt' to current directory";
}

# Perl trim function to remove whitespace
sub trim($) {
	my $str = shift;
	$str =~ s/^\s+//;
	$str =~ s/\s+$//;
	return $str;
}

sub run($) {
	my $cmd = shift;
	msg("Postprocess.pl: Running \"$cmd\"");
	return system($cmd);
}

sub toStr($) {
	my $cnts = shift;
	my $str = "";
	for my $k (sort keys %{$cnts}) {
		$str .= " " if $str ne "";
		$str .= "$k:".$cnts->{$k};
	}
	return $str;
}

##
# Print a gene record to the call file and a pval record to the pval
# file.
#
sub reportGene {
	my ($gene, $pval, $pvfh) = @_;
	# If p-values are in -log format, convert back to fraction
	$pval = exp(-$pval) if $minusLog;
	my $pvalstr = sprintf("%1.6e", $pval);
	defined($pvalstr) || die;
	print {$pvfh} "$gene\t$pvalstr\n";
}

msg("2/7: Postprocessing calls (somewhat slow)");
my $pvalfh;
open $pvalfh, ">pvals.txt" || die "Could not open 'pvals.txt' for writing\n";

print {$pvalfh} "ensembl_gene_id\tp_value\n";

my $gene = "";
my $pval = "";
my $alfh = undef;
my $opened = 0;
my $lines = 0;
my $als = 0;
my $alsCnt = 0;
my %genes = ();
my $genesReported = 0;
my $prevLine = "(none)";
my $line;

# Write alignments to their respective files in the 'alignments'
# subdirectory.
mkpath("alignments");
while(1) {
	if($first) {
		$_ = $firstLine;
		$first = 0;
	} else {
		$prevLine = $line;
		$_ = <STDIN>;
	}
	$line = $_;
	last unless defined($line);
	next if $line =~ /^FAKE\s*$/;
	chomp;
	my @s = split;
	if(++$lines == 100000) {
		counter("Postprocessor,Lines processed,$lines");
		$lines = 0;
	}
	substr($s[1], -1) eq "G" || die "Internal gene name should end with G: $s[1]\n";
	$s[1] = substr($s[1], 0, -1);
	if($s[1] ne $gene) {
		# Report previous gene
		close($alfh) if $opened;
		$opened = 0;
		reportGene($gene, $pval, $pvalfh) if $gene ne "";
		$genesReported++;
		$#s == 2 || die "Expected a gene record with 3 fields; got:\n$_\nprev:\n$prevLine\n";
		$s[2] eq "0" || die "Expected a gene record with 0 in 3rd col; got:\n$_\nprev:\n$prevLine\n";
		($pval, $gene) = ($s[0], $s[1]);
	} else {
		$noAlignments && die "-no-alignments was specified, but saw an alignment:\n$_\n";
		if(++$alsCnt == 10000) {
			counter("Postprocessor,Alignments processed,$alsCnt");
			$alsCnt = 0;
		}
		$pval eq $s[0] || die;
		$als++;
		$#s == 8 || die "Expected an alignment record with 9 fields, got:\n$_\nprev:\n$prevLine\n";
		unless($opened) {
			open $alfh, ">alignments/$gene.txt";
			$opened = 1;
		}
		# print "$pval\t$ival\t$ivalOff\t$orient\t$seqLen\t$oms\t$mate\t$cigar\t$lab\n";
		print $alfh "$s[8]\t".int($s[2])."\t$s[3]\t$s[4]\t$s[5]\t$s[6]\t$s[7]\n";
	}
}
reportGene($gene, $pval, $pvalfh) if $gene ne "";
$genesReported++;
close($pvalfh);
counter("Postprocessor,Lines processed,$lines");
counter("Postprocessor,Alignments processed,$alsCnt");
counter("Postprocessor,Intervals reported,$genesReported");

# Create a gene length map by scanning the exons.txt file; this will
# also serve as a gene set
if($exons ne "" && !$noGenes) {
	msg("3/7: Extracting gene ids from $exons");
	open(EXONS, "$exons") || die "Count not open $exons for reading\n";
	my @gene_names = ();
	while(<EXONS>) {
		my @s = split;
		my ($name, $begin, $end) = ($s[0], $s[4], $s[5]);
		next if $name eq "ensembl_gene_id";
		push(@gene_names, $name) unless defined($genes{$name});
		$genes{$name} += (($end - $begin)+1);
	}
	close(EXONS);
	counter("Postprocessor,Annotated genes from exons.txt,".scalar(keys %genes));
	open (LENS, ">gene_lengths.txt") || die "Could not open gene_lengths.txt for writing\n";
	for my $n (@gene_names) {
		print LENS "$n\t$genes{$n}\n";
	}
	close(LENS);
} else {
	msg("SKIPPING 3/7: Extracting gene ids from $exons");
}

# Get the set of all genes with non-0 counts
if(!Util::is_local($counts)) {
	Get::ensureDirFetched($counts, $dest_dir, \@counterUpdates);
} else {
	$dest_dir = $counts;
}

msg("4/7: Extracting per-gene, per-label counts from $dest_dir");
my %ival_labs = ();
my %labs = ();
my %norms = ();

# Now construct matrix of counts per gene/label
my @count_files = <$dest_dir/*.txt>;
msg("Found ".scalar(@count_files)." count files");
for my $f (@count_files) {
	msg("  processing counts file $f");
	my $bf = fileparse($f);
	defined($bf) || die "Undefined basename of count file name '$f'";
	$bf =~ /\.txt$/ || die "Basename should have ended in .txt: '$bf'";
	$bf =~ s/\.txt$//;
	$labs{$bf} = 0;
	open(CNT, "$f") || die "Could not open $f for reading\n";
	my $first = 1;
	while(<CNT>) {
		if($first) {
			# skip header
			$first = 0; next;
		}
		chomp;
		my @s = split; # $s[0] = gene name, s[1] = count
		$ival_labs{$s[0]}{$bf} = $s[1];
		$labs{$bf} += $s[1];
	}
	close(CNT);
	counter("Postprocessor,Genes with at least 1 non-zero count,".scalar(keys %ival_labs));
}

# Now construct a vector of normalization factors
my @norm_files = <$dest_dir/*.norm>;
for my $f (@norm_files) {
	msg("  processing normalization-factor file $f");
	my $bf = fileparse($f);
	$bf =~ s/\.norm$//;
	open(NORM, $f) || die "Could not open $f for reading\n";
	my $n = <NORM>;
	chomp($n);
	$norms{$bf} = $n;
	close(NORM);
}

# Grab all the per-sample count & normalization-factor files
for my $ty ("txt", "norm", "norms") {
	my @fs = <$dest_dir/*.$ty>;
	next if scalar(@fs) == 0;
	system("cp $dest_dir/*.$ty .") == 0 || die "Error running cp $dest_dir/*.$ty .\n";
}

if($noGenes) { for my $k (keys %ival_labs) { $genes{$k} = 1; } }

open (COUNTS, ">count_table.txt") || die "Could not open count_table.txt for writing\n";
open (RPKM, ">rpkm_table.txt") || die "Could not open rpkm_table.txt for writing\n";
msg("5/7: Writing count_table.txt and rpkm_table.txt");

my @ls = sort keys %labs;
my @gs = sort keys %genes;

#scalar(@ls) > 0 || die "No labels!\n";
#scalar(@gs) > 0 || die "No genes!\n";

# Print column names
for(my $li = 0; $li <= $#ls; $li++) {
	print COUNTS "\t" if $li > 0;
	print COUNTS $ls[$li];
	print RPKM "\t" if $li > 0;
	print RPKM $ls[$li];
}
print COUNTS "\n";
print RPKM "\n";
# For each gene
for(my $gi = 0; $gi <= $#gs; $gi++) {
	print COUNTS $gs[$gi]; # row name = gene name
	print RPKM $gs[$gi]; # row name = gene name
	# For each label
	for(my $li = 0; $li <= $#ls; $li++) {
		# row elements = counts for this gene
		my $el = $ival_labs{$gs[$gi]}{$ls[$li]};
		if(defined($el)) {
			defined($norms{$ls[$li]}) || die;
			defined($genes{$gs[$gi]}) || die;
			$genes{$gs[$gi]} > 0 || die;
			my $rpm = $el * 1000000.0 / $labs{$ls[$li]};
			my $RPKM = $rpm * 1000.0 / $genes{$gs[$gi]};
			print COUNTS "\t$el";
			print RPKM "\t$RPKM";
		} else {
			print COUNTS "\t0";
			print RPKM "\t0";
		}
	}
	print COUNTS "\n";
	print RPKM "\n";
}
close(COUNTS);
close(RPKM);

# Make plots
my $ret = 0;
if($als > 0 && !$noGenes) {
	# TODO: still make plots when $noGenes is set, ignoring exons
	msg("6/7: Making plots");
	$noGenes || -f "exons.txt" || die;
	my $cmd = "$r $r_args $Bin/Postprocess.R --args $cores";
	msg("$cmd");
	$ret = system($cmd);
} else {
	msg("SKIPPING 6/7: No alignments or no exons");
}

# Push results
msg("7/7: Pushing results");
my $aldir = $noAlignments ? "" : "alignments";

my @fs = <*.txt>;
push @fs, <*.norm>;
push @fs, <*.norms>;
push @fs, <pval_hist*.pdf>;
push @fs, <pval_scatter*.pdf>;
push @fs, <qval_hist*.pdf>;
push @fs, <qval_scatter*.pdf>;
push @fs, $aldir if $als > 0;
my $tarargs = join(' ', @fs);

system("tar cvf - $tarargs | gzip -c > results.tar.gz");
$output .= "/" unless $output =~ /\/$/;
system("touch FAILED") if $ret != 0;
if($output =~ /^s3/i) {
	Get::do_s3_put("results.tar.gz", $output, \@counterUpdates);
	Get::do_s3_put("FAILED", $output, \@counterUpdates) if $ret != 0;
} elsif($output =~ /^hdfs/i) {
	Get::do_hdfs_put("results.tar.gz", $output, \@counterUpdates);
	Get::do_hdfs_put("FAILED", $output, \@counterUpdates) if $ret != 0;
} else {
	mkpath($output);
	(-d $output) || die "Could not make output directory: $output\n";
	run("cp results.tar.gz $output") == 0 || die;
	run("cp FAILED $output") if $ret != 0;
}

if($ret != 0) {
	die "Command returned $ret; aborting from PostprocessReduce.R\n";
}

unless($keep) {
	system("rm -f FAILED pvals.txt qvals.txt results.tar.gz exons.txt genes*.txt pval_hist*.pdf pval_scatter.pdf qval_hist*.pdf");
	system("rm -fr alignments");
}
flushCounters();
print "FAKE\n";
