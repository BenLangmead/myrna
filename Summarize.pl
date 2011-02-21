#!/usr/bin/perl -w

##
# Summarize.pl
#
# Author: Ben Langmead
#   Date: December 1, 2009
#
# Takes a sorted list of all tuples output from Stats.R, which includes
# both predictions and alignments, and extracts and adds P values
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

my $firstline = <STDIN>;
if(!defined($firstline)) {
	print STDERR "Summarize.pl: No input\n";
	exit 0;
}

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

sub counter($) {
	my $c = shift;
	print STDERR "reporter:counter:Summarize,$c\n";
}

my $prefix = "Summarize.pl: ";
sub msg($) {
	my $m = shift;
	print STDERR "$prefix$m\n";
}

my $top = 0;
my $nullsPerPval = 0;
my $chosenUrl = "";
my $cntfn = "";

Tools::initTools();

GetOptions (
	"accessid:s"    => \$AWS::accessKey,
	"secretid:s"    => \$AWS::secretKey,
	"hadoop:s"      => \$Tools::hadoop_arg,
	"top:i"         => \$top,
	"chosen-genes:s"=> \$chosenUrl,
	"counters:s"    => \$cntfn,
	"nulls:i"       => \$nullsPerPval) || die "Bad option\n";

$chosenUrl ne "" || die "Must specify -chosen-genes\n";
$chosenUrl =~ s/^S3N/s3n/;
$chosenUrl =~ s/^S3/s3/;
$chosenUrl =~ s/^HDFS/hdfs/;

$top = 100 if $top == 0;

msg("Capturing alignments for top $top genes");
msg("Null statistics per gene: $nullsPerPval");
msg("Chosen URL: $chosenUrl");

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

my @tops = ();
my @pvals = ();

my $allPvals = 0;
my $obsPvals = 0;
my $nullPvals = 0;
my $obsPvalsCnt = 0;
my $nullPvalsCnt = 0;
my $als = 0;
my $alsCnt = 0;
my $keep = 0;

my $tmpfn = ".Summarize.pl.$$.tmp";
my $tmpfh;
open $tmpfh, ">$tmpfn" || die "Could not open $tmpfn for writing\n";
my $first = 1;

counter("Summarize.pl invocations,1");
msg("First line");
msg("$firstline");

my $prevLine = "(none)";
my $lastPval = 0.0;
while(1) {
	$prevLine = $_;
	if($first) {
		$_ = $firstline;
		$first = 0;
	} else {
		$_ = <STDIN>;
	}
	next if $_ =~ /^FAKE\s*$/;
	unless(defined($_)) {
		msg("Got last line of input");
		last;
	}
	chomp;
	my @s = split;
	$s[0] eq '1' || die "Expected 1 in first column:\n$_\n";
	my $pval = $s[1];
	my ($type, $gene) = ($s[2], $s[3]);
	my $run = 0;
	# This is a P-val tuple
	$type eq "N" || $type eq "O" || die "Bad type for statistic record: $type\n$_\n";
	$allPvals++;
	$pval >= $lastPval || $pval eq "Inf" || die "$lastPval preceded $pval";
	$run = 0 if $pval > $lastPval;
	if($type eq "O") {
		$#s == 3 || die "Expected exactly 4 fields for observed P-val;\n$_\n";
		$obsPvals++;
		if(++$obsPvalsCnt >= 1000) {
			counter("Pvals (observed) processed,$obsPvalsCnt");
			$obsPvalsCnt = 0;
		}
	} else {
		$#s == 2 || die "Expected exactly 3 fields for null P-val;\n$_\n";
		$nullPvals++;
		$run++;
		if(++$nullPvalsCnt >= 1000) {
			counter("Pvals (null) processed,$nullPvalsCnt");
			$nullPvalsCnt = 0;
		}
		next;
	}
	# Observed statistic/pvalue
	$lastPval = $pval;
	# Thread *all* pvals through, but not all alignments
	if($nullsPerPval > 0) {
		my $np = $nullPvals;
		$run <= $np || die;
		# Resolve ties by sticking the observed statistic somewhere in the middle
		$np -= rand($run) if $run > 1;
		print $tmpfh "$np\t$gene\n";
	} else {
		scalar(@tops) <= $top || die;
		scalar(@pvals) <= $top || die;
		# Ultimately we want the *last* top -log p's
		shift @tops if scalar(@tops) == $top;
		shift @pvals if scalar(@pvals) == $top;
		push @tops, $gene;
		push @pvals, "$pval";
		# NOTE: pval is really -log(pval)
		print "$pval\t$gene\n";
	}
}

if($nullsPerPval > 0) {
	# Just finished wth P-vals so now we need to calculate P-vals
	# from the permutation test
	$nullPvals == $obsPvals * $nullsPerPval ||
		die "$obsPvals observed P-values, $nullPvals null P-values, but $nullsPerPval nulls per gene\n";
	close($tmpfh);
	open $tmpfh, "$tmpfn" || die "Couldn't open $tmpfn for reading\n";
	my $lastPval = 1.0;
	while(<$tmpfh>) {
		chomp;
		my @fs = split(/\t/, $_);
		$#fs == 1 || die;
		my ($priorNulls, $gene) = @fs;
		$priorNulls <= $nullPvals || die;
		my $pval = ($nullPvals - $priorNulls) / $nullPvals;
		$pval <= $lastPval || die "P-value $pval was greater than previous $lastPval\n";
		my $pvalStr = sprintf "%020.10f", $pval;
		scalar(@tops) <= $top || die;
		scalar(@pvals) <= $top || die;
		if(scalar(@tops) == $top) {
			shift @tops;
			shift @pvals;
		}
		push @tops, $gene;
		push @pvals, "$pvalStr";
		$lastPval = $pval;
		print "$pvalStr\t$gene\n";
	}
	close($tmpfh);
}
system("rm -f $tmpfn") unless $keep;

# Finally, push information about the chosen genes to a filesystem
my @tmps = ();
open(GENES, ">chosen_genes.txt") ||
	die "${prefix}Fatal Error: Could not open chosen_genes.txt for writing\n";
for(my $i = 0; $i <= $#tops; $i++) {
	my ($gene, $pval) = ($tops[$i], $pvals[$i]);
	print GENES "$gene\t$pval\n";
}
close(GENES);
Get::fs_put("chosen_genes.txt", "$chosenUrl");
unlink("chosen_genes.txt") unless($keep);

msg("Pvals processed: $allPvals");
msg("Pvals (observed) processed: $obsPvals");
msg("Pvals (null) processed: $nullPvals");
msg("P-values chosen: ".scalar(@tops));

counter("Pvals processed,$allPvals");
counter("Pvals (observed) processed,$obsPvals");
counter("Pvals (null) processed,$obsPvals");
counter("Chosen P values,".scalar(@tops));
print "FAKE\n";
