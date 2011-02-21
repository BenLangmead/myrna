#!/usr/bin/perl -w

##
# PostprocessMap.pl
#
# Author: Ben Langmead
#   Date: March 8, 2010
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

my $first = 1;
my $firstLine = <STDIN>;
unless(defined($firstLine)) {
	print STDERR "No input, exiting gracefully\n";
	exit 0;
}

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

my $prefix = "PostprocessMap.pl: ";
sub msg($) {
	my $m = shift;
	print STDERR "$prefix$m\n";
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

my $dest_dir = "";
my $noAlignments = 0;
my $chosenUrl = "";
my $cntfn = "";

Tools::initTools();

GetOptions (
	"s3cmd:s"       => \$Tools::s3cmd_arg,
	"s3cfg:s"       => \$Tools::s3cfg,
	"jar:s"         => \$Tools::jar_arg,
	"accessid:s"    => \$AWS::accessKey,
	"secretid:s"    => \$AWS::secretKey,
	"hadoop:s"      => \$Tools::hadoop_arg,
	"wget:s"        => \$Tools::wget_arg,
	"destdir:s"     => \$dest_dir,
	"counters:s"    => \$cntfn,
	"chosen-genes:s"=> \$chosenUrl,
	"no-alignments" => \$noAlignments) || die "Bad option\n";

$chosenUrl ne "" || die "Must specify -chosen-genes\n";
$chosenUrl =~ s/^S3N/s3n/;
$chosenUrl =~ s/^S3/s3/;
$chosenUrl =~ s/^HDFS/hdfs/;
$chosenUrl =~ s/\/*$//; # Remove trailing slash(es)

msg("Alignments ditched?: $noAlignments");
msg("Chosen genes URL: $chosenUrl");
msg("Dest dir: $dest_dir");
msg("s3cmd: found: $Tools::s3cmd, given: $Tools::s3cmd_arg");
msg("jar: found: $Tools::jar, given: $Tools::jar_arg");
msg("hadoop: found: $Tools::hadoop, given: $Tools::hadoop_arg");
msg("wget: found: $Tools::wget, given: $Tools::wget_arg");
msg("s3cfg: $Tools::s3cfg");
msg("Contents of directory:");
msg("ls -al");
msg(`ls -al`);

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

##
# Parse an alignment output by the Normal stage.
#
sub parseNormalAlignment() {
	my $h = shift;
	my $s = shift;
	scalar(@{$s}) == 9 || die;
	($h->{ival},  $h->{ioff}, $h->{fw},
	 $h->{len},   $h->{oms},  $h->{mate},
	 $h->{cigar}, $h->{lab},  $h->{norm}) = @{$s};
}

# Get the set of all genes with non-0 counts
my $chosenDir = $chosenUrl;
if(!Util::is_local($chosenUrl)) {
	$dest_dir ne "" || die "-chosen-genes is non-local, but -destdir is not specified\n";
	mkpath("$dest_dir/chosen");
	(-d "$dest_dir/chosen") || die "Could not create directory: $dest_dir/chosen\n";
	Get::ensureFetched("$chosenUrl/chosen_genes.txt", "$dest_dir/chosen", \@counterUpdates);
	$chosenDir = "$dest_dir/chosen";
}

my %chosenGenes = ();
my %unobservedGenes = ();

(-f "$chosenDir/chosen_genes.txt") ||
	die "chosen_genes.txt file should be present in $chosenDir but isn't";

open(CHOSEN, "$chosenDir/chosen_genes.txt") ||
	die "${prefix}Fatal error: Could not open chosen_genes.txt for reading\n";
while(<CHOSEN>) {
	chomp;
	next if $_ =~ /^\s*$/;
	my ($gene, $pval) = split(/\t/);
	(defined($gene) && defined($pval)) ||
		die "${prefix}Fatal Error: failed to parse chosen-gene file line: \"$_\"";
	$chosenGenes{$gene} = $pval;
	$unobservedGenes{$gene} = $pval;
	msg("Chose gene \"$gene\" with P-value (or statistic) \"$pval\"");
}
close(CHOSEN);

counter("Postprocess map,Chose ".(scalar(keys %chosenGenes))." genes,1");

my ($als, $alsCnt, $alsPass, $alsPassCnt, $alsFilt, $alsFiltCnt, $pvs, $pvsCnt) = (0, 0, 0, 0, 0, 0, 0, 0);
my $f1len = 0;
while(1) {
	if($first) {
		$_ = $firstLine;
		$first = 0;
	} else {
		$_ = <STDIN>;
	}
	next if $_ =~ /^FAKE\s*$/;
	unless(defined($_)) { msg("Got last line of input"); last; }
	chomp;
	# Record is either an alignment from Stats.pl or a P-value
	my @s = split(/[\t]/);
	my $pval = undef;
	if(scalar(@s) == 9) {
		# It's an alignment
		$noAlignments && die "-no-alignments specified, but there was at least 1 alignment:\n$_\n";
		$als++;
		if(++$alsCnt >= 10000) {
			counter("Postprocess map,Alignments processed,$alsCnt");
			$alsCnt = 0;
		}
		my ($ival, $ivalOff, $orient, $seqLen, $oms, $mate, $cigar, $lab, $norm) = @s;
		delete $unobservedGenes{$ival} if defined($unobservedGenes{$ival});
		$pval = $chosenGenes{$ival};
		if(defined($pval)) {
			$alsPass++;
			if(++$alsPassCnt >= 10000) {
				counter("Postprocess map,Alignments allowed through,$alsPassCnt");
				$alsPassCnt = 0;
			}
			print "$pval\t$ival\t$ivalOff\t$orient\t$seqLen\t$oms\t$mate\t$cigar\t$lab\n";
		} else {
			$alsFilt++;
			if(++$alsFiltCnt >= 10000) {
				counter("Postprocess map,Alignments filtered out,$alsFiltCnt");
				$alsFiltCnt = 0;
			}
		}
	} else {
		scalar(@s) == 2 || die "Expected either a 9-field alignment or a 2-field P-value:\n$_\n";
		$pvs++;
		if(++$pvsCnt >= 10000) {
			counter("Postprocess map,P-values processed,$pvsCnt");
			$pvsCnt = 0;
		}
		# It's a P-value
		my $ival = $s[1];
		$pval = $s[0];
		print "$pval\t$ival\t0\n";
	}
	if($f1len == 0 && defined($pval)) {
		$f1len = length("$pval");
	} elsif(defined($pval)) {
		$f1len == length("$pval") || die "Not all field-1 pvals are the same length\n$_\nPval: $pval\n";
	}
}

msg("Alignments processed: $als");
msg("Alignments allowed through: $alsPass");
msg("Alignments filtered out: $alsFilt");
msg("P-values processed: $pvs");

counter("Postprocess map,Alignments processed,$alsCnt");
counter("Postprocess map,Alignments allowed through,$alsPassCnt");
counter("Postprocess map,Alignments filtered out,$alsFiltCnt");
counter("Postprocess map,P-values processed,$pvsCnt");

flushCounters();
print "FAKE\n";
