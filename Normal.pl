#!/usr/bin/perl -w

##
# Normal.pl
#
# Author: Ben Langmead
#   Date: November 5, 2009
#
# Add up subtotals for a given label, then add label total field to the
# outgoing tuples.  Also, promite Ival and IvalOff to first and second
# field slots so that they can be primary and secondary keys.
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

my $lastLab = "";
my $als = "";
my $labTot = 0;
my $ignoreBelow = 1; # exclude counts below this floor from quantiles
my %labsSeen = ();
my $all9s = "999999999";
my $normal = "lup";
my $normal_url = "lup";
my $output_url = "";
my $cntfn = "";

Tools::initTools();

sub msg($) {
	my $m = shift;
	$m =~ s/[\r\n]*$//;
	print STDERR "Normal.pl: $m\n";
}

GetOptions (
	"counters:s" => \$cntfn,
	"normal:s" => \$normal,
	"normal_url:s" => \$normal_url,
	"hadoop:s" => \$Tools::hadoop_arg,
	"output:s" => \$output_url) || die "Bad option\n";

$output_url = "/myrna/output/counts" if $output_url eq "";
$output_url =~ s/^S3N/s3n/;
$output_url =~ s/^S3/s3/;
$output_url =~ s/^HDFS/hdfs/;

msg("Normalization: $normal");
msg("Output URL:    $output_url");
msg("Counters file: $cntfn");

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages\n");

sub validNormal {
	my $n = shift;
	return $n eq "lup" || $n eq "llow" || $n eq "lmed" || $n eq "ltot" || $n eq "max";
}
validNormal($normal) || die "Invalid normalization type: $normal\n";

my %cnthash = (); # read counts
my ($q25, $q50, $q75, $mx) = (0, 0, 0, 0); # middle quartiles & max
my $totals = 0;
my $totlabs = 0;
my $lines = 0;

sub descendingCnt { $cnthash{$b} <=> $cnthash{$a}; }
sub ascendingCnt  { $cnthash{$a} <=> $cnthash{$b}; }

my $norm = undef;
while(<STDIN>) {
	next if /^FAKE\s*$/;
	chomp;
	$lines++;
	my @s = split(/[\t]/);
	my ($lab, $typ) = ($s[0], $s[1]);
	defined($lab) || die "No first field for normalization input record:\n\"$_\"\n";
	defined($typ) || die "No second field for normalization input record:\n\"$_\"\n";
	if($lab ne $lastLab) {
		# We've moved on to a new label
		$labTot = 0;
		$als = 0;
		$lastLab = $lab;
		defined($labsSeen{$lab}) && die "Error, label $lab is intermingled with other labels\n";
		$labsSeen{$lab} = 1;
		%cnthash = ();
		($q25, $q50, $q75, $mx) = (0, 0, 0, 0);
		$totlabs++;
		print STDERR "Processing label $lab\n";
	}
	if($typ ne $all9s) {
		$als == 0 || die "Error: subtotals intermingled with alignments\n";
		# Line represents a count
		#  1. Label (primary)
		#  2. Count
		#  3. Gene name
		$#s == 2 || die "Bad number of tokens on subtotal line: ".scalar(@s)."\n$_\n";
		$typ == int($typ) || die "Count must be numeric: $typ\n";
		$typ = int($typ);
		$cnthash{$s[2]} += $typ;
		$labTot += $typ;
	} else {
		# Input line represents an alignment:
		#  1. Label (primary)
		#  2. Type (secondary)
		#  3. Orient
		#  4. SeqLen
		#  5. Oms
		#  6. Mate
		#  7. CIGAR
		#  8. Ival
		#  9. IvalOff
		if($als == 0 && scalar(keys %cnthash) > 0) {
			# Dump counts from high to low
			open TMP, ">.tmp.Normal.pl.$$" || die "Could not open .tmp.Normal.pl.$$ for writing\n";
			my @cnts = ();
			print TMP "gene_id\tcount\n";
			for my $k (sort ascendingCnt (keys %cnthash)) {
				substr($k, -1) eq "G" || die "Internal gene names must end with G: $k\n";
				my $gene = substr($k, 0, -1);
				print TMP "$gene\t$cnthash{$k}\n";
				push @cnts, $cnthash{$k} if $cnthash{$k} >= $ignoreBelow;
			}
			close(TMP);
			# Create counts output dir if it doesn't already exist
			# Store gene count table
			for my $fn ("$lab.txt", "$lab.norm", "$lab.norms") {
				Get::fs_remove("$output_url/$fn") if Get::fs_exists("$output_url/$fn");
			}
			system("mv .tmp.Normal.pl.$$ $lab.txt");
			Get::fs_put("$lab.txt", "$output_url");
			system("rm -f .tmp.Normal.pl.$$");
			# Calculate quartiles on the non-ignored counts
			my $num = scalar(@cnts);
			$mx  = $cnts[-1];
			$q25 = $cnts[$num*3 / 4];
			$q50 = $cnts[$num   / 2];
			$q75 = $cnts[$num   / 4];
			# Pick which normalization factor to report based on the
			# -normal parameter.
			$norm = $labTot;
			if($normal eq 'lup') {
				$norm = $q25;
			} elsif($normal eq 'lmed') {
				$norm = $q50;
			} elsif($normal eq 'llow') {
				$norm = $q75;
			} elsif($normal eq 'lmax') {
				$norm = $mx;
			} else {
				$normal eq 'ltot' || die "Bad normalization type: $normal\n";
			}
			open (NORM, ">.tmp.Normal.pl.norm.$$") || die "Couldn't open norm file for writing";
			print NORM "$norm\n";
			close(NORM);
			open (NORMS, ">.tmp.Normal.pl.norms.$$") || die "Couldn't open norms file for writing";
			print NORMS "tot\tupper_quart\tmedian\tlower_quart\tmax\n";
			print NORMS "$labTot\t$q25\t$q50\t$q75\t$mx\n";
			close(NORMS);
			system("mv .tmp.Normal.pl.norm.$$ $lab.norm");
			system("mv .tmp.Normal.pl.norms.$$ $lab.norms");
			Get::fs_put("$lab.norm", "$output_url");
			Get::fs_put("$lab.norms", "$output_url");
			print STDERR "$lab: total $labTot\n";
			print STDERR "$lab: distinct non-zero counts $num\n";
			print STDERR "$lab: maximum $mx\n";
			print STDERR "$lab: upper quartile $q25\n";
			print STDERR "$lab: median $q50\n";
			print STDERR "$lab: lower quartile $q75\n";
			print STDERR "reporter:counter:Normal,Label $lab total,$labTot\n";
			print STDERR "reporter:counter:Normal,Label $lab distinct non-zero counts,$num\n";
			print STDERR "reporter:counter:Normal,Label $lab maximum,$mx\n";
			print STDERR "reporter:counter:Normal,Label $lab upper quartile,$q25\n";
			print STDERR "reporter:counter:Normal,Label $lab median,$q50\n";
			print STDERR "reporter:counter:Normal,Label $lab lower quartile,$q75\n";
		}
		# Output line represents an alignment, annotated with
		# normalization constants:
		#  1. Ival (primary)
		#  2. IvalOff (secondary)
		#  3. Orient
		#  4. SeqLen
		#  5. Oms
		#  6. Mate
		#  7. CIGAR
		#  8. Lab
		#  9. Per-label normalization factor
		defined($norm) || die;
		scalar(@s) == 9 || die "Bad number of tokens on overlap line: ".scalar(@s)."\n";
		my ($orient,  $seqLen,  $oms, $mate, $cigar, $ival, $ivalOff) =
		   (  $s[2],    $s[3], $s[4], $s[5],  $s[6], $s[7],    $s[8]);
		print "$ival\t$ivalOff\t$orient\t$seqLen\t$oms\t$mate\t$cigar\t$lab\t$norm\n";
		$als++;
		$totals++;
	}
}
print STDERR "Alignments handled: $totals\n";
print STDERR "Labels handled: $totlabs\n";
print STDERR "Lines handled: $lines\n";
print STDERR "reporter:counter:Normal,Alignments handled by Normal.pl,$totals\n" if $totals > 0;
print STDERR "reporter:counter:Normal,Labels handled by Normal.pl,$totlabs\n" if $totlabs > 0;
print STDERR "reporter:counter:Normal,Lines read by Normal.pl,$lines\n" if $lines > 0;
print "FAKE\n";
