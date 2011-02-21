#!/usr/bin/perl -w

##
# Stats.pl
#
# Author: Ben Langmead
#   Date: December 1, 2009
#
# Sanity-check normalized tuples and write them to R.
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

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
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
my $tmp = ".tmp.Stats.pl.$$";
my $r = "";
my $r_arg = "";
my $Rfetch = "";
my $family = "";
my $globals_dir = "";
my $labs_arg = "";
my $bypassPvals = 0;
my $nulls = 0;
my $seed = -1;
my $maxalns = 300000;
my $profile = 0;
my $addFudge = 0;
my $paired = 0;
my $errorDir = "";
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

sub msg($) {
	my $m = shift;
	$m =~ s/[\r\n]*$//;
	print STDERR "Normal.pl: $m\n";
}

GetOptions (
	"R:s"           => \$r_arg,
	"Rfetch:s"      => \$Rfetch,
	"destdir:s"     => \$dest_dir,
	"errdir:s"      => \$errorDir,
	"counters:s"    => \$cntfn,
	"s3cmd:s"       => \$Tools::s3cmd_arg,
	"s3cfg:s"       => \$Tools::s3cfg,
	"jar:s"         => \$Tools::jar_arg,
	"accessid:s"    => \$AWS::accessKey,
	"secretid:s"    => \$AWS::secretKey,
	"hadoop:s"      => \$Tools::hadoop_arg,
	"wget:s"        => \$Tools::wget_arg,
	"family:s"      => \$family,
	"nulls:i"       => \$nulls,
	"maxalns=i"     => \$maxalns,
	"bypass-pvals"  => \$bypassPvals,
	"labs:s"        => \$labs_arg,
	"seed:i"        => \$seed,
	"add-fudge:i"   => \$addFudge,
	"paired"        => \$paired,
	"profile"       => \$profile,
	"globals:s"     => \$globals_dir) || die "Bad option\n";

$family = "poisson" if $family eq "";
$globals_dir = "/globals" if $globals_dir eq "";
$globals_dir =~ s/^S3N/s3n/;
$globals_dir =~ s/^S3/s3/;
$globals_dir =~ s/^HDFS/hdfs/;

msg("Family: $family");
msg("# nulls per gene: $nulls");
msg("seed (-1 = let R decide): $seed");
msg("Bypass P-value calculation: $bypassPvals");
msg("Profiling enabled: $profile");
msg("Add fudge factor: $addFudge");
msg("Samples are paired: $paired");

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

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

sub get_mset_global {
	my $k = shift;
	my $dir = "$globals_dir/multiset/$k/";
	my $ret = "";
	my @files = Get::lsDir($dir);
	for (@files) {
		my @s = split(/\//);
		my $str = $s[-1];
		$str =~ s/_\$folder.*//;
		if($str ne "") {
			$ret .= "," unless $ret eq "";
			$ret .= $str;
		}
	}
	return $ret;
}

my $labs = get_mset_global("label") if $labs_arg eq "";
$labs = $labs_arg if $labs_arg ne "";

msg("Result of get_mset_global('label'): $labs");
msg("Warning - labs is empty!") if $labs eq "";

my $alns = 0;
my $alnsBatch = 0;

sub analyzeBatch() {
	length("$tmp")         > 0 || die;
	length("$labs")        > 0 || die;
	length("$family")      > 0 || die;
	length("$nulls")       > 0 || die;
	length("$seed")        > 0 || die;
	length("$bypassPvals") > 0 || die;
	length("$profile")     > 0 || die;
	length("$addFudge")    > 0 || die;
	length("$paired")      > 0 || die;
	my $cmd = "$r --vanilla --default-packages=stats,zoo,grDevices,graphics,utils,methods,lmtest,MASS $Bin/Stats.R --args $tmp $labs $family $nulls $seed $bypassPvals $profile $addFudge $paired";
	open TMPSH, ">$tmp.sh" || die "Could not open $tmp.sh for writing\n";
	print TMPSH "$cmd\n";
	close(TMPSH);
	# Now R will look at all of the alignments falling into the interval,
	# calculate a differential-expression P-value, and output it.
	my $ret = Util::runAndWait($cmd, "R");
	if($ret != 0) {
		if(defined($errorDir) && $errorDir ne "") {
			# Write the relevant data to an error directory
			mkpath("$errorDir/Stats.pl.$$");
			open ERR, ">$errorDir/Stats.pl.$$/err.sh";
			print ERR "$cmd\n";
			close(ERR);
			system("cp * $errorDir/Stats.pl.$$/");
			system("cp .* $errorDir/Stats.pl.$$/");
		}
		die "Exitlevel $ret from command $cmd\n";
	}
	msg("reporter:counter:Stats,Alignments handled by Stats.pl,$alnsBatch");
	unlink("$tmp.sh");
	$alnsBatch = 0;
}

my $lastIval = "";
my $lastOff = -99999;
my %seenIvals = ();
open TMP, ">$tmp" || die "Could not open $tmp for writing\n";
while(<STDIN>) {
	next if /^FAKE\s*$/;
	flushCounters();
	chomp;
	my @s = split(/[\t]/);
	scalar(@s) == 9 || die "Expected 9 fields from normal step, got ".scalar(@s).":\n$_\n";
	my ($ival, $off, $lab) = ($s[0], $s[1], $s[7]);
	$off == int($off) || die "Offset (2nd col) must be a number, was $off\n$_";
	if($ival ne $lastIval) {
		defined($seenIvals{$ival}) && die "Already saw interval $ival\n$_";
		if($maxalns > 0 && $alnsBatch >= $maxalns) {
			close(TMP);
			analyzeBatch();
			open TMP, ">$tmp" || die "Could not open $tmp for writing\n";
		}
		$seenIvals{$ival} = 1;
		$lastIval = $ival;
		$lastOff = -99999;
	}
	$off >= $lastOff || die "Offsets out of order; $lastOff preceded $off\n$_";
	$lastOff = $off;
	my $al = join("\t", @s);
	# Send the alignment to R
	print TMP "$al\n";
	$alns++;
	$alnsBatch++;
}
close(TMP);
analyzeBatch() if $alnsBatch > 0;
unlink($tmp);

if($alns > 0) {
	msg("Alignments handled: $alns");
	msg("reporter:counter:Stats,Intervals with > 1K alignments,1")   if $alns > 1000;
	msg("reporter:counter:Stats,Intervals with > 10K alignments,1")  if $alns > 10000;
	msg("reporter:counter:Stats,Intervals with > 100K alignments,1") if $alns > 100000;
	msg("reporter:counter:Stats,Intervals with > 1M alignments,1")   if $alns > 1000000;
	msg("reporter:counter:Stats,Intervals with > 10M alignments,1")  if $alns > 10000000;
	msg("reporter:counter:Stats,Intervals with > 100M alignments,1") if $alns > 100000000;
	msg("reporter:counter:Stats,Intervals with > 1B alignments,1")   if $alns > 1000000000;
}
print "FAKE\n";
