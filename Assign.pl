#!/usr/bin/perl -w

##
# Assign.pl
#
# Author: Ben Langmead
#   Date: November 5, 2009
#
# Dump each distinct partition's worth of input to a file, then run the
# Assign.R script on that file.
#

use strict;
use warnings;
use 5.004;
use Getopt::Long;
use FindBin qw($Bin); 
use lib $Bin;
use Counters;
use Get;
use Util;
use Tools;
use AWS;
use File::Path qw(mkpath);
use Fcntl qw(:DEFAULT :flock); # for locking

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

sub msg($) {
	my $m = shift;
	$m =~ s/[\r\n]*$//;
	print STDERR "Assign.pl: $m\n";
}

my $ivalsjar = "";
my $dest_dir = "";
my $lastPart = "";
my $tmp = ".tmp.Assign.pl.$$";
my $als = 0;
my $parts = 0; # num partitions processed so far
my $maxparts = 0; # max num partitions to process
my $keep = 0; # 1 -> keep .tmp files
my $partbin = 100; # num partitions to bundle up before sending to Assign.R
my $ivals = ".";
my $maxalns = 50000; # max # alignments per invocation of R
my $Rfetch = "";
my $r = "";
my $r_arg = "";
my $r_args = "--vanilla --default-packages=base,methods,utils,stats,IRanges";
my $all9s = "999999999";
my $maxInfluence = 999999;
my $binWidth = 999999;
my $bin = 0;
my $ivalModel = "";
my $from5prime = 0;
my $fromMiddle = 0;
my $profile = 0;
my $cntfn = "";
my $errorDir = "";
my $globals_dir = "";

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

GetOptions (
	"ivalsjar:s" => \$ivalsjar,
	"ivaljar:s"  => \$ivalsjar,
	"ival-model:s"=> \$ivalModel,
	"destdir:s"  => \$dest_dir,
	"errdir:s"   => \$errorDir,
	"s3cmd:s"    => \$Tools::s3cmd_arg,
	"s3cfg:s"    => \$Tools::s3cfg,
	"jar:s"      => \$Tools::jar_arg,
	"accessid:s" => \$AWS::accessKey,
	"secretid:s" => \$AWS::secretKey,
	"hadoop:s"   => \$Tools::hadoop_arg,
	"wget:s"     => \$Tools::wget_arg,
	"maxparts=i" => \$maxparts,
	"maxalns=i"  => \$maxalns,
	"partbin=i"  => \$partbin,
	"ivals=s"    => \$ivals,
	"influence=i"=> \$maxInfluence,
	"bin"        => \$bin,
	"profile"    => \$profile,
	"binwidth=i" => \$binWidth,
	"from5prime" => \$from5prime,
	"from-middle" => \$fromMiddle,
	"counters:s" => \$cntfn,
	"globals:s"  => \$globals_dir,
	"R:s"        => \$r_arg,
	"Rfetch:s"   => \$Rfetch,
	"keep"       => \$keep) || die "Bad option";

msg("Interval dir: $ivals");
msg("R path: $r");
msg("Partition bin size: $partbin");
msg("Max read influence: $maxInfluence");
msg("Interval model: $ivalModel");
msg("Using bins: $bin");
msg("Bin width: $binWidth");
msg("Enable profiling: $profile");
msg("Max alignments per R invocation: $maxalns");
msg("R to fetch: $Rfetch");
msg("Measure influence from 5' end?: $from5prime");
msg("Measure influence from middle?: $fromMiddle");
msg("Contents of directory:");
msg("Globals directory: $globals_dir");
msg("ls -al");
msg(`ls -al`);

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

$globals_dir = "/globals" if $globals_dir eq "";
$globals_dir =~ s/^S3N/s3n/;
$globals_dir =~ s/^S3/s3/;
$globals_dir =~ s/^HDFS/hdfs/;

my %labCnts = ();

#
# Now update our HDFS-based pseudo-multimap with all the labels we
# observed.  We'll re-read these in the Stats phase so that we know
# exactly what groups to test.
#

sub fs_ensure_dir_weak {
	my ($paths, $local) = @_;
	if($local) {
		mkpath(@$paths);
	} else {
		my $pathstr = join(' ', @$paths);
		my $hadoop = Tools::hadoop();
		$hadoop ne "" || die "Empty hadoop path: '$hadoop'";
		system("($hadoop fs -mkdir $pathstr) >& /dev/null");
	}
}

my $set_mset_global_first = 1;
my %set_mset_global_first_key = ();
my %set_mset_global_first_keyval = ();
my @set_mset_global_delayed = ();

##
# Make all the directories tempoarily queued up in the
# @set_mset_global_delayed list.
#
sub set_mset_flush {
	if(scalar(@set_mset_global_delayed) > 0) {
		fs_ensure_dir_weak(
			\@set_mset_global_delayed,
			Util::is_local($set_mset_global_delayed[0]));
	}
	@set_mset_global_delayed = ();
	counter("Bowtie,Label update flushes,1");
}

sub set_mset_global {
	my ($k, $v) = @_;
	my $local = Util::is_local($globals_dir);
	if($set_mset_global_first) {
		fs_ensure_dir_weak([ "$globals_dir/multiset" ], $local);
		$set_mset_global_first = 0;
	}
	unless(defined($set_mset_global_first_key{$k})) {
		fs_ensure_dir_weak([ "$globals_dir/multiset/$k" ], $local);
		$set_mset_global_first_key{$k} = 1;
	}
	unless(defined($set_mset_global_first_keyval{"$k/$v"})) {
		push @set_mset_global_delayed, "$globals_dir/multiset/$k/$v";
		set_mset_flush() if scalar(@set_mset_global_delayed) >= 20;
		$set_mset_global_first_keyval{"$k/$v"} = 1;
		counter("Bowtie,Label updates,1");
	}
}

sub get_mset_global {
	my $k = shift;
	my $ret = "";
	my $v;
	if(Util::is_local($globals_dir)) {
		$v = `ls -1 $globals_dir/multiset/$k`;
	} else {
		my $hadoop = Tools::hadoop();
		$hadoop ne "" || die "Empty hadoop path: '$hadoop'";
		$v = `$hadoop fs -ls $globals_dir/multiset/$k`;
	}
	for my $line (split(/[\r\n]+/, $v)) {
		# Take everything after the final slash
		next if $line eq "";
		next if $line =~ /^Found/; # discard 'Found N items' message
		my @s = split(/\//, $line);
		my $str = $s[-1];
		if($str ne "") {
			$ret .= "," unless $ret eq "";
			$ret .= $str;
		}
	}
	return $ret;
}

sub finalizeLabCounts() {
	while(my ($k, $v) = each(%labCnts)) {
		counter("Bowtie,Label $k aligned reads,$v");
	}
	for my $k (keys %labCnts) { set_mset_global("label", $k); }
	set_mset_flush();
	msg("Result of get_mset_global('label'): ".get_mset_global("label")."");
}

my $from3prime = $from5prime ? "0" : "1";

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
$partbin >= 1 || die "-partbin was $partbin, but must be >= 1\n";

##
# Make a set of Exon interval files from the ivals/exons.txt file
# included in the reference jar.  Either the user specifies the ivals
# directory or it defaults to $dest_dir/ivals.
#
sub makeExons {
	my $ivalPath = shift;
	my $exonPath;
	if(!defined($ivalPath)) {
		$ivalPath = "$dest_dir/ivals";
		$exonPath = "$ivalPath/exon";
		msg("Making exon intervals in default path '$exonPath'");
	} else {
		$exonPath = "$ivalPath/exon";
		msg("Making exon intervals in specified path '$exonPath'");
		(-d $ivalPath) || die "makeExons: Specified ivals path '$ivalPath' does not exist";
	}
	if(-d $exonPath) {
		# Assume it was included in the archive
		return;
	}
	mkpath($exonPath);
	my $exonFile = "$ivalPath/exons.txt";
	msg("Reading exon information from '$exonFile'");
	open (EXONS, $exonFile) || die "Expected a file '$exonFile'";
	# One output filehandle per chromosome
	my %outFhs = ();
	msg("PID $$ making exon directory");
	my $header = <EXONS>; # skip header line
	while(<EXONS>) {
		chomp;
		my ($name1, $name2, $name3, $chr, $start, $end) = split(/\t/);
		if(!defined($outFhs{$chr})) {
			my $fn = "$exonPath/$chr.ivals";
			open ($outFhs{$chr}, ">$fn") || die "Could not open '$fn' for writing";
			defined($outFhs{$chr}) || die "Didn't install $name3 in filehandle hash";
		}
		print {$outFhs{$chr}} "$chr\t$name3\t$start\t$end\n";
	}
	close(EXONS);
	for my $k (keys %outFhs) {
		close($outFhs{$k});
	}
}

##
# Make a set of Exon interval files from the ivals/exons.txt file
# included in the reference jar.  If ivalPath argument is not
# specified, use $dest_dir/ivals.
#
sub makeExonsSync($) {
	my $ivalPath = shift;
	my $exonPath;
	if(!defined($ivalPath)) {
		$ivalPath = "$dest_dir/ivals";
		$exonPath = "$ivalPath/exon";
		msg("Making exon intervals in default path '$exonPath'");
	} else {
		$exonPath = "$ivalPath/exon";
		msg("Making exon intervals in specified path '$exonPath'");
		(-d $ivalPath) || die "makeExons: Specified ivals path '$ivalPath' does not exist";
	}
	my $lock_file = "$ivalPath/.exon.lock";
	my $done_file = "$ivalPath/.exon.done";
	# Create the file to lock
	system("touch $lock_file");
	msg("Pid $$: Attempting to obtain lock...");
	open(FH, "<$lock_file") or croak("Can't open lock file \"$lock_file\": $!");
	# Attempt to get an exclusive lock.  Do not block.  Blocking
	# without printing anything while we wait for a while might get us
	# killed by Hadoop.
	if(flock(FH, LOCK_EX | LOCK_NB)) {
		makeExons($ivalPath);
		system("touch $done_file");
	} else {
		# We didn't get the lock.  Spin until the process with the lock
		# is done so that we don't get.  Do so verbosely so that Hadoop
		# doesn't kill us.
		my $sleeps = 0;
		while(! -f $done_file) {
			sleep(1);
			if((++$sleeps % 10) == 0) {
				my $secs = $sleeps;
				msg("Pid $$: still waiting (it's been $secs seconds)");
			}
		}
		msg("Pid $$: I see done file '$done_file' now; I'm assuming exon intervals are in place\n");
		sleep(3);
	}
	close(FH);
}

my $ivalPath = "";
if($ivalsjar ne "") {
	mkpath($dest_dir);
	(-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";
	msg("Ensuring reference jar is installed");
	Get::ensureFetched($ivalsjar, $dest_dir, \@counterUpdates);
	if($ivals ne "") {
		msg("Overriding old ivals = $ivals");
		msg("  with $dest_dir/ivals/$ivalModel");
	}
	$ivalPath = "$dest_dir/ivals";
	$ivals = "$ivalPath/$ivalModel";
} else {
	$ivalPath = $ivals;
	$ivals = "$ivalPath/$ivalModel";
}

##
# If the exon directory isn't there, try to make it from the exons.txt
# file.  Use mutual exclusion so that processes don't trip over each
# other.
#
makeExonsSync($ivalPath) if $ivalModel eq "exon";
(-d $ivals) || die "Interval dir $ivals doesn't exist\n";

(-f "$Bin/Assign.R" || die "$Bin/Assign.R doesn't exist or isn't readable");

# Dispatch bundles of sa
open TMP, ">$tmp.$parts" || die "Could not open $tmp.$parts for writing";
my $bparts = 0;
my $alsin = 0;
while(1) {
	my $l = <STDIN>;
	flushCounters();
	my $part = "";
	if(defined($l)) {
		chomp($l);
		next if $l =~ /^FAKE\s*$/;
		$alsin++;
		my $t1 = index($l, "\t");
		$t1 > 0 || die "Bad position for 1st tab: $t1, \"$l\"";
		my $t2 = index($l, "\t", $t1+1);
		$t2 > $t1 || die "Bad position for 2nd tab: $t2, \"$l\"";
		$part = substr($l, 0, $t2);
		my @s = split(/\t/, $l);
		scalar(@s) == 9 || die "Expected 9 tokens in input to Assign.pl, instead saw:\n$l\n";
		# Final token is label, without LB: prefix and already pooled
		my $lab = $s[-1];
		$labCnts{$lab}++;
	}
	$bparts < $partbin || die "# partitions, $bparts, exceeded bin size, $partbin\n";
	if(!defined($l) || ($part ne $lastPart && ++$bparts == $partbin) || $als == $maxalns) {
		$bparts = 1 if $bparts == 0;
		# Finished partition
		$als <= $maxalns ||
			die "# alignments buffered, $als, exceeded maximum, $maxalns\n";
		close(TMP);
		if($als > 0) {
			msg("Sending partition $part");
			counter("Assign,Alignments sent to Assign.R,$als");
			counter("Assign,Partitions sent to Assign.R,$bparts");
			msg("head -4 $tmp.$parts:");
			msg(`head -4 $tmp.$parts`);
			msg("tail -4 $tmp.$parts:");
			msg(`tail -4 $tmp.$parts`);
			my $cmd = "$r $r_args $Bin/Assign.R --args ".
				"$tmp.$parts ".    # alnFile
				"$maxInfluence ".  # maxAlgnInfluence
				"$bin ".           # ivalsFromDir (sense is inverted in Assign.R)
				"$ivals ".         # ivalsDir
				"$binWidth ".      # binWidth
				"$from3prime ".    # from3prime
				"$fromMiddle |";   # fromMiddle
			msg("Running: $cmd");
			# Write the relevant data to an error directory
			if(defined($errorDir) && $errorDir ne "") {
				mkpath("$errorDir/Assign.pl.$$");
				open(ERR, ">$errorDir/Assign.pl.$$/err.sh") || die;
				print ERR "$cmd\n";
				close(ERR);
				system("cp * $errorDir/Assign.pl.$$/ 2>/dev/null");
				system("cp .* $errorDir/Assign.pl.$$/ 2>/dev/null");
			}
			# Print messages while reading records from R; otherwise,
			# Hadoop might want to kill us for being inactive
			my $doneFile = ".write_table.$parts.done";
			my $pid = fork();
			if($pid == 0) {
				my $sleepSecs = 0;
				while(! -f $doneFile) {
					print STDERR "Waited for R for $sleepSecs...\n";
					sleep(10);
					$sleepSecs += 10;
				}
				exit 0;
			}
			open (CMD, $cmd) || die "Could not open pipe '$cmd'\n";
			while(<CMD>) {
				chomp;
				my @s = split(/\t/);
				if(scalar(@s) == 9) {
					$s[7] .= "G"; # Add a trailing G to gene name
				} else {
					scalar(@s) == 3 || die "Bad number of tokens in line from Assign.R; must be 3 or 9\n";
					$s[2] .= "G"; # Add a trailing G to gene name
				}
				print join("\t", @s)."\n";
			}
			close(CMD);
			system("touch $doneFile");
			die "Invocation of '$cmd' aborted with exitlevel $?\n" if $? != 0;
			if(defined($errorDir) && $errorDir ne "") {
				system("rm -rf $errorDir/Assign.pl.$$");
			}
			last if ($parts+1) == $maxparts;
		}
		unlink("$tmp.$parts") unless $keep;
		$parts++;
		$bparts = 0;
		$als = 0;
		open TMP, ">$tmp.$parts" || die "Could not open $tmp.$parts for writing";
	}
	last unless defined($l);
	print TMP "$l\n";
	$als++;
	msg("Handled $als alignments") if ($als % 10000) == 0;
	$lastPart = $part;
}
close(TMP);
unlink("$tmp.$parts") unless $keep;

# If there's any more input, chew it up
while(<STDIN>) { }
msg("reporter:counter:Assign,Alignments handled by Assign.pl,$alsin") if $alsin > 0;
print "FAKE\n";
finalizeLabCounts();
