#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: February 20, 2010
#
# Use 'elastic-mapreduce' ruby script to invoke an EMR job described
# in a dynamically-generated JSON file.  Constructs the elastic-
# mapreduce invocation from paramteres/defaults/environment variables.
#

package MyrnaIface;
use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use List::Util qw[min max];
use Cwd 'abs_path';
use lib $Bin;
use Tools;
use File::Path qw(mkpath);
use AWS;

##
# Function interface for invoking the generic Myrna wrapper.
#
sub myrna {

scalar(@_) == 7 || die "Must specify 7 arguments";

our @args = @{$_[0]};
our $scr   = $_[1];
our $usage = $_[2];
our $msg   = $_[3];
our $msgf  = $_[4];
our $emsg  = $_[5];
our $emsgf = $_[6];

defined($msg)   || ($msg   = sub { print @_ });
defined($msgf)  || ($msgf  = sub { printf @_ });
defined($emsg)  || ($emsg  = sub { print STDERR @_ });
defined($emsgf) || ($emsgf = sub { printf STDERR @_ });

our $APP = "Myrna";
my $pre = "MYRNA_";
our $app = lc $APP;
our $VERSION = `cat $Bin/VERSION`; $VERSION =~ s/\s//g;
if($VERSION eq "") {
	$VERSION = `cat $Bin/VERSION_MYRNA`; $VERSION =~ s/\s//g;
}

our $umaskOrig = umask();

sub dieusage($$$) {
	my ($text, $usage, $lev) = @_;
	$emsg->("$text\n");
	$emsg->("$usage\n");
	exit $lev;
}

our $warnings = 0;
sub warning($) {
	my $str = shift;
	$emsg->("$str\n");
	$warnings++;
}

# AWS params
our $awsEnv = 0;
our $emrScript = "";
our $hadoopVersion = "";
our $accessKey = "";
our $secretKey = "";
our $keypair = "";
our $keypairFile = "";
our $zone = "";
our $credentials = "";
our $swap = 0; # to add

# EMR params
our $dryrun = 0;
our $name = "";
our $waitJob = 0;
our $instType = "";
our $numNodes = 1;
our $bidPrice = 0.10;
our $reducersPerNode = 0;
our $emrArgs = "";
our $noLogs = 0;
our $logs = "";
our $noEmrDebugging = 0;
our $rUrl = "";

# Job params
our $input  = "";
our $output = "";
our $intermediate = "";
our $partitionLen = 0;
our $justAlign = 0;
our $resumeAlign = 0;
our $justCount = 0;
our $resumeOlap = 0;
our $resumeNormal = 0;
our $resumeStats = 0;
our $resumeSumm = 0;
our $count = "";
our $chosen = "";
our $keepAll = 0;
our $keepIntermediate = 0;

# Lobal job params
our $localJob = 0;
our $test = 0;
our $inputLocal  = "";
our $outputLocal = "";
our $intermediateLocal = "";
our $cores = 0;
our $dontForce = 0;
our $bowtie = "";
our $samtools = "";
our $fastq_dump = "";
our $useSamtools = 0;
our $useFastqDump = 0;
our $Rhome = "";
our $externalSort = 0;
our $maxSortRecords = 800000;
our $maxSortFiles = 40;

# Hadoop job params
our $hadoopJob = 0;
our $hadoop_arg = "";
our $hadoopStreamingJar_arg = "";

# Preprocessing
our $preprocess = 0;
our $justPreprocess = 0;
our $preprocOutput = "";
our $preprocCompress = "";
our $preprocStop = 0;
our $preprocMax = 0;

# Myrna params
our $ref = "";
our $bt_args = "";
our $qual = "";
our $discardReads = 0;
our $indexLocal = "";
our $truncate = 0;
our $truncateDiscard = 0;
our $ivalLocal = "";
our $top = 0;
our $family = "";
our $norm = "";
our $ivalModel = "";
our $bypassPvals = 0;
our $bin = 0;
our $influence = 0;
our $fromStr = "";
our $sampass = 0;
our $poolReplicates = 0;
our $poolTechReplicates = 0;
our $poolTrimLen = 0;
our $samLabRg = 0;
our $maxalns = 0;
our $partbin = 0;
our $permTest = 0;
our $pairedTest = 0;
our $ditchAlignments = 0;
our $discardMate = 0;
our $profile = 0;
our $addFudge = 0;

# Other parmams
our $tempdir = "";
our $slaveTempdir = "";
our $splitJars = 0;
our $verbose = 0;

sub absPath($) {
	my $path = shift;
	defined($path) || die;
	if($path =~ /^hdfs:/i || $path =~ /^s3n?:/i || $path eq "") {
		return $path;
	}
	$path =~ s/^~/$ENV{HOME}/;
	my $ret = abs_path($path);
	defined($ret) || die "abs_path turned $path into undef\n";
	return $ret;
}

##
# A tiny log facility in case we need to report what we did to the user.
#
our $checkExeMsg = "";
sub checkExeLog($) {
	my $text = shift;
	$checkExeMsg .= $text;
	$emsg->($text) if $verbose;
}

##
# Can I run the executable and receive error 256?  This is a little
# more robust than -x, but it requires that the executable return 1
# immediately if run without arguments.
#
sub canRun {
	my ($nm, $f, $exitlevel) = @_;
	$exitlevel = 0 unless defined($exitlevel);
	my $ret = system("$f 2>/dev/null >/dev/null") >> 8;
	return 1 if $ret == $exitlevel;
	if($ret != 1 && $ret != 255) {
		return 0;
	}
	if($nm eq "Rscript" || $nm eq "R") {
		checkExeLog("  Checking whether R has appropriate R/Bioconductor packages...\n");
		my $packages = "";
		for my $pack ("lmtest", "multicore", "IRanges", "geneplotter") {
			$packages .= "suppressPackageStartupMessages(library($pack)); print('Found required package $pack'); ";
		}
		my $out = `$f -e \"$packages print('All packages found')\" 2>&1`;
		checkExeLog($out);
		$ret = $? >> 8;
		return $ret == $exitlevel;
	}
	return 1;
}

##
# Scan the bin subdirectory for a working version of the given program.
#
sub scanPrebuiltBin {
	my ($nm, $base, $exitlevel) = @_;
	defined($nm) || die;
	defined($base) || die;
	$exitlevel = 0 unless defined($exitlevel);
	my @ret = ();
	for my $f (<$base/bin/*>) {
		checkExeLog("     Scanning directory: $f\n");
		for my $f2 (<$f/$nm>) {
			next unless -f $f2;
			checkExeLog("       Found candidate: $f2\n");
			checkExeLog("         Runnable?...");
			if(canRun($nm, $f2, $exitlevel)) {
				checkExeLog("YES\n");
				push @ret, $f2;
			} else {
				checkExeLog("no\n");
			}
		}
	}
	if($nm eq "Rscript" || $nm eq "R") {
		my $path = "$Bin/R/bin/Rscript";
		checkExeLog("     I'm searching for R or Rscript, so scanning directory: $path\n");
		if(canRun($nm, $path, $exitlevel)) {
			push @ret, $path;
		}
	}
	if(scalar(@ret) > 0) {
		@ret = sort @ret;
		checkExeLog("       Settling on $ret[-1]\n");
		return $ret[-1];
	} else {
		checkExeLog("       No runnable candidates\n");
		return "";
	}
}

##
# Require that an exe be specified and require that it's there.
#
sub checkExe {
	my ($path, $nm, $env, $sub, $arg, $dieOnFail, $exitlevel) = @_;
	$exitlevel = 0 unless defined($exitlevel);
	$nm ne "" || die "Empty name\n";
	defined($path) || die "Path for $nm undefined\n";
	checkExeLog("Searching for '$nm' binary...\n");
	checkExeLog(sprintf "   Specified via $arg?....%s\n", (($path ne "") ? "YES" : "no"));
	if($path ne "") {
		my $cr = canRun($nm, $path, $exitlevel);
		checkExeLog(sprintf("     Runnable?....%s\n", ($cr ? "YES" : "no")));
		return $path if $cr;
		die "Error: $arg specified, but path $path does not point to something $APP can execute\n";
	}
	my $envSpecified = defined($ENV{$env}) && $ENV{$env} ne "";
	checkExeLog(sprintf "   \$$env specified?....%s\n", ($envSpecified ? "YES ($ENV{$env})" : "no"));
	if($envSpecified) {
		my $envPath = $ENV{$env};
		$envPath .= "/$sub" if $sub ne "";
		$envPath .= "/$nm";
		my $cr = canRun($nm, $envPath, $exitlevel);
		checkExeLog(sprintf "     Runnable?....%s\n", ($cr ? "YES" : "no"));
		return $envPath if $cr;
	}
	checkExeLog("   Checking $Bin/bin...\n");
	$path = scanPrebuiltBin($nm, $Bin);
	return $path if $path ne "";
	checkExeLog("   Checking \$PATH...\n");
	$path = `which $nm 2>/dev/null`;
	if(defined($path)) {
		chomp($path);
		if($path) {
			checkExeLog("     Found '$path'...\n");
			my $cr = canRun($nm, $path, $exitlevel);
			checkExeLog(sprintf "       Runnable?....%s\n", ($cr ? "YES" : "no"));
			return $path if $cr;
		} else {
			checkExeLog("     Didn't find anything...\n");
		}
	}
	$emsg->("Error: Could not find '$nm' executable\n");
	if($hadoopJob) {
		$emsg->("Note: for Hadoop jobs, required executables must be located at the same path on all cluster nodes including the master.\n");
	}
	unless($verbose) {
		$emsg->("Here's what I tried:\n");
		$emsg->($checkExeMsg);
	}
	exit 1 if $dieOnFail;
	return "";
}

@ARGV = @args;

my $help = 0;

Getopt::Long::Configure("no_pass_through");
GetOptions (
# AWS params
	"aws-env"                   => \$awsEnv,
	"emr-script:s"              => \$emrScript,
	"elastic-mapreduce:s"       => \$emrScript,
	"hadoop-version:s"          => \$hadoopVersion,
	"accessid:s"                => \$accessKey,
	"secretid:s"                => \$secretKey,
	"keypair|key-pair:s"        => \$keypair,
	"key-pair-file:s"           => \$keypairFile,
	"zone|region:s"             => \$zone,
	"credentials:s"             => \$credentials,
# EMR params
	"dryrun"                    => \$dryrun,
	"dry-run"                   => \$dryrun,
	"name:s"                    => \$name,
	"instance-type:s"           => \$instType,
	"stay-alive"                => \$waitJob,
	"wait-on-fail"              => \$waitJob,
	"nodes:s"                   => \$numNodes,
	"bid-price:f"               => \$bidPrice,
	"instances|num-instances:s" => \$numNodes,
	"emr-args:s"                => \$emrArgs,
	"no-logs"                   => \$noLogs,
	"logs:s"                    => \$logs,
	"no-emr-debug"              => \$noEmrDebugging,
	"swap:i"                    => \$swap,
# Job params
	"input:s"                   => \$input,
	"output:s"                  => \$output,
	"intermediate:s"            => \$intermediate,
	"partition-len:i"           => \$partitionLen,
	"just-align"                => \$justAlign,
	"resume-align"              => \$resumeAlign,
	"count:s"                   => \$count,
	"just-count"                => \$justCount,
	"resume-olap"               => \$resumeOlap,
	"resume-normal"             => \$resumeNormal,
	"resume-stats"              => \$resumeStats,
	"resume-summary"            => \$resumeSumm,
	"local-job"                 => \$localJob,
	"hadoop-job"                => \$hadoopJob,
	"keep-all"                  => \$keepAll,
	"keep-intermediates"        => \$keepIntermediate,
	"test"                      => \$test,
# Local job params
	"input-local:s"             => \$inputLocal,
	"output-local:s"            => \$outputLocal,
	"intermediate-local:s"      => \$intermediateLocal,
	"cores:i"                   => \$cores,
	"cpus:i"                    => \$cores,
	"max-sort-records:i"        => \$maxSortRecords,
	"max-sort-files:i"          => \$maxSortFiles,
	"dont-overwrite"            => \$dontForce,
	"no-overwrite"              => \$dontForce,
	"bowtie:s"                  => \$bowtie,
	"samtools:s"                => \$samtools,
	"fastq-dump:s"              => \$fastq_dump,
	"Rhome:s"                   => \$Rhome,
	"external-sort"             => \$externalSort,
# Hadoop job params
	"hadoop:s"                  => \$hadoop_arg,
	"streaming-jar:s"           => \$hadoopStreamingJar_arg,
# Myrna params
	"reference:s"               => \$ref,
	"index-local:s"             => \$indexLocal,
	"quality|qual|quals:s"      => \$qual,
	"bowtie-args:s"             => \$bt_args,
	"discard-reads:f"           => \$discardReads,
	"truncate|truncate-length:i"=> \$truncate,
	"truncate-discard:i"        => \$truncateDiscard,
	"ival-local:s"              => \$ivalLocal,
	"ivals-local:s"             => \$ivalLocal,
	"bin:i"                     => \$bin,
	"influence:i"               => \$influence,
	"from3prime"                => sub { $fromStr = ""; },
	"from5prime"                => sub { $fromStr = "--from5prime"; },
	"from-middle"               => sub { $fromStr = "--from-middle"; },
	"sam-passthrough"           => \$sampass,
	"sam-lab-rg"                => \$samLabRg,
	"top:i"                     => \$top,
	"family:s"                  => \$family,
	"normalize:s"               => \$norm,
	"ival-model:s"              => \$ivalModel,
	"gene-footprint:s"          => \$ivalModel,
	"bypass-pvals"              => \$bypassPvals,
	"perm-tests:i"              => \$permTest,
	"paired-ttest"              => \$pairedTest,
	"permutation-tests:i"       => \$permTest,
	"nulls:i"                   => \$permTest,
	"nulls-per-gene:i"          => \$permTest,
	"ditch-alignments"          => \$ditchAlignments,
	"discard-mate:i"            => \$discardMate,
	"profile"                   => \$profile,
	"pool-replicates"           => \$poolReplicates,
	"pool-reps"                 => \$poolReplicates,
	"pool-tech-replicates"      => \$poolTechReplicates,
	"pool-technical-replicates" => \$poolTechReplicates,
	"pool-tech-reps"            => \$poolTechReplicates,
	"pool-technical-reps"       => \$poolTechReplicates,
	"pool-trim-length:i"        => \$poolTrimLen,
	"add-fudge:i"               => \$addFudge,
# Preprocessing params
	"preprocess"                => \$preprocess,
	"just-preprocess"           => \$justPreprocess,
	"myrna"                     => sub { $justPreprocess = 0 },
	"pre-output:s"              => \$preprocOutput,
	"preproc-output:s"          => \$preprocOutput,
	"preprocess-output:s"       => \$preprocOutput,
	"pre-compress:s"            => \$preprocCompress,
	"preproc-compress:s"        => \$preprocCompress,
	"preprocess-compress:s"     => \$preprocCompress,
	"pre-stop:i"                => \$preprocStop,
	"pre-filemax:i"             => \$preprocMax,
# Other parmams
	"tempdir:s"                 => \$tempdir,
	"slave-tempdir:s"           => \$slaveTempdir,
	"split-jars"                => \$splitJars,
	"verbose"                   => \$verbose,
	"version"                   => \$VERSION,
	"help"                      => \$help
) || dieusage("Error parsing options", $usage, 1);

dieusage("", $usage, 0) if $help;

# This function generates random strings of a given length
sub randStr($) {
	my $len = shift;
	my @chars = ('a'..'z', 'A'..'Z', '0'..'9', '_');
	my $str = "";
	foreach (1..$len) {
		$str .= $chars[int(rand(scalar(@chars)))];
	}
	return $str;
}
srand(time ^ $$);
my $randstr = randStr(10);

sub validateFamily($) {
	my $tes = shift;
	$tes eq "poisson" ||
	 $tes eq "gaussian" ||
	  $tes eq "binomial" ||
	   die "Bad --test family: \"$tes\"; must be poisson, gaussian, or binomial\n";
}

sub validateNorm($) {
	my $norm = shift;
	$norm eq "upper-quartile" ||
	 $norm eq "total" ||
	  $norm eq "median" ||
	   die "Bad --norm: \"$norm\"; must be upper-quartile, median, or total\n";
}

sub xformIvalModel($) {
	my $model = shift;
	if(substr("union", 0, length($model)) eq $model) {
		return "un";
	}
	if(substr("constitutive", 0, length($model)) eq $model) {
		return "un_const";
	}
	if(substr("intersect", 0, length($model)) eq $model) {
		return "ui";
	}
	if(substr("exon", 0, length($model)) eq $model) {
		return "exon";
	}
	die "Bad --gene-footprint: \"$model\"; must be union, constitutive, intersect or exon\n";
}

# See http://aws.amazon.com/ec2/instance-types/

our %instTypeNumCores = (
	"m1.small" => 1,
	"m1.large" => 2,
	"m1.xlarge" => 4,
	"c1.medium" => 2,
	"c1.xlarge" => 8,
	"m2.xlarge" => 2,
	"m2.2xlarge" => 4,
	"m2.4xlarge" => 8,
	"cc1.4xlarge" => 8
);

our %instTypeSwap = (
	"m1.small"    => (2 *1024), #  1.7 GB
	"m1.large"    => (8 *1024), #  7.5 GB
	"m1.xlarge"   => (16*1024), # 15.0 GB
	"c1.medium"   => (2 *1024), #  1.7 GB
	"c1.xlarge"   => (8 *1024), #  7.0 GB
	"m2.xlarge"   => (16*1024), # 17.1 GB
	"m2.2xlarge"  => (16*1024), # 34.2 GB
	"m2.4xlarge"  => (16*1024), # 68.4 GB
	"cc1.4xlarge" => (16*1024)  # 23.0 GB
);

our %instTypeBitsMap = (
	"m1.small" => 32,
	"m1.large" => 64,
	"m1.xlarge" => 64,
	"c1.medium" => 32,
	"c1.xlarge" => 64,
	"m2.xlarge" => 64,
	"m2.2xlarge" => 64,
	"m2.4xlarge" => 64,
	"cc1.4xlarge" => 64
);

##
# Return the appropriate configuration string for setting the number of fields
# to bin on.  This depends on the Hadoop version.
#
sub partitionConf($) {
	my $binFields = shift;
	my @vers = split(/[^0-9]+/, $hadoopVersion);
	scalar(@vers) >= 2 && scalar(@vers <= 5) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	my $hadoop18Partition = "num.key.fields.for.partition=$binFields";
	my $hadoop19Partition = "mapred.text.key.partitioner.options=-k1,$binFields";
	if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return $hadoop18Partition;
	}
	return $hadoop19Partition;
}

##
# Return the parameter used to configure Hadoop.  In older versions it
# was -jobconf; in newer versions, it's -D.
#
sub confParam() {
	my @vers = split(/[^0-9]+/, $hadoopVersion);
	scalar(@vers) >= 2 && scalar(@vers <= 5) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return "-jobconf\", \"";
	}
	return "-D\", \"";
}

##
# Return the parameter used to ask streaming Hadoop to cache a file.
#
sub cacheFile() {
	my @vers = split(/[^0-9]+/, $hadoopVersion);
	scalar(@vers) >= 2 && scalar(@vers <= 5) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	#if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return "-cacheFile";
	#}
	#return "-files";
}

sub validateInstType($) {
	defined($instTypeNumCores{$_[0]}) || die "Bad --instance-type: \"$_[0]\"\n";
}

sub instanceTypeBits($) {
	defined($instTypeBitsMap{$_[0]}) || die "Bad --instance-type: \"$_[0]\"\n";
	return $instTypeBitsMap{$_[0]};
}

$hadoopVersion = "0.20.205" if !defined($hadoopVersion) || $hadoopVersion eq "";
my $appDir = "$app-emr/$VERSION";
$accessKey = $ENV{AWS_ACCESS_KEY_ID} if
	$accessKey eq "" && $awsEnv && defined($ENV{AWS_ACCESS_KEY_ID});
$accessKey = $ENV{AWS_ACCESS_ID} if
	$accessKey eq "" && $awsEnv && defined($ENV{AWS_ACCESS_ID});
$secretKey = $ENV{AWS_SECRET_ACCESS_KEY} if
	$secretKey eq "" && $awsEnv && defined($ENV{AWS_SECRET_ACCESS_KEY});
$secretKey = $ENV{AWS_ACCESS_KEY} if
	$secretKey eq "" && $awsEnv && defined($ENV{AWS_ACCESS_KEY});
$name = "$APP-$VERSION" if $name eq "";
$qual = "phred33" if $qual eq "";
($qual eq "phred33" || $qual eq "phred64" || $qual eq "solexa64") ||
	dieusage("Bad quality type: $qual", $usage, 1);
$instType = "c1.xlarge" if $instType eq "";
validateInstType($instType);
$cores = 1 if $cores == 0 && $localJob;
$cores = ($instTypeNumCores{$instType} || 1) if $cores == 0;
$cores > 0 || die;
$swap = ($instTypeSwap{$instType} || 0) if $swap == 0;
$reducersPerNode = $cores if $reducersPerNode == 0;
$reducersPerNode > 0 || die;
$partitionLen = 1000000 if $partitionLen == 0;
$bt_args = "-m 1" if $bt_args eq "";
$ref eq "" || $ref =~ /\.jar$/ || dieusage("--reference must end with .jar", $usage, 1);
$numNodes = "1" if !$numNodes;
my $totalNodes = 0;
if(index($numNodes, ',') != -1) {
	my @nn = split(/,/, $numNodes);
	for my $i (1..scalar($#nn)) {
		$totalNodes += int($nn[$i]);
	}
} else {
	$totalNodes = int($numNodes);
}
$totalNodes > 0 || die "Bad total number of cluster nodes: $totalNodes";
my $R_VER = "3.0.1";
$rUrl = "S3N://$appDir/R-${R_VER}.tar.gz";
$family = "poisson" if $family eq "";
validateFamily($family);
$norm = "upper-quartile" if $norm eq "";
validateNorm($norm);
$ivalModel = "intersect" if $ivalModel eq "";
$ivalModel = xformIvalModel($ivalModel);
$top = 50 if $top == 0;
$top >= 1 || die "--top must be >= 1; was $top\n";
$influence = 1 if $influence == 0;
$maxalns = 350000 if $maxalns == 0;
$partbin = 200 if $partbin == 0;
$justCount = 0 unless(defined($justCount));
$pairedTest == 0 || $permTest == 0 ||
	die "Cannot specify both --paired-ttest and non-zero --perm-tests\n";
$justAlign = 0 unless(defined($justAlign));
$resumeAlign = 0 unless(defined($resumeAlign));
$preprocess = 0 unless(defined($preprocess));
$justPreprocess = 0 unless(defined($justPreprocess));
$preprocStop = 0 unless(defined($preprocStop));
$preprocOutput eq "" || $preprocess ||
	warning( "Warning: --pre-output is specified but --preprocess is not");
$preprocCompress eq "" || $preprocess ||
	warning("Warning: --pre-compress is specified but --preprocess is not");
$preprocStop == 0 || $preprocess ||
	warning("Warning: --pre-stop is specified but --preprocess is not");
$preprocMax == 0 || $preprocess ||
	warning("Warning: --pre-filemax is specified but --preprocess is not");
$preprocCompress = "gzip" if $preprocCompress eq "";
$preprocCompress = "gzip" if $preprocCompress eq "gz";
$preprocMax = 500000 if !$preprocMax;
$preprocCompress eq "gzip" || $preprocCompress eq "none" ||
	dieusage("--pre-compress must be \"gzip\" or \"none\"", $usage, 1);
$tempdir = "/tmp/$app-$randstr" unless $tempdir ne "";
my $scriptTempdir = "$tempdir/invoke.scripts";
mkpath($scriptTempdir);
if(!$hadoopJob && !$localJob) {
	$slaveTempdir = "/mnt/$$" if $slaveTempdir eq "";
} else {
	$slaveTempdir = "$tempdir" if $slaveTempdir eq "";
}
-d $tempdir || die "Could not create temporary directory \"$tempdir\"\n";
if(!$hadoopJob && !$localJob) {
	if($waitJob) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--alive";
	}
	unless($noEmrDebugging) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--enable-debugging";
	}
}

my $failAction = "TERMINATE_JOB_FLOW";
$failAction = "CANCEL_AND_WAIT" if $waitJob;

($discardReads >= 0.0 && $discardReads <= 1.0) ||
	die "--discard-reads must be in [0,1], was: $discardReads\n";
length("$discardReads") > 0 || die "--discard-reads was empty\n";

##
# Parse a URL, extracting the protocol and type of program that will
# be needed to download it.
#
sub parse_url($) {
	my $s = shift;
	defined($s) || croak();
	my @ss = split(/[:]/, $s);
	if($ss[0] =~ /s3n?/i) {
		return "s3";
	} elsif($ss[0] =~ /hdfs/i) {
		return "hdfs";
	} else {
		return "local";
	}
}

$input = absPath($input);
$output = absPath($output);
$intermediate = absPath($intermediate);
$ref = absPath($ref);
$indexLocal = absPath($indexLocal);
$preprocOutput = absPath($preprocOutput);
$tempdir = absPath($tempdir);
$count = absPath($count);
$ivalLocal = absPath($ivalLocal);

my $resume = $resumeAlign || $resumeOlap || $resumeNormal ||
             $resumeStats || $resumeSumm;

#
# Work out which phases are going to be executed
#
my %stages = (
	"preprocess"  => 0,
	"align"       => 0,
	"overlap"     => 0,
	"normalize"   => 0,
	"statistics"  => 0,
	"summarize"   => 0,
	"postprocess" => 0
);

my ($firstStage, $lastStage) = ("", "");
if($justPreprocess) {
	$stages{preprocess} = 1;
} elsif($justAlign) {
	# --just-align specified.  Either preprocess and align (input =
	# manifest) or just align (input = preprocessed reads).
	$stages{preprocess} = 1 if $preprocess;
	$stages{align} = 1;
} elsif($resumeAlign) {
	$stages{overlap} = 1;
	$stages{normalize} = 1;
	$stages{statistics} = 1;
	$stages{summarize} = 1;
	$stages{postprocess} = 1;
} elsif($resumeOlap) {
	$stages{normalize} = 1;
	$stages{statistics} = 1;
	$stages{summarize} = 1;
	$stages{postprocess} = 1;
} elsif($resumeNormal) {
	$stages{statistics} = 1;
	$stages{summarize} = 1;
	$stages{postprocess} = 1;
} elsif($resumeStats) {
	$stages{summarize} = 1;
	$stages{postprocess} = 1;
} elsif(!$resumeSumm) {
	$stages{preprocess} = 1 if $preprocess;
	$stages{align} = 1;
	$stages{overlap} = 1;
	$stages{normalize} = 1;
	$stages{statistics} = 1;
	$stages{summarize} = 1;
	$stages{postprocess} = 1;
}
# Determine first and last stages
for my $s ("preprocess", "align", "overlap", "normalize", "statistics", "summarize", "postprocess") {
	if(defined($stages{$s}) && $stages{$s} != 0) {
		$firstStage = $s if $firstStage eq "";
		$lastStage = $s;
	}
}
$firstStage ne "" || die;
$lastStage ne "" || die;
my $numStages = 0;
for my $k (keys %stages) { $numStages += $stages{$k}; }

$useFastqDump = $stages{preprocess};
$useSamtools = $stages{align} && $sampass;
my $useBowtie = $stages{align};
my $useR = $stages{overlap} || $stages{statistics} || $stages{postprocess};
$bowtie     =~ s/^~/$ENV{HOME}/;
$samtools   =~ s/^~/$ENV{HOME}/;
$Rhome      =~ s/^~/$ENV{HOME}/;
$fastq_dump =~ s/^~/$ENV{HOME}/;
if($test) {
	$verbose = 1;
	my $failed = 0;
	if($localJob || $hadoopJob) {
		# Check for binaries
		$bowtie     = checkExe($bowtie,     "bowtie",    "${pre}BOWTIE_HOME",     "",    "--bowtie"  ,    0);
		$samtools   = checkExe($samtools,   "samtools",  "${pre}SAMTOOLS_HOME",   "",    "--samtools",    0) if $useSamtools;
		$Rhome      = checkExe($Rhome,      "Rscript",   "${pre}RHOME",           "bin", "--Rhome"   ,    0);
		$fastq_dump = checkExe($fastq_dump, "fastq-dump","${pre}SRATOOLKIT_HOME", "",    "--fastq-dump",  0, 4);
		$msg->("Summary:\n");
		$msgf->("  bowtie: %s\n",     ($bowtie     ne "" ? "INSTALLED at $bowtie"           : "NOT INSTALLED"));
		$msgf->("  samtools: %s\n",   ($samtools   ne "" ? "INSTALLED at $samtools"         : "NOT INSTALLED")) if $useSamtools;
		$msgf->("  R: %s\n",          ($Rhome      ne "" ? "INSTALLED with RHOME at $Rhome" : "NOT INSTALLED"));
		$msgf->("  fastq-dump: %s\n", ($fastq_dump ne "" ? "INSTALLED at $fastq_dump"       : "NOT INSTALLED")) if $useFastqDump;
		$msg->("Hadoop note: executables must be runnable via the SAME PATH on all nodes.\n") if $hadoopJob;
		$failed = $bowtie eq "" || ($useSamtools && $samtools eq "") || $Rhome eq ""; # || $sra eq "";
		if($failed) {
			$msg->("FAILED install test\n");
		} elsif($fastq_dump eq "") {
			$msg->("PASSED WITH ***WARNING***: SRA toolkit fastq-dump not found; .sra inputs won't work but others will\n");
		} else {
			$msg->("PASSED install test\n");
		}
	} else {
		$emrScript = checkExe($emrScript, "elastic-mapreduce", "${pre}EMR_HOME", "", "--emr-script", 0);
		$msg->("Summary:\n");
		$msgf->("  elastic-mapreduce: %s\n", ($emrScript ne "" ? "INSTALLED at $emrScript" : "NOT INSTALLED"));
		$failed = $emrScript eq "";
		$msg->($failed ? "FAILED install test\n" : "PASSED install test\n");
	}
	exit $failed ? 1 : 0;
}
if($localJob || $hadoopJob) {
	# Check for binaries
	$bowtie     = checkExe($bowtie,     "bowtie",     "${pre}BOWTIE_HOME",     "",    "--bowtie"  ,      1) if $useBowtie;
	$samtools   = checkExe($samtools,   "samtools",   "${pre}SAMTOOLS_HOME",   "",    "--samtools",      1) if $useSamtools;
	$Rhome      = checkExe($Rhome,      "Rscript",    "${pre}RHOME",           "bin", "--Rhome",         1) if $useR;
	$fastq_dump = checkExe($fastq_dump, "fastq-dump", "${pre}SRATOOLKIT_HOME", "",    "--fastq-dump", 0, 4) if $useFastqDump;
	if($fastq_dump eq "") {
		print STDERR "***WARNING***\n";
		print STDERR "***WARNING***: fastq-dump not found; .sra inputs won't work but others will\n";
		print STDERR "***WARNING***\n";
	}
} else {
	$emrScript = checkExe($emrScript, "elastic-mapreduce", "${pre}EMR_HOME", "", "--emr-script", 1);
}

# Parse input, output and intermediate directories
if($inputLocal eq "") {
	defined($input) || die;
	$input = "hdfs://$input" if parse_url($input) eq "local";
} else {
	parse_url($inputLocal) eq "local" || die "--input-local specified non-local URL: $inputLocal\n";
	$input = $inputLocal;
}
if($outputLocal eq "") {
	defined($output) || die;
	$output = "hdfs://$output" if parse_url($output) eq "local";
} else {
	parse_url($outputLocal) eq "local" || die "--output-local specified non-local URL: $outputLocal\n";
	$output = $outputLocal;
}
if(!$hadoopJob && !$localJob) {
	# If the user hasn't specified --no-logs and hasn't specified a --log-uri
	# via --emr-args, then specify a subdirectory of the output directory as
	# the log dir.
	$logs = "${output}_logs" if $logs eq "";
	if(!$noLogs && $emrArgs !~ /-log-uri/) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--log-uri $logs ";
	}
	my @vers = split(/[^0-9]+/, $hadoopVersion);
	if($vers[0] < 1 && $vers[1] < 20) {
		die "Error: Myrna not compatible with Hadoop versions before 0.20";
	}
	scalar(@vers) >= 2 && scalar(@vers <= 5) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	if     ($vers[0] == 1 && $vers[1] == 0 && scalar(@vers) > 2 && $vers[2] == 3) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--hadoop-version=1.0.3 --ami-version 2.3 ";
	} elsif($vers[0] == 0 && $vers[1] == 20 && scalar(@vers) > 2 && $vers[2] == 205) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--hadoop-version=0.20.205 --ami-version 2.0 ";
	} elsif($vers[0] == 0 && $vers[1] == 20) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--hadoop-version=0.20 --ami-version 1.0 ";
	} else {
		print STDERR "Error: Expected Hadoop version 0.20 or 0.20.205, got $hadoopVersion\n";
		exit 1;
	}
}
my $intermediateSet = ($intermediate ne "" || $intermediateLocal ne "");
if($intermediateLocal eq "") {
	if($intermediate eq "") {
		if($localJob) {
			$intermediate = "$tempdir/$app/intermediate/$$";
		} else {
			$intermediate = "hdfs:///$app/intermediate/$$";
		}
	}
} else {
	parse_url($intermediateLocal) eq "local" || die "--intermediate-local specified non-local URL: $intermediateLocal\n";
	$intermediate = $intermediateLocal;
}

$output ne "" || dieusage("Must specify --output", $usage, 1);
if(!$localJob && !$hadoopJob) {
	parse_url($output) eq "s3" || die "Error: In cloud mode, --output path must be an S3 path; was: $output\n";
}
if($resume && $intermediateSet) {
	die "Cannot specify both --resume-* and --intermediate; specify intermediate directory\n".
	    "to be resumed using --input.  --intermediate is automatically set to --input\n";
}
if($intermediate eq "" && $localJob) {
	$intermediate = "$tempdir/$app/intermediate";
} elsif($intermediate eq "") {
	$intermediate = "hdfs:///tmp/$app" if $intermediate eq "";
}
$input  ne "" || dieusage("Must specify --input", $usage, 1);
if(!$localJob && !$hadoopJob) {
	parse_url($input) eq "s3" || die "Error: In cloud mode, --input path must be an S3 path; was: $input\n";
}
if($localJob && !$justPreprocess) {
	$ivalLocal ne "" || die "Must specify --ivals-local when --local-job is specified\n";
	$indexLocal ne "" || die "Must specify --index-local when --local-job is specified\n";
}

sub checkArgs($$) {
	my ($args, $param) = @_;
	if($args =~ /[\t\n\r]/) {
		die "$param \"$args\" has one or more illegal whitespace characters\n";
	} elsif($args =~ /[_]/) {
		$emsg->("$param \"$args\" contains underscores; this may confuse $APP\n");
	}
	$args =~ s/ /_/g;
	$args =~ /\s/ && die "$param still has whitespace after space conversion: \"$args\"\n";
	return $args;
}

sub upperize($) {
	my $url = shift;
	$url =~ s/^s3n/S3N/;
	$url =~ s/^s3/S3/;
	$url =~ s/^hdfs/HDFS/;
	return $url;
}

#
# If the caller has provided all the relevant individual parameters,
# bypass the credentials file.
#
my $credentialsFile = "";
if($credentials eq "" && $accessKey ne "" && $secretKey ne "") {
	my ($regionStr, $keypairStr, $keypairFileStr) = ("", "", "");
	$regionStr      = "--region=$zone"               if $zone ne "";
	$keypairStr     = "--key-pair=$keypair"          if $keypair ne "";
	$keypairFileStr = "--key-pair-file=$keypairFile" if $keypairFile ne "";
	$credentials = "--access-id=$accessKey --private-key=$secretKey $keypairStr $keypairFileStr $regionStr";
} elsif($credentials ne "") {
	$credentialsFile = $credentials;
	$credentials = "-c $credentials";
}

my $intermediateUpper = upperize($intermediate);
$ref ne "" || ($indexLocal ne "" && $ivalLocal ne "") || ($bin > 0 && $sampass) || $justPreprocess || $localJob ||
	dieusage("Must specify --reference OR both --bin and --sam-passthrough OR --just-preprocess", $usage, 1);
$ref eq "" || $ref =~ /\.jar$/ || dieusage("--reference must end with .jar", $usage, 1);
$indexLocal eq "" || -f "$indexLocal.1.ebwt" || dieusage("--index-local \"$indexLocal\" path doesn't point to an index", $usage, 1);
$ivalLocal eq "" || -d $ivalLocal || dieusage("--ival-local \"$ivalLocal\" path doesn't point to a directory", $usage, 1);

if(!$localJob && !$hadoopJob && defined($ref) && $ref ne "") {
	parse_url($ref) eq "s3" || die "Error: In cloud mode, --reference path must be an S3 path; was: $ref\n";
}

# Remove inline credentials from URLs
$input =~ s/:\/\/[^\/]@//;
$output =~ s/:\/\/[^\/]@//;
$ref =~ s/:\/\/[^\/]@//;
my $refIdx = $ref;
$refIdx =~ s/\.jar$/.idx.jar/ if $splitJars;
my $refIval = $ref;
$refIval =~ s/\.jar$/.ivals.jar/ if $splitJars;
my $refIvalUpper = upperize($refIval);
my $refIdxUpper = upperize($refIdx);

# Remove trailing slashes from output
$output =~ s/[\/]+$//;

my $hadoop = "";
my $hadoopStreamingJar = "";
if(!$localJob && !$hadoopJob) {
} elsif($hadoopJob) {
	# Look for hadoop script here on the master
	if($hadoop_arg eq "") {
		if(defined($ENV{HADOOP_HOME})) {
			$hadoop = "$ENV{HADOOP_HOME}/bin/hadoop";
			chomp($hadoop);
		}
		if($hadoop eq "" || system("$hadoop version 2>/dev/null >/dev/null") != 0) {
			$hadoop = `which hadoop 2>/dev/null`;
			chomp($hadoop);
		}
	} else {
		$hadoop = $hadoop_arg;
	}
	if(system("$hadoop version 2>/dev/null >/dev/null") != 0) {
		if($hadoop_arg ne "") {
			die "Specified --hadoop: '$hadoop_arg' cannot be run\n";
		} else {
			die "Cannot find working 'hadoop' in PATH or HADOOP_HOME/bin; please specify --hadoop\n";
		}
	}
	# Now look for hadoop streaming jar file here on the master
	my $hadoopHome;
	if($hadoopStreamingJar_arg eq "") {
		$hadoopHome = `dirname $hadoop`;
		$hadoopHome = `dirname $hadoopHome`;
		chomp($hadoopHome);
		$hadoopStreamingJar = "";
		my @hadoopStreamingJars;
		@hadoopStreamingJars = <$hadoopHome/contrib/streaming/hadoop-*-streaming.jar>;
		if(scalar(@hadoopStreamingJars) == 0) {
			# Alternate naming scheme
			@hadoopStreamingJars = <$hadoopHome/contrib/streaming/hadoop-streaming-*.jar>;
		}
		if(scalar(@hadoopStreamingJars) == 0) {
			# Alternate naming scheme
			@hadoopStreamingJars = <$hadoopHome/contrib/streaming/hadoop-streaming.jar>;
		}
		$hadoopStreamingJar = $hadoopStreamingJars[0] if scalar(@hadoopStreamingJars) > 0;
	} else {
		$hadoopStreamingJar = $hadoopStreamingJar_arg;
	}
	unless(-f $hadoopStreamingJar) {
		if($hadoopStreamingJar_arg ne "") {
			die "Specified --streaming-jar: '$hadoopStreamingJar_arg' cannot be found\n";
		} else {
			die "Cannot find streaming jar in $hadoopHome/contrib/streaming; please specify --streaming-jar\n";
		}
	}
	$hadoopStreamingJar =~ /hadoop-(.*)-streaming\.jar$/; $hadoopVersion = $1;
	if(!defined($hadoopVersion)) {
		# Alternate naming scheme
		$hadoopStreamingJar =~ /hadoop-streaming-(.*)\.jar$/; $hadoopVersion = $1;
	}
	defined($hadoopVersion) || die "Could not parse streaming jar name: $hadoopStreamingJar";
	# Hadoop version might be as simlpe as 0.20 or as complex as 0.20.2+737
	$emsg->("Detected Hadoop version '$hadoopVersion'") if $verbose;
} elsif($localJob) {
	system("sort < /dev/null") == 0 || die "Could not invoke 'sort'; is it in the PATH?\n";
}

# Set up the --samtools, --bowtie, and --R arguments for each script invocation
my $bowtie_arg = "";
my $samtools_arg = "";
my $R_arg = "";
my $fastq_dump_arg = "";
if($localJob || $hadoopJob) {
	if($useSamtools) {
		$samtools ne "" || die;
		$msg->("$APP expects 'samtools' to be at path $samtools on the workers\n") if $hadoopJob;
		$samtools_arg = "--samtools $samtools";
	}

	if($useBowtie) {
		$bowtie ne "" || die;
		$msg->("$APP expects 'bowtie' to be at path $bowtie on the workers\n") if $hadoopJob;
		$bowtie_arg = "--bowtie $bowtie";
	}
	
	if($useR) {
		$Rhome ne "" || die;
		$msg->("$APP expects 'Rscript' to be at path $Rhome on the workers\n") unless $localJob;
		$R_arg = "--R $Rhome";
	}
	
	if($useFastqDump) {
		$fastq_dump ne "" || die;
		$msg->("$APP expects 'fastq-dump' to be at path $fastq_dump on the workers\n") unless $localJob;
		$fastq_dump_arg = "--fastq-dump $fastq_dump";
	}
}

# Set up name of streaming jar for EMR mode
my $emrStreamJar = "/home/hadoop/contrib/streaming/hadoop-streaming-$hadoopVersion.jar";
if($hadoopVersion eq "0.20" || $hadoopVersion eq "0.18") {
	$emrStreamJar = "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar";
}

# Set up some variables to save us some typing:

my $cachef = cacheFile();
my $ec2CacheFiles =
qq!	"$cachef", "s3n://$appDir/Get.pm#Get.pm",
	"$cachef", "s3n://$appDir/Counters.pm#Counters.pm",
	"$cachef", "s3n://$appDir/Util.pm#Util.pm",
	"$cachef", "s3n://$appDir/Tools.pm#Tools.pm",
	"$cachef", "s3n://$appDir/AWS.pm#AWS.pm"!;

my $hadoopCacheFiles = qq! \\
	-file '$Bin/Get.pm' \\
	-file '$Bin/Counters.pm' \\
	-file '$Bin/Util.pm' \\
	-file '$Bin/Tools.pm' \\
	-file '$Bin/AWS.pm' \\
!;

my $inputPreproc = $input;
my $outputPreproc = ($preprocOutput ne "" ? $preprocOutput : "$intermediate/preproc");
$outputPreproc = $output if $justPreprocess;
my $outputPreprocUpper = upperize($outputPreproc);
my $bits = instanceTypeBits($instType);
$bits == 32 || $bits == 64 || die "Bad samtoolsBits: $bits\n";
my $forceStr = ($dontForce ? "" : "--force");
my $keepAllStr = $keepAll ? "--keep-all" : "";
$samLabRg = $samLabRg ? "--label-rg" : "";

my $preprocArgs = "";
$preprocArgs .= " --compress=$preprocCompress";
$preprocArgs .= " --stop=$preprocStop";
$preprocArgs .= " --maxperfile=$preprocMax";
$preprocArgs .= " --s";
$preprocArgs .= " --push=$outputPreprocUpper";

my $samtoolsCacheFiles = qq!"$cachef",   "s3n://$appDir/samtools$bits#samtools"!;
my $sraCacheFiles      = qq!"$cachef",   "s3n://$appDir/fastq-dump$bits#fastq-dump"!;

my $conf = confParam();

my $preprocessJson = qq!
{
  "Name": "Preprocess short reads",
  "ActionOnFailure": "$failAction",
  "HadoopJarStep": {
    "Jar": "$emrStreamJar",
    "Args": [
      "${conf}mapred.reduce.tasks=0",
      "-input",       "$inputPreproc",
      "-output",      "$outputPreproc",
      "-mapper",      "s3n://$appDir/Copy.pl $preprocArgs",
      "-inputformat", "org.apache.hadoop.mapred.lib.NLineInputFormat",
      $ec2CacheFiles,
      $sraCacheFiles,
      $samtoolsCacheFiles
    ]
  }
}!;

my $preprocessHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Preprocess
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D mapred.reduce.tasks=0 \\
	-D mapred.job.name='Preprocess $inputPreproc' \\
	-input $inputPreproc \\
	-output $outputPreproc \\
	-mapper '$Bin/Copy.pl $samtools_arg $fastq_dump_arg $preprocArgs' \\
	$hadoopCacheFiles \\
	-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Preprocess stage" && exit 1
phase=`expr \$phase + 1`
!;

my $preprocessSh = qq!
perl $Bin/MapWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Preprocess \\
	--input $inputPreproc \\
	--output $outputPreproc \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--line-by-line \\
	--silent-skipping \\
	$keepAllStr \\
	$forceStr \\
	--mappers $cores -- \\
		perl $Bin/Copy.pl \\
			--compress=$preprocCompress \\
			--stop=$preprocStop \\
			--maxperfile $preprocMax \\
			$fastq_dump_arg \\
			$samLabRg \\
			--push $outputPreproc \\
			--counters ${output}_counters/counters.txt

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Preprocess stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputPreproc (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputPreproc
fi
phase=`expr \$phase + 1`
!;

my $inputAlign  = (($firstStage eq "align") ? $input  : $outputPreproc);
my $outputAlign = (($lastStage  eq "align") ? $output : "$intermediate/align");
$truncate = max($truncate, $truncateDiscard);
$truncateDiscard = $truncateDiscard > 0 ? "--discard-small" : "";
$sampass = ($sampass ? "--sampass" : "");
$poolReplicates = $poolReplicates ? "--pool-replicates" : "";
$poolTechReplicates = $poolTechReplicates ? "--pool-tech-replicates" : "";
my $globalsUpper = upperize("$intermediateUpper/globals");

my $alignArgs = "";
$alignArgs .= " --discard-reads=$discardReads";
$alignArgs .= " --ref=$refIdxUpper";
$alignArgs .= " --destdir=$slaveTempdir";
$alignArgs .= " --partlen=$partitionLen";
$alignArgs .= " --qual=$qual";
$alignArgs .= " --truncate=$truncate";
$alignArgs .= " --globals=$globalsUpper";
$alignArgs .= " --discard-mate=$discardMate";
$alignArgs .= " --pool-trim-length=$poolTrimLen";
$alignArgs .= " $sampass";
$alignArgs .= " $poolReplicates";
$alignArgs .= " $poolTechReplicates";
$alignArgs .= " $truncateDiscard";
$alignArgs .= " --";
$alignArgs .= " --partition -$partitionLen";
$alignArgs .= " --mm -t --hadoopout --startverbose";
$alignArgs .= " $bt_args";

my $alignJson = qq!
{
  "Name": "$APP Step 1: Align with Bowtie", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}mapred.reduce.tasks=0",
      "-input",       "$inputAlign",
      "-output",      "$outputAlign",
      "-mapper",      "s3n://$appDir/Align.pl $alignArgs",
      "$cachef",   "s3n://$appDir/bowtie$bits#bowtie",
      $ec2CacheFiles
    ] 
  }
}!;

my $alignHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Align
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D mapred.reduce.tasks=0 \\
	-D mapred.job.name='Align $inputAlign' \\
	-input $inputAlign \\
	-output $outputAlign \\
	-mapper '$Bin/Align.pl $bowtie_arg $alignArgs' \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Align streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $preprocOutputSpecified = $preprocOutput ne "" ? "1" : "0";

my $alignSh = qq!
perl $Bin/MapWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Align \\
	--input $inputAlign \\
	--output $outputAlign \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	$keepAllStr \\
	$forceStr \\
	--mappers $cores -- \\
		perl $Bin/Align.pl \\
			$bowtie_arg \\
			--discard-reads=$discardReads \\
			--index-local=$indexLocal \\
			--partlen=$partitionLen \\
			--qual=$qual \\
			--counters ${output}_counters/counters.txt \\
			--truncate=$truncate \\
			$truncateDiscard \\
			--globals=$intermediate/globals \\
			--discard-mate=$discardMate \\
			--pool-trim-length=$poolTrimLen \\
			$sampass \\
			$poolReplicates \\
			$poolTechReplicates \\
			-- \\
			--partition $partitionLen \\
			--mm -t --hadoopout --startverbose \\
			$bt_args

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Align stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 -a $preprocOutputSpecified -eq 0 ] ; then
	echo "Removing $inputAlign (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputAlign
fi
phase=`expr \$phase + 1`
!;

my $binstr = ($bin > 0 ? "--bin" : "");
my $assignTasks = $totalNodes * $reducersPerNode * 4;
my $inputOlap  = (($firstStage eq "overlap") ? $input  : $outputAlign);
my $outputOlap = (($lastStage  eq "overlap") ? $output : "$intermediate/olaps");
$profile = $profile ? "--profile" : "";

my $olapArgs = "--ivaljar=$refIvalUpper";
$olapArgs   .= " --maxalns=$maxalns";
$olapArgs   .= " --partbin=$partbin";
$olapArgs   .= " --influence=$influence";
$olapArgs   .= " --ival-model=$ivalModel";
$olapArgs   .= " --globals=$globalsUpper";
$olapArgs   .= " $binstr";
$olapArgs   .= " --binwidth=$bin";
$olapArgs   .= " --destdir=$slaveTempdir";
$olapArgs   .= " --globals=$globalsUpper";
$olapArgs   .= " $profile";
$olapArgs   .= " $fromStr";

my $olapPartitionConf = partitionConf(2);
my $olapJson = qq!
{
  "Name": "$APP Step 2: Calculate overlaps", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [
      "${conf}stream.num.map.output.key.fields=3",
      "${conf}$olapPartitionConf",
      "${conf}mapred.reduce.tasks=$assignTasks",
      "-input",       "$inputOlap",
      "-output",      "$outputOlap",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Assign.pl $olapArgs --Rfetch=$rUrl",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner", 
      $ec2CacheFiles,
      "$cachef",   "s3n://$appDir/Assign.R#Assign.R"
    ] 
  }
}!;

my $olapHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Overlap
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=3 \\
	-D $olapPartitionConf \\
	-D mapred.job.name='Overlap $inputOlap' \\
	-D mapred.reduce.tasks=$assignTasks \\
	-input $inputOlap \\
	-output $outputOlap \\
	-mapper cat \\
	-reducer '$Bin/Assign.pl $olapArgs $R_arg' \\
	-file '$Bin/Assign.R' \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Overlap streaming job" && exit 1
phase=`expr \$phase + 1`
!;

$externalSort = $externalSort ? "--external-sort" : "";
my $olapSh = qq!
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Overlap \\
	--input $inputOlap \\
	--output $outputOlap \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--reducers $cores \\
	--tasks $assignTasks \\
	--bin-fields 2 \\
	--sort-fields 3 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/Assign.pl \\
			--ivals=$ivalLocal \\
			--maxalns=$maxalns \\
			--partbin=$partbin \\
			--influence=$influence \\
			--ival-model=$ivalModel \\
			$fromStr \\
			$binstr \\
			--globals=$intermediate/globals \\
			$profile \\
			--binwidth $bin \\
			--counters ${output}_counters/counters.txt \\
			$R_arg

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Overlap stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputOlap (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputOlap
fi
phase=`expr \$phase + 1`
!;

my $normalTasks  = $totalNodes * $reducersPerNode * 2;
my $inputNormal  = (($firstStage eq "normalize") ? $input  : $outputOlap);
my $outputNormal = (($lastStage  eq "normalize") ? $output : "$intermediate/normal");
my $outputCount = ($count eq "" ? "$intermediate/count" : $count);
my $outputCountUpper = upperize($outputCount);
my $normalType = "ltot";
$normalType = "lup" if $norm eq "upper-quartile";
$normalType = "lmed" if $norm eq "median";
$normalType = "ltot" if $norm eq "total";

my $normalArgs = "";
$normalArgs   .= " --normal=$normalType";
$normalArgs   .= " --output=$outputCountUpper";

my $normalizePartitionConf = partitionConf(1);
my $normalizeJson = qq!
{
  "Name": "$APP Step 3: Normalize", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}stream.num.map.output.key.fields=2",
      "${conf}$normalizePartitionConf",
      "${conf}mapred.reduce.tasks=$normalTasks",
      "-input",       "$inputNormal",
      "-output",      "$outputNormal",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Normal.pl $normalArgs",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
      $ec2CacheFiles
    ] 
  }
}!;

my $normalizeHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Normalize
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=2 \\
	-D $normalizePartitionConf \\
	-D mapred.job.name='Normalize $inputNormal' \\
	-D mapred.reduce.tasks=$normalTasks \\
	-input $inputNormal \\
	-output $outputNormal \\
	-mapper cat \\
	-reducer '$Bin/Normal.pl $normalArgs' \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Normalize streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $normalizeSh = qq!
if [ $dontForce -eq 0 -a -d $outputCount ] ; then
	echo "Removing directory $outputCount due to -force" >&2;
	rm -rf $outputCount
	mkdir -p $outputCount
fi
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Normalize \\
	--input $inputNormal \\
	--output $outputNormal \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--reducers $cores \\
	--tasks $normalTasks \\
	--bin-fields 1 \\
	--sort-fields 2 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/Normal.pl \\
			--normal=$normalType \\
			--output=$outputCount \\
			--counters ${output}_counters/counters.txt

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Normal stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputNormal (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputNormal
fi
phase=`expr \$phase + 1`
!;

my $statsTasks  = $totalNodes * $reducersPerNode * 4;
my $inputStats  = (($firstStage eq "statistics") ? $input  : $outputNormal);
my $outputStats = (($lastStage  eq "statistics") ? $output : "$intermediate/stats");
$bypassPvals = $bypassPvals ? "--bypass-pvals" : "";
$pairedTest = $pairedTest ? "--paired" : "";

my $statsArgs = "";
$statsArgs   .= " --family=$family";
$statsArgs   .= " --globals=$globalsUpper";
$statsArgs   .= " --destdir=$slaveTempdir";
$statsArgs   .= " --add-fudge=$addFudge";
$statsArgs   .= " --nulls=$permTest";
$statsArgs   .= " $pairedTest";
$statsArgs   .= " $bypassPvals";
$statsArgs   .= " $profile";

my $statsPartitionConf = partitionConf(1);
my $statsJson = qq!
{
  "Name": "$APP Step 4: Calculate interval statistics", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}stream.num.map.output.key.fields=2",
      "${conf}$statsPartitionConf",
      "${conf}mapred.reduce.tasks=$statsTasks",
      "-input",       "$inputStats",
      "-output",      "$outputStats",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Stats.pl $statsArgs --Rfetch=$rUrl",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
      $ec2CacheFiles,
      "$cachef",   "s3n://$appDir/Stats.R#Stats.R"
    ]
  }
}!;

my $statsHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Statistics
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=2 \\
	-D $statsPartitionConf \\
	-D mapred.job.name='Statistics $inputStats' \\
	-D mapred.reduce.tasks=$statsTasks \\
	-input $inputStats \\
	-output $outputStats \\
	-mapper cat \\
	-reducer '$Bin/Stats.pl $R_arg $statsArgs' \\
	-file '$Bin/Stats.R' \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Statistics streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $statsSh = qq!
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Statistics \\
	--input $inputStats \\
	--output $outputStats \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--reducers $cores \\
	--tasks $statsTasks \\
	--bin-fields 1 \\
	--sort-fields 2 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/Stats.pl \\
			--family=$family \\
			$R_arg \\
			--globals=$intermediate/globals \\
			--add-fudge=$addFudge \\
			--nulls=$permTest \\
			--counters ${output}_counters/counters.txt \\
			$pairedTest \\
			$profile \\
			$bypassPvals

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Statistics stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $intermediate/globals (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $intermediate/globals
fi
phase=`expr \$phase + 1`
!;

my $inputSumm  = (($firstStage eq "summarize") ? $input  : $outputStats);
my $outputSumm = (($lastStage  eq "summarize") ? $output : "$intermediate/summ");

my $outputChosen = ($chosen eq "" ? "$intermediate/chosen" : $chosen);
my $outputChosenUpper = upperize($outputChosen);

my $summArgs = "";
$summArgs .= " --top=$top";
$summArgs .= " --nulls=$permTest";
$summArgs .= " --chosen-genes=$outputChosenUpper";

my $summarizePartitionConf = partitionConf(1);
my $summarizeJson = qq!
{
  "Name": "$APP Step 5: Summarize", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}stream.num.map.output.key.fields=2",
      "${conf}$summarizePartitionConf",
      "${conf}mapred.reduce.tasks=1",
      "-input",       "$inputSumm",
      "-output",      "$outputSumm",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Summarize.pl $summArgs",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
      $ec2CacheFiles
    ] 
  }
}!;

my $summarizeHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Summarize
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=2 \\
	-D $summarizePartitionConf \\
	-D mapred.job.name='Summarize $inputSumm' \\
	-D mapred.reduce.tasks=1 \\
	-input $inputSumm \\
	-output $outputSumm \\
	-mapper cat \\
	-reducer '$Bin/Summarize.pl $summArgs' \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Summarize streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $summarizeSh = qq!
if [ $dontForce -eq 0 -a -d $outputChosen ] ; then
	echo "Removing directory $outputChosen due to -force" >&2;
	rm -rf $outputChosen
	mkdir -p $outputChosen
fi
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Summarize \\
	--input $inputSumm \\
	--output $outputSumm \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--reducers $cores \\
	--tasks 1 \\
	--bin-fields 1 \\
	--sort-fields 2 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/Summarize.pl \\
			--top=$top \\
			--nulls=$permTest \\
			--chosen-genes=$outputChosen \\
			--counters ${output}_counters/counters.txt

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Summarize stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputSumm (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputSumm
fi
phase=`expr \$phase + 1`
!;

my $inputDummy = "s3n://$app-emr/dummy-input";
my $outputUpper = upperize($output);
my $countersArgs = "";
$countersArgs   .= " --output=${outputUpper}_${app}_counters";

my $countersJson = qq!
{
  "Name": "Get counters", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}mapred.reduce.tasks=1",
      "-input",       "$inputDummy",
      "-output",      "${output}_${app}_counters/ignoreme1",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Counters.pl $countersArgs",
      $ec2CacheFiles
    ]
  }
}!;
my $countersSh = qq!
!;

my $inputPostproc = "$intermediate/summ";
my $inputPostprocSh = "$intermediate/summ";
$inputPostproc .= ",$intermediate/normal" unless ($ditchAlignments);
$inputPostprocSh .= " $intermediate/normal" unless ($ditchAlignments);
my $outputPostproc = "$output/${app}_results";
$ditchAlignments = $ditchAlignments ? "--no-alignments" : "";

my $postprocMapArgs = "";
$postprocMapArgs .= " --destdir=$slaveTempdir";
$postprocMapArgs .= " $ditchAlignments";
$postprocMapArgs .= " --chosen-genes=$outputChosenUpper";
my $minusLog = ($permTest == 0 ? "--minus-log" : "");
my $postprocNoGenes = ($bin == 0 ? "" : "--no-genes");

my $postprocReduceArgs = "";
$postprocReduceArgs   .= " --ivaljar=$refIvalUpper";
$postprocReduceArgs   .= " --cores=$cores";
$postprocReduceArgs   .= " --destdir=$slaveTempdir";
$postprocReduceArgs   .= " --output=$outputUpper/${app}_results";
$postprocReduceArgs   .= " --counts=$outputCountUpper";
$postprocReduceArgs   .= " $minusLog";
$postprocReduceArgs   .= " $postprocNoGenes";
$postprocReduceArgs   .= " $ditchAlignments";

my $postprocPartitionConf = partitionConf(2);
my $postprocJson = qq!
{
  "Name": "$APP Step 6: Postprocess", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "$emrStreamJar", 
    "Args": [ 
      "${conf}stream.num.map.output.key.fields=3",
      "${conf}$postprocPartitionConf",
      "${conf}mapred.reduce.tasks=1",
      "-input",       "$inputPostproc", 
      "-output",      "$output/ignoreme2",
      "-mapper",      "s3n://$appDir/PostprocessMap.pl $postprocMapArgs",
      "-reducer",     "s3n://$appDir/PostprocessReduce.pl $postprocReduceArgs --Rfetch=$rUrl",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
      "$cachef",   "s3n://$appDir/Postprocess.R#Postprocess.R",
      $ec2CacheFiles
    ] 
  }
}!;

my $postprocHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Postprocess
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=3 \\
	-D $postprocPartitionConf \\
	-D mapred.job.name='Postprocess $inputSumm' \\
	-D mapred.reduce.tasks=1 \\
	-input $inputPostproc \\
	-output $output/ignoreme2 \\
	-mapper '$Bin/PostprocessMap.pl $postprocMapArgs' \\
	-reducer '$Bin/PostprocessReduce.pl $R_arg $postprocReduceArgs' \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Postprocess streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $postprocSh = qq!
# Map step: discard irrelevant alignments
perl $Bin/MapWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Postprocess \\
	--input $inputPostproc \\
	--output ${outputPostproc}_map \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	$keepAllStr \\
	$forceStr \\
	--mappers $cores -- \\
		perl $Bin/PostprocessMap.pl \\
			--chosen-genes=$outputChosen \\
			--counters ${output}_counters/counters.txt \\
			$ditchAlignments

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Postprocess map stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputPostproc (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $intermediate/summ
	echo "Removing $outputNormal (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $intermediate/normal
fi

# Reduce step: create plots
perl $Bin/ReduceWrap.pl \\
	--input ${outputPostproc}_map \\
	--output $outputPostproc \\
	--counters ${output}_counters/counters.txt \\
	--messages myrna.local.\$\$.out \\
	--reducers $cores \\
	--tasks 1 \\
	--bin-fields 2 \\
	--sort-fields 3 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/PostprocessReduce.pl \\
			--ivals=$ivalLocal \\
			--cores=$cores \\
			$R_arg \\
			--output=$output/${app}_results \\
			--counts=$outputCount \\
			$minusLog \\
			$postprocNoGenes \\
			--counters ${output}_counters/counters.txt \\
			$ditchAlignments

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Postprocess reduce stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing ${outputPostproc}_map (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf ${outputPostproc}_map
	echo "Removing $outputCount (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $outputCount
	echo "Removing $outputChosen (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $outputChosen
fi
if [ $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $intermediate (to keep some or all of its contents, specify --keep-all or --keep-intermediates)"
	rm -rf $intermediate
fi
phase=`expr \$phase + 1`
!;

my $jsonFile = "$scriptTempdir/myrna.$$.json";
my $runJsonFile = "$scriptTempdir/myrna.$$.json.sh";
my $runHadoopFile = "$scriptTempdir/myrna.$$.hadoop.sh";
my $runLocalFile = "$scriptTempdir/myrna.$$.sh";
umask 0077;
my $json = "";
open JSON, ">$jsonFile" || die "Error: Could not open $jsonFile for writing\n";
my $sh = "";
open SH, ">$runLocalFile" || die "Error: Could not open $runLocalFile for writing\n";
my $had = "";
open HADOOP, ">$runHadoopFile" || die "Error: Could not open $runHadoopFile for writing\n";
$json .= "[";
$sh .= "#!/bin/sh\n\nphase=1\n";
$sh .= "rm -f myrna.local.\$\$.out\n";
$sh .= qq!
perl $Bin/CheckDirs.pl \\
	--input $input \\
	--intermediate $intermediate \\
	--output $output \\
	--counters ${output}_counters \\
	--messages myrna.local.\$\$.out \\
	$forceStr
!;
$had .= "#!/bin/sh\n\nphase=1\n";
#$had .= "rm -f myrna.hadoop.\$\$.out\n";
if($stages{preprocess}) {
	$json .= "," if $json ne "[";
	$json .= $preprocessJson;
	$had .= $preprocessHadoop;
	$sh .= $preprocessSh;
}
if($stages{align}) {
	$json .= "," if $json ne "[";
	$json .= $alignJson;
	$had .= $alignHadoop;
	$sh .= $alignSh;
}
if($stages{overlap}) {
	$json .= "," if $json ne "[";
	$json .= $olapJson;
	$had .= $olapHadoop;
	$sh .= $olapSh;
}
if($stages{normalize}) {
	$json .= "," if $json ne "[";
	$json .= $normalizeJson;
	$had .= $normalizeHadoop;
	$sh .= $normalizeSh;
}
if($stages{statistics}) {
	$json .= "," if $json ne "[";
	$json .= $statsJson;
	$had .= $statsHadoop;
	$sh .= $statsSh;
}
if($stages{summarize}) {
	$json .= "," if $json ne "[";
	$json .= $summarizeJson;
	$had .= $summarizeHadoop;
	$sh .= $summarizeSh;
}
if($stages{postprocess}) {
	$json .= "," if $json ne "[";
	$json .= $postprocJson;
	$had .= $postprocHadoop;
	$sh .= $postprocSh;
}
$json .= "," if $json ne "[";
$json .= $countersJson;
$sh .= "echo \"All output to console recorded in myrna.local.\$\$.out\"\n";
$sh .= "date ; echo DONE\n";
#$had .= "echo \"All output to console recorded in myrna.hadoop.\$\$.out\"\n";
$had .= "date ; echo DONE\n";
$json .= "\n]\n";
print JSON $json;
close(JSON);
print SH $sh;
close(SH);
print HADOOP $had;
close(HADOOP);
umask $umaskOrig;

if(!$localJob && !$hadoopJob) {
	$cores == 1 || $cores == 2 || $cores == 4 || $cores == 8 || die "Bad number of cores: $cores\n";
}
$name =~ s/"//g;
(defined($emrScript) && $emrScript ne "") || $localJob || $hadoopJob || die;
my $instTypeStr = "--num-instances $totalNodes --instance-type $instType ";
if(index($numNodes, ',') != -1) {
	my @nn = split(',', $numNodes);
	while(scalar(@nn) < 3) {
		push @nn, 0;
	}
	# TODO: allow different instance type for master / core / task
	my @instTypes = ($instType) x 3;
	$instTypeStr = AWS::instanceTypeString(\@instTypes, \@nn, $bidPrice);
}

my $cmdJson = "$emrScript ".
    "--create ".
    "$credentials ".
    "$emrArgs ".
    "--name \"$name\" ".
    "$instTypeStr ".
    "--json $jsonFile ".
    "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive ".
    "--bootstrap-name \"Set memory-intensive mode\" ".
    "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop ".
    "--bootstrap-name \"Configure Hadoop\" ".
    "--args \"-s,mapred.job.reuse.jvm.num.tasks=1,-s,mapred.tasktracker.reduce.tasks.maximum=$cores,-s,mapred.tasktracker.map.tasks.maximum=$cores,-s,io.sort.mb=100\" ".
    "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/add-swap ".
    "--bootstrap-name \"Add Swap\" ".
    "--args \"$swap\"";

my $cmdSh = "sh $runLocalFile";
my $cmdHadoop = "sh $runHadoopFile";

if($dryrun) {
	open RUN, ">$runJsonFile" || die "Error: Could not open $runJsonFile for writing\n";
	print RUN "#!/bin/sh\n";
	print RUN $cmdJson; # include argument passthrough
	close(RUN);
}

$msg->("\n");
$msg->("$APP job\n");
$msg->("------------\n");
$msg->("Job json in: $jsonFile\n") if (!$localJob && !$hadoopJob);
$msg->("Job command in: $runJsonFile\n") if (!$localJob && !$hadoopJob && $dryrun);
$msg->("Local commands in: $runLocalFile\n") if $localJob;
$msg->("Hadoop streaming commands in: $runHadoopFile\n") if $hadoopJob;
if($dryrun) {
	$msg->("Exiting without running command because of --dryrun\n");
} else {
	my $ms = "";
	my $pipe;
	if($localJob) {
		$pipe = "$cmdSh 2>&1 |";
		$ms .= "$cmdSh\n" if $verbose;
	} elsif($hadoopJob) {
		$pipe = "$cmdHadoop 2>&1 |";
		$ms .= "$cmdHadoop\n" if $verbose;
	} else {
		$pipe = "$cmdJson 2>&1 |";
		$ms .= "$cmdJson\n" if $verbose;
	}
	$msg->($ms) if $verbose;
	$msg->("Running...\n");
	open(CMDP, $pipe) || die "Could not open pipe '$pipe' for reading\n";
	while(<CMDP>) { $msg->($_); }
	close(CMDP);
	$msg->("elastic-mapreduce script completed with exitlevel $?\n");
}
$msg->("$warnings warnings\n") if $warnings > 0;

}

1;
