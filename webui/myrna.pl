#!/usr/bin/perl -w

##
# Myrna web interface.  Requires S3Util.pm and MyrnaIface.pm in the
# same directory.
#

use strict;
use warnings;
use CGI;
use CGI::Ajax;
use Net::Amazon::S3;
use FindBin qw($Bin);
use lib $Bin;
use MyrnaIface;
use S3Util;
use CGI::Carp qw(fatalsToBrowser);

my $VERSION = "1.2.3";
my $debugLev = 0;
my $cgi  = CGI->new();
my $ajax = CGI::Ajax->new(submitClicked  => \&submitClicked,
                          checkS3URL     => \&checkS3URL,
                          checkS3Creds   => \&checkS3Creds,
                          checkRefURL    => \&checkRefURL,
                          checkInputURL  => \&checkInputURL,
                          checkOutputURL => \&checkOutputURL);
$ajax->js_encode_function('encodeURIComponent');
$ajax->JSDEBUG($debugLev);
print $ajax->build_html( $cgi, \&main );

##
# Verify that given input URL exists.
#
sub checkInputURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckInputURL()\">Re-check input URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 0 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Input URL does not exist: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Input URL does not exist</font> $recheck"
		}
	}
}

##
# Verify that given reference-jar URL exists.
#
sub checkRefURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckRefURL()\">Re-check reference URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 0 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Reference jar URL does not exist: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Reference jar URL does not exist</font> $recheck"
		}
	}
}

##
# Verify that given output URL does not exist.
#
sub checkOutputURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckOutputURL()\">Re-check output URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 0) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 1 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Output URL already exists: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Output URL already exists</font> $recheck"
		}
	}
}

##
# Check if the given S3 credentials work.
#
sub checkS3Creds {
	my ($awsId, $awsSecret) = @_;
	my $ret = eval { S3Util::checkCreds($awsId, $awsSecret); };
	my $recheck = "(<a href=\"javascript:jsCheckS3Creds()\">Re-check credentials...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: checkCreds died with message \"$@\": \"$awsId\", \"$awsSecret\"</font> $recheck";
		} else {
			return "<font color='red'>Error: checkCreds died with message \"$@\"</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$awsId\", \"$awsSecret\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} else {
		if($debugLev > 0) {
			return "<font color='red'>Error: Bad AWS ID and/or Secret Key: \"$awsId\", \"$awsSecret\"</font> ";
		} else {
			return "<font color='red'>Error: Bad AWS ID and/or Secret Key</font> $recheck";
		}
	}
}

#
# Form elements:
#
#  AWSId: text
#  AWSSecret: password
#  AWSKeyPair: text
#  JobName: text
#  JobType: radio (just-preprocess | myrna)
#  InputURL: text
#  OutputURL: text
#  InputType: radio (manifest | preprocessed)
#  TruncateLength: text (blank or 0 = don't truncate)
#  TruncateDiscard: check
#  DiscardFraction: text (blank or 0 = don't discard)
#  QualityEncoding: dropdown (Phred+33 | Phred+64 | Solexa+64)
#  GenesToReport: text
#  Genome: dropdown (bunch of genomes)
#  GenomeColorspace: check
#  SpecifyRef: check
#  Ref: text
#  BowtieOpts: text
#  Family: dropdown (gaussian | poisson)
#  Permutations: text
#  GeneFootprint: radio (union | intersect) 
#  PoolTechReps: check
#  PoolAllReps: check
#  DiscardMate: check
#  ClusterWait: check
#  NumNodes: text
#  InstanceType: dropdown (c1.xlarge)
#

sub submitClicked {
	my ($awsId,
	    $awsSecret,
	    $keyPairName,
	    $name,
	    $jobType,
	    $inputURL,
	    $outputURL,
	    $inputType,
	    $truncLen,
	    $truncDiscard,
	    $discardFrac,
	    $qual,
	    $reportNum,
	    $genome,
	    $genomeColor,
	    $specifyRef,
	    $ref,
	    $bowtieOpts,
	    $family,
	    $permutations,
	    $footprint,
	    $poolTechReps,
	    $poolAllReps,
	    $discardMate,
	    $clusterWait,
	    $numNodes,
	    $instanceType) = @_;

	##
	# Map from short names to URLs for the pre-built reference jars.
	#
	my %refMap = (
		"human_67"     => "s3n://myrna-refs/human_ensembl_67.jar",
		"mouse_67"     => "s3n://myrna-refs/mouse_ensembl_67.jar",
		"rat_67"       => "s3n://myrna-refs/rat_ensembl_67.jar",
		"chimp_67"     => "s3n://myrna-refs/chimp_ensembl_67.jar",
		"macaque_67"   => "s3n://myrna-refs/rhesus_ensembl_67.jar",
		"dog_67"       => "s3n://myrna-refs/dog_ensembl_67.jar",
		"chicken_67"   => "s3n://myrna-refs/chicken_ensembl_67.jar",
		"worm_67"      => "s3n://myrna-refs/worm_ensembl_67.jar",
		"fly_67"       => "s3n://myrna-refs/fly_ensembl_67.jar",
		"yeast_67"     => "s3n://myrna-refs/yeast_ensembl_67.jar",
		"zebrafish_67" => "s3n://myrna-refs/zebrafish_ensembl_67.jar"
	);
	
	$name = "Myrna" unless defined($name) && $name ne "";
	$jobType eq "--just-preprocess" || $jobType eq "--myrna" || croak("Bad JobType: $jobType");
	$numNodes == int($numNodes) || croak("NumNodes is not an integer: $numNodes");
	$reportNum == int($reportNum) || croak("GenesToReport is not an integer: $reportNum");
	$qual eq "phred33" || $qual eq "phred64" || $qual eq "solexa64" || croak("Bad quality string: \"$qual\"");
	
	my @as = ();
	push @as, "--accessid=$awsId";
	push @as, "--secretid=$awsSecret";
	push @as, "--key-pair=$keyPairName" if defined($keyPairName) && $keyPairName ne "";
	push @as, "--emr-script=\"/var/www/cgi-bin/elastic-mapreduce\"";
	push @as, "--name=\"$name\"";
	push @as, "$jobType";
	push @as, "--input=$inputURL";
	push @as, "--output=$outputURL";
	if($jobType eq "just-preprocess") {
		# Preprocess job
	} else {
		# Myrna job
		$truncDiscard = "--truncate-length" unless $truncDiscard ne "";
		push @as, "$truncDiscard=$discardFrac" if $truncLen > 0;
		push @as, "--discard-reads=$truncLen" if $discardFrac > 0;
		push @as, "--quality=$qual";
		push @as, "--top=$reportNum";
		push @as, "--preprocess" if $inputType eq "manifest";
		if($specifyRef) {
			# User-specified ref URL
			my ($proto, $bucket, $path) = S3Util::parsePath($ref);
			defined($proto)  || croak("Could not parse reference path: $ref");
			defined($bucket) || croak("Could not parse bucket in reference path: $ref");
			defined($path)   || croak("Could not parse path in reference path: $ref");
			push @as, "--ref=$ref";
		} else {
			# Pre-built ref
			defined($refMap{$genome}) || croak("Bad genome short name: \"$genome\"");
			my $g = $refMap{$genome};
			defined($g) || croak();
			$g =~ s/\.jar$/.cs.jar/ if $genomeColor;
			push @as, "--ref=$g";
		}
		push @as, "--family=$family";
		push @as, "--nulls=$permutations" if $permutations > 0;
		push @as, "--bowtie-args=$bowtieOpts";
		push @as, "--gene-footprint=$footprint";
		push @as, "$poolTechReps";
		push @as, "$poolAllReps";
		push @as, "$discardMate";
	}
	push @as, "$clusterWait";
	push @as, "--instances=$numNodes";
	push @as, "--verbose";
	push @as, "--instance-type=$instanceType";
	
	my $stdout = "";
	my $stderr = "";

	my $stdoutf = sub { $stdout .= $_[0]; };
	my $stdoutff = sub {
		my $str = shift @_;
		$stdout .= sprintf $str, @_;
	};
	my $stderrf = sub { $stderr .= $_[0]; };
	my $stderrff = sub {
		my $str = shift @_;
		$stderr .= sprintf $str, @_;
	};
	if(!defined($ENV{HOME})) {
		$stderr .= "Had to define HOME in myrna.pl\n";
		$ENV{HOME} = "/var/www/cgi-bin";
	}
	MyrnaIface::myrna(\@as, "myrna.pl", "(no usage)", $stdoutf, $stdoutff, $stderrf, $stderrff);
	
	my $jobid = "";
	$stdout =~ /Created job flow (.*)/;
	$jobid = $1 if defined($1);
	
	my $resultHtml = "";
	if($jobid eq "") {
		my $asStr = "";
		for my $a (@as) {
			next unless $a ne "";
			$asStr .= "$a\n";
		}
		# Error condition
		$resultHtml .= <<HTML;
			<font color="red"><b>Error invoking Myrna. Job not submitted.</b></font>
			
			<br><b>Arguments given to Myrna driver script:</b>
			<pre>$asStr</pre>
			
			<b>Standard output from driver:</b>
			<pre>$stdout</pre>
			
			<b>Standard error from driver:</b>
			<pre>$stderr</pre>
HTML
	} else {
		# Everything seemed to go fine
		$resultHtml .= <<HTML;
			<br>
			Job created; MapReduce job ID = $jobid
			<br>
			Go to the
			<a href="https://console.aws.amazon.com/elasticmapreduce" target="_blank">
			AWS Console's Elastic MapReduce</a> tab to monitor your
			job.
HTML
	}
	return $resultHtml;
}

sub main {
	my $html = "";
	$html .= <<HTML;
<html>
<head>
</head>
<body>
<script src="http://jotform.com/js/form.js?v2.0.1347" type="text/javascript"></script>
<style type="text/css">
.main {
  font-family:"Verdana";
  font-size:11px;
  color:#666666;
}
.tbmain{ 
 /* Changes on the form */
 background: white !important;
}
.left{
  /* Changes on the form */
  color: black !important; 
  font-family: Verdana !important;
  font-size: 12px !important;
}
.right{
  /* Changes on the form */
  color: black !important; 
  font-family: Verdana !important;
  font-size: 12px !important;
}
.check{
  color: black !important; 
  font-family: Verdana !important;
  font-size: 10px !important;
}
.head{
  color:#333333;
  font-size:20px;;
  text-decoration:underline;
  font-family:"Verdana";
}
td.left {
  font-family:"Verdana";
  font-size:12px;
  color:black;
}
.pagebreak{
  font-family:"Verdana";
  font-size:12px;
  color:black;
}
.tbmain{
  height:100%;
  background:white;
}
span.required{
  font-size: 13px !important;
  color: red !important;
}

div.backButton{
    background: transparent url("http://jotform.com//images/btn_back.gif") no-repeat scroll 0 0;
    height:16px;
    width:53px;
    float:left;
    margin-bottom:15px;
    padding-right:5px;
}
div.backButton:hover{
    background: transparent url("http://jotform.com//images/btn_back_over.gif") no-repeat scroll 0 0;
}
div.backButton:active{
    background: transparent url("http://jotform.com//images/btn_back_down.gif") no-repeat scroll 0 0;
}
div.nextButton{
    background: transparent url("http://jotform.com//images/btn_next.gif") no-repeat scroll 0 0;
    height:16px;
    width:53px;
    float: left;
    margin-bottom:15px;
    padding-right:5px;
}
div.nextButton:hover{
    background: transparent url("http://jotform.com//images/btn_next_over.gif") no-repeat scroll 0 0;
}
div.nextButton:active{
    background: transparent url("http://jotform.com//images/btn_next_down.gif") no-repeat scroll 0 0;
}
.pageinfo{
    padding-right:5px;
    margin-bottom:15px;
    float:left;
}
 
</style> 
<table width="100%" cellpadding="2" cellspacing="0" class="tbmain">
<tr><td class="topleft" width="10" height="10">&nbsp;</td>
<td class="topmid">&nbsp;</td>
<td class="topright" width="10" height="10">&nbsp;</td>
  </tr>
<tr>
<td class="midleft" width="10">&nbsp;&nbsp;&nbsp;</td>
<td class="midmid" valign="top">
<form accept-charset="utf-8"  action="/myrnaform" method="post" name="form">
<div id="main"> 
<div class="pagebreak"> 
<table width="520" cellpadding="5" cellspacing="0">
 <tr >
  <td class="left" colspan=2>
   <h2>Myrna $VERSION</h2>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS ID <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckS3Creds()"
    size="25" name="AWSId" class="text" value="" onmouseover="ddrivetip('Your AWS Access Key ID, usually 20 characters long (not your Secret Access Key or your Account ID).', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS Secret Key <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="password"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckS3Creds()"
    size="50" name="AWSSecret" class="text" value="" onmouseover="ddrivetip('Your AWS Secret Access Key, usually 40 characters long (not your Access Key ID or your Account ID).', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS Keypair Name</label>
  </td>
  <td class="right" >
   <input type="text"
    size="30" name="AWSKeyPair" class="text" value="gsg-keypair" onmouseover="ddrivetip('Name of the keypair that AWS should install on the cluster, allowing you to log in.', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
   <a href="https://console.aws.amazon.com/ec2/home#c=EC2&s=KeyPairs" target="_blank">Look it up</a>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <span id="credcheck" class="check"><a href="javascript:jsCheckS3Creds()">Check credentials...</a></span>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >Job name</label>
  </td>
  <td class="right" >
   <input type="text" size="30" name="JobName" class="text" value="Myrna" onmouseover="ddrivetip('Name given to Elastic MapReduce job.', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label>Job type</label>
  </td>
  <td class="right">
   <input type="radio" class="other" name="JobType" onclick="enableApp()" onmouseover="ddrivetip('Run the Myrna pipeline, starting with a manifest file or preprocessed reads, and ending with Myrna results.', 200)" onmouseout="hideddrivetip()" value="--myrna" checked  /> 
    <label class="left">Myrna</label> <br /> 
   <input type="radio" class="other" name="JobType" onclick="disableApp()" onmouseover="ddrivetip('Just run the Preprocess step and place preprocessed reads at Output URL.', 200)" onmouseout="hideddrivetip()" value="--just-preprocess" /> 
    <label class="left">Just preprocess reads</label> <br /> 
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >Input URL <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text" size="60" name="InputURL"
    onmouseover="ddrivetip('S3 URL where manifest file or preprocessed reads are located.', 200)"
    onmouseout="hideddrivetip()"
    class="text" value="s3n://"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckInputURL()"
    maxlength="400" maxsize="400" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <div id="inputcheck" class="check"><a href="javascript:jsCheckInputURL()">Check that input URL exists...</a></div>
  </td>
 </tr>
 
 <tr >
  <td width="165" class="left" >
   <label >Output URL <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text" size="60" name="OutputURL"
    onmouseover="ddrivetip('S3 URL where Myrna output should be placed.', 200)"
    onmouseout="hideddrivetip()"
    class="text" value="s3n://"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckOutputURL()"
    maxlength="400" maxsize="400" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <div id="outputcheck" class="check"><a href="javascript:jsCheckOutputURL()">Check that output URL doesn't exist...</a></div>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="app-input-type-label">Input type</label>
  </td>
  <td class="right">
   <input type="radio" id="app-input-type-radio-preprocess" class="other" name="InputType" name="InputType" onmouseover="ddrivetip('Input URL points to a directory of files that have already been preprocessed by Myrna.', 200)" onmouseout="hideddrivetip()" value="preprocessed" checked  /> 
    <label id="app-input-type-preprocess-label">Preprocessed reads</label> <br /> 
   <input type="radio" id="app-input-type-radio-manifest" class="other" name="InputType" name="InputType" onmouseover="ddrivetip('Input URL points to a manifest file listing publicly-readable URLs of input FASTQ files; FASTQ files are both preprocessed and analyzed.', 200)" onmouseout="hideddrivetip()" value="manifest"   /> 
    <label id="app-input-type-manifest-label">Manifest file</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-truncate-length-label">Truncate length</label>
  </td>
  <td class="right" >
   <input type="text" size="5" id="app-truncate-length-text" class="text" name="TruncateLength" onmouseover="ddrivetip('Specifies N such that reads longer than N bases are truncated to length N by removing bases from the 3\\' end.', 200)" onmouseout="hideddrivetip()" class="text" value="0" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
   <span class="main">&nbsp(If blank or 0, truncation is disabled)</span>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
  </td>
  <td valign="top" class="right">
   <input id="app-skip-truncate-check" type="checkbox" class="other"
    name="TruncateDiscard"
    value="--truncate-discard" /> 
    <label id="app-skip-truncate-label">Skip reads shorter than truncate length</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-discard-fraction-label">Discard fraction</label>
  </td>
  <td class="right" >
   <input id="app-discard-fraction-text" type="text" size="5" name="DiscardFraction" onmouseover="ddrivetip('Randomly discard specified fraction of the input reads.  Useful for testing purposes.', 200)" onmouseout="hideddrivetip()" class="text" value="0" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label id="app-quality-label">Quality encoding</label>
  </td>
  <td class="right">
   <select id="app-quality-dropdown" class="other" name="QualityEncoding" onmouseover="ddrivetip('Quality value encoding scheme used for input reads.', 200)" onmouseout="hideddrivetip()">
    <option value="phred33">Phred+33</option>
    <option value="phred64">Phred+64</option>
    <option value="solexa64">Solexa+64</option>
   </select>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-top-label">Genes to report in detail</label>
  </td>
  <td class="right" >
   <input id="app-top-text" type="text" size="5" name="GenesToReport" onmouseover="ddrivetip('In addition to P-values for all genes, Myrna reports and plots all alignments in the top N differentially expressed genes, where N is set here.', 200)" onmouseout="hideddrivetip()" class="text" value="50" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
   <span class="main">&nbsp(If blank or 0, all alignments are discarded)</span>
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label id="app-genome-label">Genome/Annotation</label>
  </td>
  <td class="right">
   <select id="app-genome-dropdown" class="other" name="Genome" onmouseover="ddrivetip('Genome assembly to use as reference genome and annotation database to use for gene, transcript and exon annotations.', 200)" onmouseout="hideddrivetip()">
    <option value="human_67">Human (Ensembl 67)</option>
    <option value="mouse_67">Mouse (Ensembl 67)</option>
    <option value="rat_67">Rat (Ensembl 67)</option>
    <option value="chimp_67">Chimp (Ensembl 67)</option>
    <option value="macaque_67">Macaque (Ensembl 67)</option>
    <option value="dog_67">Dog (Ensembl 67)</option>
    <option value="chicken_67">Chicken (Ensembl 67)</option>
    <option value="zebrafish_67">Zebrafish (Ensembl 67)</option>
    <option value="worm_67">Worm (Ensembl 67)</option>
    <option value="fly_67">Fly (Ensembl 67)</option>
    <option value="yeast_67">Yeast (Ensembl 67)</option>
   </select>
   <input id="app-ref-colorspace-check"
    type="checkbox"
    onmouseover="ddrivetip('Check this box to use a colorspace version of the index.', 200)"
    onmouseout="hideddrivetip()"
    onclick="updateColorspace()"
    class="other"
    value="1"
    name="GenomeColorspace"
    />
    <label id="app-ref-colorspace-label">Colorspace</label>
  </td>
 </tr>
 <tr>
  <td width="165" class="left"  valign="top" >
  </td>
  <td class="right">
   <input id="app-specify-ref-check"
    type="checkbox"
    onclick="updateElements()"
    onmouseover="ddrivetip('Specify an S3 url for a reference jar.', 200)"
    onmouseout="hideddrivetip()"
    class="other"
    value="1"
    name="SpecifyRef"
    />
    <label id="app-specify-ref-label">Specify reference jar URL:</label> <br />
   <br/>
   <!-- Reference URL text box -->
   <input id="app-specify-ref-text"
    disabled
    type="text"
    size="60"
    name="Ref"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckRefURL()"
    onmouseover="ddrivetip('Specify an S3 url for a reference jar.', 200)"
    onmouseout="hideddrivetip()"
    value="s3n://" class="text" value=""  maxlength="100" maxsize="100" />
  </td>
 </tr>
 <tr>
  <td width="165" class="left" valign="top" >
  </td>
  <td class="right">
   <div id="refcheck" class="check"><a href="javascript:jsCheckRefURL()">Check that reference jar URL exists...</a></div>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-bowtie-options-label">Bowtie options</label>
  </td>
  <td class="right" >
   <input id="app-bowtie-options-text" type="text" size="50" name="BowtieOpts" onmouseover="ddrivetip('Options to pass to Bowtie in the Align stage.', 200)" onmouseout="hideddrivetip()" class="text" value="-m 1"  maxlength="400" maxsize="400" />
   <br/>
   <span class="main">Don't forget to specify <a href="http://bowtie-bio.sourceforge.net/manual.shtml#bowtie-options-C">-C</a> or <a href="http://bowtie-bio.sourceforge.net/manual.shtml#bowtie-options-C">--color</a> if index is colorspace.</span>
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label id="app-family-label">Model family</label>
  </td>
  <td class="right">
   <select id="app-family-dropdown" class="other" name="Family" onmouseover="ddrivetip('Model family to use for gene expression statistics.', 200)" onmouseout="hideddrivetip()">
    <option value="poisson">Poisson</option>
    <option value="gaussian">Gaussian</option>
   </select>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-nulls-label">Null permutations</label>
  </td>
  <td class="right" >
   <input id="app-nulls-text" type="text" size="5" name="Permutations" onmouseover="ddrivetip('Number of null statitics to calculate per observed statistic.', 200)" onmouseout="hideddrivetip()" class="text" value="0" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
   <span class="main">&nbsp(If blank or 0, permutation test is disabled)</span>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="app-gene-footprint-label">Gene Intervals</label>
  </td>
  <td class="right">
   <input id="app-gene-footprint-radio1" type="radio" class="other" name="GeneFootprint" onmouseover="ddrivetip('Define a gene\\'s footprint as all of the bases covered by an exon in all its transcripts.', 200)" onmouseout="hideddrivetip()" value="intersect" checked  /> 
    <label id="app-gene-footprint-radio1-label">Intersection of transcripts</label> <br /> 
   <input id="app-gene-footprint-radio2" type="radio" class="other" name="GeneFootprint" onmouseover="ddrivetip('Define a gene\\'s footprint as all of the bases covered by any exon in any transcript.', 200)" onmouseout="hideddrivetip()" value="union"   /> 
    <label id="app-gene-footprint-radio2-label">Union of exons</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="app-options3-label">Options</label>
  </td>
  <td valign="top" class="right">
   <input id="app-pool-tech-reps-check" type="checkbox" onmouseover="ddrivetip('When calculating statistics, pool totals from technical replicates.', 200)" onmouseout="hideddrivetip()" class="other"
    name="PoolTechReps"
    value="--pool-technical-replicates" />
    <label id="app-pool-reps-label">Pool technical replicates</label> <br /> 
   <input id="app-pool-reps-check" type="checkbox" onmouseover="ddrivetip('When calculating statistics, pool totals from biological and technical replicates.', 200)" onmouseout="hideddrivetip()" class="other"
    name="PoolAllReps"
    value="--pool-replicates" /> 
    <label id="app-pool-reps-label">Pool all replicates</label> <br />
   <input id="app-discard-mate-check" type="checkbox" onmouseover="ddrivetip('Discard mate #2 from all input read pairs.  Does not affect unpaired input reads.  Useful for comparing datasets that are mixed paired/unpaired.', 200)" onmouseout="hideddrivetip()" class="other"
    name="DiscardMate"
    value="--discard-mate=2" /> 
    <label id="app-discard-mate-label">For paired-end reads, use just one mate</label> <br /> 
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="options-label">Options</label>
  </td>
  <td valign="top" class="right">
   <input id="wait-check" type="checkbox" onmouseover="ddrivetip('Typically the cluster is terminated as soon as the job either completes or aborts.  Check this to keep the cluster running either way.', 200)" onmouseout="hideddrivetip()" class="other"
    name="ClusterWait"
    value="--stay-alive" />
    <label id="wait-label">Keep cluster running after job finishes/aborts</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label ># EC2 instances</label>
  </td>
  <td class="right" >
   <input type="text" size="5" name="NumNodes" onmouseover="ddrivetip('Number of Amazon EC2 instances (virtual computers) to use for this computation.', 200)" onmouseout="hideddrivetip()" class="text" value="1" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label><a href="http://aws.amazon.com/ec2/instance-types/" target="_blank">Instance type</a></label>
  </td>
  <td class="right">
   <select class="other" name="InstanceType" onmouseover="ddrivetip('Type of EC2 instance (virtual computer) to use; c1.xlarge is strongly recommended.', 200)" onmouseout="hideddrivetip()">
    <option value="c1.xlarge">c1.xlarge (recommended)</option>
    <option value="c1.medium">c1.medium</option>
    
    <option value="m2.xlarge">m2.xlarge</option>
    <option value="m2.2xlarge">m2.2xlarge</option>
    <option value="m2.4xlarge">m2.4xlarge</option>
    
    <option value="m1.xlarge">m1.xlarge</option>
    <option value="m1.large">m1.large</option>
    <option value="m1.small">m1.small</option>
   </select>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top">
   <span class="main">Made with the help of</span>
   <br>
   <a href="http://www.jotform.com/" target="_blank">
   <img border=0 width=115
    src="http://www.jotform.com/images/jotform.gif"
    alt="Made with the help of JotForm" /></a>
  </td>
  <td class="right">
  <input type="button" class="btn" value="Submit"
   onclick="document.getElementById('result1').innerHTML = '<img border=0 src=\\'/wait.gif\\' /> Creating job, please wait ...' ;
    submitClicked(
    ['AWSId',
     'AWSSecret',
     'AWSKeyPair',
     'JobName',
     'JobType',
     'InputURL',
     'OutputURL',
     'InputType',
     'TruncateLength',
     'TruncateDiscard',
     'DiscardFraction',
     'QualityEncoding',
     'GenesToReport',
     'Genome',
     'GenomeColorspace',
     'SpecifyRef',
     'Ref',
     'BowtieOpts',
     'Family',
     'Permutations',
     'GeneFootprint',
     'PoolTechReps',
     'PoolAllReps',
     'DiscardMate',
     'ClusterWait',
     'NumNodes',
     'InstanceType'],
     ['result1'])" />
 </td>
 <tr >
  <td colspan="2" class="right">
  <span class="main"><b>Please cite</b>:
  Langmead B, Hansen K, Leek J.
    <a href="http://genomebiology.com/2010/11/8/R83">Cloud-scale RNA-sequencing differential expression analysis with Myrna</a>. <i>Genome Biology</i> 11:R83.</span>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr> <!-- Horizontal rule -->
  </td>
 </tr>
 <tr>
  <td colspan=2 id="result1" class="right">
    <!-- Insert result here -->
  </td>
 </tr>
</table>
</div>
</div>
</form>
</td>
<td class="midright" width="10">&nbsp;&nbsp;&nbsp;</td>
</tr>
<tr>
 <td class="bottomleft" width="10" height="10">&nbsp;</td>
 <td class="bottommid">&nbsp;</td>
 <td class="bottomright" width="10" height="10">&nbsp;</td>
</tr>
</table>
<script type="text/javascript">

var isAppRegex=/^app-/;
var isLabel=/-label\$/;

function updateElements() {
	if(document.form.SpecifyRef.checked) {
		document.form.Ref.disabled = false;
		document.form.Ref.style.color = "black";
		document.form.Genome.disabled = true;
		document.form.GenomeColorspace.disabled = true;
		document.getElementById("app-ref-colorspace-label").style.color = "gray";
	} else {
		document.form.Ref.disabled = true;
		document.form.Ref.style.color = "gray";
		document.form.Genome.disabled = false;
		document.form.GenomeColorspace.disabled = false;
		document.getElementById("app-ref-colorspace-label").style.color = "black";
	}
}

function checkS3ExistsWait(div) {
	document.getElementById(div).innerHTML = '<img border=0 width=18 src=\\'/wait.gif\\' />';
}

function enableApp() {
	var elts = document.getElementsByTagName('*');
	var count = elts.length;
	for(i = 0; i < count; i++) {
		var element = elts[i]; 
		if(isAppRegex.test(element.id)) {
			// Yes, this is an app-related form element that should be re-enabled
			element.disabled = false;
			if(isLabel.test(element.id) || element.type == "text") {
				element.style.color = "black";
			}
		}
	}
	updateElements();
}
function disableApp() {
	var elts = document.getElementsByTagName('*');
	var count = elts.length;
	for(i = 0; i < count; i++) {
		var element = elts[i]; 
		if(isAppRegex.test(element.id)) {
			// Yes, this is an app-related form element that should be disabled
			element.disabled = true;
			if(isLabel.test(element.id) || element.type == "text") {
				element.style.color = "gray";
			}
		}
	}
}

function jsResetCheckS3Creds() {
	document.getElementById('credcheck').innerHTML = '<a href=\\'javascript:jsCheckS3Creds()\\'>Check credentials...</a>';
}

function jsCheckS3Creds() {
	document.getElementById('credcheck').innerHTML = "Checking, please wait...";
	checkS3Creds(['AWSId', 'AWSSecret'], ['credcheck']);
}

function jsResetCheckRefURL() {
	document.getElementById('refcheck').innerHTML = '<a href=\\'javascript:jsCheckRefURL()\\'>Check that reference jar URL exists...</a>';
}

function jsCheckRefURL() {
	document.getElementById('refcheck').innerHTML = "Checking, please wait...";
	checkInputURL(['AWSId', 'AWSSecret', 'Ref'], ['refcheck']);
}

function jsResetCheckInputURL() {
	document.getElementById('inputcheck').innerHTML = '<a href=\\'javascript:jsCheckInputURL()\\'>Check that input URL exists...</a>';
}

function jsCheckInputURL() {
	document.getElementById('inputcheck').innerHTML = "Checking, please wait...";
	checkInputURL(['AWSId', 'AWSSecret', 'InputURL'], ['inputcheck']);
}

function jsResetCheckOutputURL() {
	document.getElementById('outputcheck').innerHTML = '<a href=\\'javascript:jsCheckOutputURL()\\'>Check that output URL doesn\\'t exist...</a>';
}

function jsCheckOutputURL() {
	document.getElementById('outputcheck').innerHTML = "Checking, please wait...";
	checkOutputURL(['AWSId', 'AWSSecret', 'OutputURL'], ['outputcheck']);
}

function colorSpecified() {
	var s = document.getElementById('app-bowtie-options-text').value;
	return s.match(/\\s--color\\s/) ||
	       s.match(/\\s--color\$/) ||
	       s.match(/^--color\\s/)  ||
	       s.match(/^--color\$/)  ||
	       s.match(/\\s-C\\s/) ||
	       s.match(/\\s-C\$/) ||
	       s.match(/^-C\\s/)  ||
	       s.match(/^-C\$/);
}

function removeColor() {
	var s = document.getElementById('app-bowtie-options-text').value;
	s = s.replace(/\\s--color\\s/g, " ");
	s = s.replace(/\\s--color\$/g, "");
	s = s.replace(/^--color\\s/g,  "");
	s = s.replace(/^--color\$/g,  "");
	s = s.replace(/\\s-C\\s/g, " ");
	s = s.replace(/\\s-C\$/g, "");
	s = s.replace(/^-C\\s/g,  "");
	s = s.replace(/^-C\$/g,  "");
	document.getElementById('app-bowtie-options-text').value = s;
}

function updateColorspace() {
	if(document.getElementById('app-ref-colorspace-check').checked) {
		if(!colorSpecified()) {
			document.getElementById('app-bowtie-options-text').value += " --color";
		}
	} else {
		if(colorSpecified()) {
			removeColor();
		}
	}
}

validate();

</script>

<!-- Google analytics code -->
<script type="text/javascript">
var gaJsHost = (("https:" == document.location.protocol) ? "https://ssl." : "http://www.");
document.write(unescape("%3Cscript src='" + gaJsHost + "google-analytics.com/ga.js' type='text/javascript'%3E%3C/script%3E"));
</script>
<script type="text/javascript">
var pageTracker = _gat._getTracker("UA-5334290-1");
pageTracker._trackPageview();
</script>
<!-- End google analytics code -->

</body>
</html>
HTML
	return $html;
}

exit 0;
__END__
