#!/usr/bin/perl -w

##
# Ensembl.pl
#
# Wrapper for Ensembl.R, which preprocesses intervals by connecting to
# Ensembl via biomaRt.  Results are placed in 'ivals' subdirectory.
# E.g.:
#
# To build a set of intervals for human:
# perl Ensembl.pl -organism hsapiens
#

use strict;
use warnings;
use Getopt::Long;
use FileHandle;
use FindBin qw($Bin); 

my $help = 0;
my $rpath = "";
my $rcmd = "Rscript";
my $rargs = "--vanilla --default-packages=base,methods,utils,stats,IRanges,biomaRt,Biostrings";
my $mart = "";
my $organism = "";
my $dataset = "";
my $inclChrs = "";
my $exclChrs = "";
my $ftpBase  = "";
my $maskFa = 0;
my $repMask = 0;

my $usage = qq{
Ensembl.pl:
   Get exon information from Ensembl and write a set of files called.
	
Usage: perl Ensembl.pl -organism <name> -ftp-base <base> [options]

Arguments:
 -ftp-base <base>      Ensembl FTP URL up to but not including the "dna" or
                       "dna_rm" portion
 -mart <name>          Use mart <name>; default: "ensembl"
 -dataset <name>       Use dataset <name>; def.: "<organism_name>_gene_ensembl"
 -organism <name>      Ensembl name for organism whose genes to retrieve
                       (e.g. "mmusculus", "dmelanogaster", "hsapiens")

Options:
 -include-chrs <list>  Comma-separated list of names of chromosomes to include.
 -exclude-chrs <list>  Comma-separated list of names of chromosomes to exclude.
 -mask-pseudogenes     If set, all pseudogene intervals are set to all Ns
 -repeat-mask          Use the "dna_rm" version of the Ensembl fasta
                       (default: "dna")

Some common arguments for -ftp-base (w/r/t Ensembl version 58):

Human: ftp://ftp.ensembl.org/pub/current_fasta/homo_sapiens/dna/Homo_sapiens.GRCh37.58.
Mouse: ftp://ftp.ensembl.org/pub/current_fasta/mus_musculus/dna/Mus_musculus.NCBIM37.58.
Yeast: ftp://ftp.ensembl.org/pub/current_fasta/saccharomyces_cerevisiae/dna/Saccharomyces_cerevisiae.SGD1.01.58.
};

sub dieusage($$) {
	my ($msg, $lev) = @_;
	print STDERR "$msg\n\n";
	print STDERR $usage;
	exit $lev;
}

GetOptions (
	"R:s"              => \$rpath,
	"help"             => \$help,
	"usage"            => \$help,
	"mart=s"           => \$mart,
	"organism=s"       => \$organism,
	"dataset=s"        => \$dataset,
	"ftp-base=s"       => \$ftpBase,
	"mask-pseudogenes" => \$maskFa,
	"repeat-mask"      => \$repMask,
	"include-chrs=s"   => \$inclChrs,
	"exclude-chrs=s"   => \$exclChrs) || dieusage("Bad option", 1);

$inclChrs = "-" if !defined($inclChrs) || $inclChrs eq "";
$exclChrs = "-" if !defined($exclChrs) || $exclChrs eq "";

$mart = "ensembl" if $mart eq "";
$dataset = "${organism}_gene_ensembl" if $dataset eq "";

print "Mart: $mart\n";
print "Organism: $organism\n";
print "Dataset: $dataset\n";
print "FTP Base: $ftpBase\n";
print "Mask pseudogenes: $maskFa\n";
print "Use repeat-masked Ensembl: $repMask\n";

if($help) { print $usage; exit 0 };
$rcmd = "$rpath/Rscript" if $rpath ne "";
system("$rcmd --version > /dev/null 2> /dev/null") &&
	die "Non-0 exitlevel: $rcmd --version\nSet path to R/Rscript using -R\n";
$organism ne "" || die "Must specify -organism\n";

system("mkdir -p ivals");
my %fhs = ();
my %cnts = ();

$ftpBase = "-" if $ftpBase eq "";
my $cmd = "$rcmd $rargs $Bin/Ensembl.R --args $mart $organism $dataset $ftpBase $maskFa $repMask $inclChrs $exclChrs > .Ensembl.pl.$$";
print STDERR "Ensembl.pl: Running R with command:\n";
print STDERR "            $cmd\n";
system("mkdir -p ivals/un");
system("mkdir -p ivals/ui");
system("mkdir -p ivals/gene_olaps");
my $ret = system($cmd);
$ret == 0 || die "R command '$cmd' failed with exitlevel $ret\n";
open(TMP, ".Ensembl.pl.$$") || die "Could not open .Ensembl.pl.$$ for reading\n";
my %sizes = ();
while(<TMP>) {
	chomp;
	my @s = split(/[\t]/);
	# Sanity check output line
	$#s == 4 || $#s == 3 || die "Expected 4 or 5 fields:\n$_\n";
	$s[0] eq "un" || $s[0] eq "ui" || $s[0] eq "gene_olaps" || die;
	$s[0] eq "gene_olaps" || $#s == 4 || die "Type wasn't gene_olaps, but had != 5 fields\n";
	$s[0] ne "gene_olaps" || $#s == 3 || die "Type was gene_olaps, but had != 4 fields\n";
	$sizes{$s[1]}++;
}
close(TMP);
# Pick the 100 chromosomes with the largest numbers of intervals and
# stick them in largeChrs
my %largeChrs = ();
my $smallChrs = 0;
for my $k (sort {$sizes{$b} <=> $sizes{$a}} keys %sizes) {
	if(scalar(keys %largeChrs) > 100) {
		$smallChrs++;
		next;
	}
	$largeChrs{$k} = 1;
}
print STDERR "Ensembl.pl: $smallChrs chromosomes are small and will be lumped into other.ivals\n";
open (TMP, ".Ensembl.pl.$$") || die "Could not open .Ensembl.pl.$$ for reading\n";
while(<TMP>) {
	chomp;
	my @s = split(/[\t]/);
	my $chrn = $s[1];
	# If the chromosome is really small, lump with other small
	# chromosomes in "other.ivals"
	$chrn = "other" unless defined($largeChrs{$chrn});
	my $fn = "ivals/$s[0]/$chrn.ivals"; # <type>/<chromosome>.ivals
	shift @s;
	my $fhk = $fn;
	unless(defined($fhs{$fn})) {
		# First encounter for this chromosome; make a new filehandle
		$fhs{$fn} = FileHandle->new;
		$fhs{$fn}->open(">$fn");
	}
	print {$fhs{$fn}} join("\t", @s)."\n";
	$cnts{$fn}++;
}
close(TMP);
# Close all filehandles
for my $k (keys %fhs) {
	print STDERR "Ensembl.pl: Wrote $cnts{$k} records to file $k\n";
	$fhs{$k}->close;
}
print STDERR "Ensembl.pl: Done\n";
system("rm -f .Ensembl.pl.$$");
