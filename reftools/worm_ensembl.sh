#!/bin/sh

#
# Build a worm reference jar from scratch using info from the current
# version of Ensembl.
#

SUFFIX=$1
shift
ENSEMBL_VER=61
ENSEMBL_PREFIX=Caenorhabditis_elegans.WS220.$ENSEMBL_VER
ENSEMBL_ORGANISM=celegans
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/caenorhabditis_elegans/dna
INDEX=worm_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

# Change to jar scratch directory
mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

# Compose the list of fasta files to download
BASE_CHRS=""
BASE_CHRS="$BASE_CHRS chromosome.I"
BASE_CHRS="$BASE_CHRS chromosome.II"
BASE_CHRS="$BASE_CHRS chromosome.III"
BASE_CHRS="$BASE_CHRS chromosome.IV"
BASE_CHRS="$BASE_CHRS chromosome.V"
BASE_CHRS="$BASE_CHRS chromosome.X"
BASE_CHRS="$BASE_CHRS chromosome.MtDNA"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$MYRNA_HOME" ] && echo "MYRNA_HOME not set" && exit 1
source $MYRNA_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_ivals
do_index $*
do_jar

cd ..
