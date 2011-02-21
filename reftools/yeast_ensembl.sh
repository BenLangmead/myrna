#!/bin/sh

#
# Build a yeast reference jar from scratch using info from the current
# version of Ensembl.
#

SUFFIX=$1
shift
ENSEMBL_VER=61
ENSEMBL_PREFIX=Saccharomyces_cerevisiae.EF2.$ENSEMBL_VER
ENSEMBL_ORGANISM=scerevisiae
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/saccharomyces_cerevisiae/dna
INDEX=yeast_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

# Change to jar scratch directory
mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

BASE_CHRS=
for i in 2-micron I II III IV IX Mito V VI VII VIII X XI XII XIII XIV XV XVI ; do
	BASE_CHRS="$BASE_CHRS chromosome.$i"
done
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$MYRNA_HOME" ] && echo "MYRNA_HOME not set" && exit 1
source $MYRNA_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_ivals
do_index $*
do_jar

cd ..
