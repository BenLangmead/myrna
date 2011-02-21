#!/bin/sh

#
# Build a zebrafish reference jar from scratch using info from the
# current version of Ensembl.  Put results in subdirectory called
# "human".
#
# NOTE: I can't find a way to ask biomaRt for a particular archive (or
# non-archive) version of Ensembl, which means that this script
# periodically breaks unless the user manually sets ENSEMBL_VER and
# ENSEMBL_PREFIX in a way that accurately reflects the current Ensembl
# version and FASTA file naming scheme.  I'll probably switch to a
# BioPerl-based link to biomart in the future.
#

SUFFIX=$1
shift
ENSEMBL_VER=61
ENSEMBL_PREFIX=Danio_rerio.Zv9.$ENSEMBL_VER
ENSEMBL_ORGANISM=drerio
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/danio_rerio/dna
INDEX=zebrafish_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

i=2
BASE_CHRS="chromosome.1"
while [ $i -lt 26 ] ; do
	BASE_CHRS="$BASE_CHRS chromosome.$i"
	i=`expr $i + 1`
done
BASE_CHRS="$BASE_CHRS chromosome.MT nonchromosomal"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$MYRNA_HOME" ] && echo "MYRNA_HOME not set" && exit 1
source $MYRNA_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_ivals
do_index $*
do_jar

cd ..
