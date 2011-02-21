#!/bin/sh

#
# Build a chimp reference jar from scratch using info from the current
# version of Ensembl.  Put results in subdirectory called "human".
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
ENSEMBL_PREFIX=Pan_troglodytes.CHIMP2.1.$ENSEMBL_VER
ENSEMBL_ORGANISM=ptroglodytes
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/pan_troglodytes/dna
INDEX=chimp_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

i=2
BASE_CHRS="chromosome.1"
while [ $i -lt 23 ] ; do
	if [ $i -eq 2 ] ; then
		# "Chromosome 2" comes in 2 parts
		BASE_CHRS="$BASE_CHRS chromosome.2a chromosome.2b"
	else
		BASE_CHRS="$BASE_CHRS chromosome.$i"
	fi
	i=`expr $i + 1`
done
BASE_CHRS="$BASE_CHRS chromosome.X chromosome.Y chromosome.MT chromosome.M chromosome.Un nonchromosomal"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$MYRNA_HOME" ] && echo "MYRNA_HOME not set" && exit 1
source $MYRNA_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_ivals
do_index $*
do_jar

cd ..
