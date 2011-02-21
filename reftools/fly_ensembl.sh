#!/bin/sh

#
# Build a fruitfly reference jar from scratch using info from the
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
ENSEMBL_PREFIX=Drosophila_melanogaster.BDGP5.25.$ENSEMBL_VER
ENSEMBL_ORGANISM=dmelanogaster
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/drosophila_melanogaster/dna
INDEX=fly_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

BASE_CHRS=""
BASE_CHRS="$BASE_CHRS chromosome.2L"
BASE_CHRS="$BASE_CHRS chromosome.2LHet"
BASE_CHRS="$BASE_CHRS chromosome.2R"
BASE_CHRS="$BASE_CHRS chromosome.2RHet"
BASE_CHRS="$BASE_CHRS chromosome.3L"
BASE_CHRS="$BASE_CHRS chromosome.3LHet"
BASE_CHRS="$BASE_CHRS chromosome.3R"
BASE_CHRS="$BASE_CHRS chromosome.3RHet"
BASE_CHRS="$BASE_CHRS chromosome.4"
BASE_CHRS="$BASE_CHRS chromosome.U"
BASE_CHRS="$BASE_CHRS chromosome.Uextra"
BASE_CHRS="$BASE_CHRS chromosome.X"
BASE_CHRS="$BASE_CHRS chromosome.XHet"
BASE_CHRS="$BASE_CHRS chromosome.YHet"
BASE_CHRS="$BASE_CHRS chromosome.dmel_mitochondrion_genome"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$MYRNA_HOME" ] && echo "MYRNA_HOME not set" && exit 1
source $MYRNA_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_ivals
do_index $*
do_jar

cd ..
