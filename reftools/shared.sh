#!/bin/bash

get() {
	file=$1
	if ! wget --version >/dev/null 2>/dev/null ; then
		if ! curl --version >/dev/null 2>/dev/null ; then
			echo "Please install wget or curl somewhere in your PATH"
			exit 1
		fi
		curl -o `basename $1` $1
		return $?
	else
		wget -O `basename $1` $1
		return $?
	fi
}

check_prereqs() {
	SCRIPT_DIR=$MYRNA_HOME/reftools
	[ -n "$1" ] && SCRIPT_DIR=$1 
	[ ! -f "$SCRIPT_DIR/Ensembl.pl" ] && echo "Can't find $SCRIPT_DIR/Ensembl.pl" && exit 1
}

find_bowtie_build() {
	# Try current dir
	BOWTIE_BUILD_EXE=./bowtie-build
	if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
		# Try $MYRNA_BOWTIE_HOME
		BOWTIE_BUILD_EXE="$MYRNA_BOWTIE_HOME/bowtie-build"
		if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
			# Try $PATH
			BOWTIE_BUILD_EXE=`which bowtie-build`
			if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
				echo "Error: Could not find runnable bowtie-build in current directory, in \$MYRNA_BOWTIE_HOME/bowtie-build, or in \$PATH"
				exit 1
			fi
		fi
	fi
}

do_jar() {
	mkdir -p jar
	mkdir -p jar/index
	mkdir -p jar/ivals
	
	#if [ ! -f jar/$INDEX.jar -o ! -f jar/$INDEX.idx.jar -o ! -f jar/$INDEX.ivals.jar ]
	if [ ! -f jar/$INDEX.jar ]
	then
		rm -f jar/index/*
		rm -fr jar/ivals/*
		rm -f *.jar
		# Jar it up
		jar cf $INDEX.jar *
		#jar cf $INDEX.idx.jar index
		#jar cf $INDEX.ivals.jar ivals
	else
		#echo "$INDEX.jar, $INDEX.idx.jar, $INDEX.ivals.jar already present"
		echo "$INDEX.jar already present"
	fi
}

do_index() {
	if [ ! -f index/$INDEX.1.ebwt ] ; then
		INPUTS=
		mkdir -p genome
		pushd genome
		for ci in $CHRS_TO_INDEX ; do
			c=$ENSEMBL_PREFIX.dna.$ci
			if [ ! -f ${c}.fa ] ; then
				F=${c}.fa.gz
				get ${ENSEMBL_FTP}/$F || (echo "Error getting $F" && exit 1)
				gunzip $F || (echo "Error unzipping $F" && exit 1)
			fi
			[ -n "$INPUTS" ] && INPUTS=$INPUTS,../genome/${c}.fa
			[ -z "$INPUTS" ] && INPUTS=../genome/${c}.fa
		done
		popd
		mkdir -p index
		pushd index
		CMD="$BOWTIE_BUILD_EXE $* $INPUTS $INDEX"
		echo Running $CMD
		if $CMD ; then
			echo "$INDEX index built; you may remove fasta files"
		else
			echo "Index building failed; see error message"
		fi
		popd
	else
		echo "$INDEX.*.ebwt files already present"
	fi
}

do_ivals() {
	if [ ! -d ivals ] ; then
		# Create the interval files
		mkdir -p ivals
		[ -z "$ENSEMBL_MART" ] && ENSEMBL_MART="ensembl"
		[ -z "$ENSEMBL_DATASET" ] && ENSEMBL_DATASET="${ENSEMBL_ORGANISM}_gene_ensembl"
		if ! perl $SCRIPT_DIR/Ensembl.pl -mart=$ENSEMBL_MART -dataset=$ENSEMBL_DATASET -organism=$ENSEMBL_ORGANISM ; then
			echo "Error: Ensembl.pl failed; aborting..."
			exit 1
		fi
	else
		echo "ivals/*.ivals files already present"
	fi
}
