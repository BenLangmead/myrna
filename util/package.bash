#!/bin/bash

#
#  Author: Ben Langmead
#    Date: 9/26/2009
#
# Package Myrna files for release.
#

VERSION=`cat VERSION`
PKG_BASE=.pkg
APP=myrna
PKG=.pkg/$APP-${VERSION}

echo "Should have already run 'make doc' to make documentation"

rm -rf $PKG_BASE 
mkdir -p $PKG

# Copy Myrna sources
cp *.pl *.pm *.R myrna_emr myrna_local myrna_hadoop $PKG/
chmod a+x $PKG/*.pl myrna_emr myrna_local myrna_hadoop

# Include the Bowtie binaries for 32-bit and 64-bit Linux/Mac
mkdir -p $PKG/bin/linux32
mkdir -p $PKG/bin/linux64
mkdir -p $PKG/bin/mac32
mkdir -p $PKG/bin/mac64
cp bin/linux32/* $PKG/bin/linux32/
cp bin/linux64/* $PKG/bin/linux64/
cp bin/mac32/* $PKG/bin/mac32/
cp bin/mac64/* $PKG/bin/mac64/

# Copy contrib dir
mkdir -p $PKG/contrib
cp contrib/* $PKG/contrib

# Copy contrib dir
mkdir -p $PKG/R
cp R/build_r $PKG/R
chmod a+x $PKG/R/build_r

# Copy reftools dir
mkdir -p $PKG/reftools
cp reftools/*.sh $PKG/reftools
cp reftools/*.pl $PKG/reftools
cp reftools/*.R $PKG/reftools
rm -f $PKG/reftools/push.sh
chmod a+x $PKG/reftools/*.sh

# Copy example dir
mkdir -p $PKG/example
for i in yeast human ; do
	mkdir -p $PKG/example/$i
	cp example/$i/*.manifest $PKG/example/$i/
#	cp example/$i/local_*.sh $PKG/example/$i/
#	cp example/$i/hadoop_*.sh $PKG/example/$i/
#	cp example/$i/emr_*.sh $PKG/example/$i/
done

# Copy util dir
#mkdir -p $PKG/util
#cp util/build_r.sh $PKG/util

# Copy doc dir
mkdir -p $PKG/doc
cp doc/*.html $PKG/doc
cp doc/*.css $PKG/doc

cp VERSION NEWS MANUAL LICENSE* TUTORIAL $PKG/

pushd $PKG_BASE
zip -r $APP-${VERSION}.zip $APP-${VERSION}
popd
cp $PKG_BASE/$APP-${VERSION}.zip .
