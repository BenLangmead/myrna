#!/bin/sh

# On EC2:
# sudo yum -y install bzip2-devel
#
# Still has a problem with -lxml2 but fastq-dump builds.
#

wget http://downloads.sourceforge.net/project/samtools/samtools/0.1.19/samtools-0.1.19.tar.bz2
tar jxvf samtools-0.1.19.tar.bz2
cd samtools-0.1.19
make

# Now move 'samtools' to the version-appropriate subdirectory of the emr
# bucket.  Give it a suffix of 32 or 64 depending on whether you compiled a
# 32- or 64-bit binary.
