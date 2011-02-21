#!/bin/sh

# On EC2:
# sudo yum -y install bzip2-devel
#
# Still has a problem with -lxml2 but fastq-dump builds.
#

wget http://trace.ncbi.nlm.nih.gov/Traces/sra/static/sra_sdk-2.0.0rc1.tar.gz
tar zxvf sra_sdk-2.0.0rc1.tar.gz
cd sra_sdk-2.0.0rc1
make static
make
