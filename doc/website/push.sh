#!/bin/sh

##
# push.sh
#
# Run this from the $MYRNA_HOME/doc/website subdirectory.
#
# Copies the files that comprise the website at
# http://bowtie-bio.sourceforge.net/myrna to sourceforge.  You must
# have the right sourceforge privileges to do this.  The SF_USER
# environment variable must be set appropriately.
#

[ -z "$SF_USER" ] && echo "Must set SF_USER" && exit 1

scp -r * $SF_USER,bowtie-bio@web.sourceforge.net:/home/groups/b/bo/bowtie-bio/htdocs/myrna
scp -r ../images $SF_USER,bowtie-bio@web.sourceforge.net:/home/groups/b/bo/bowtie-bio/htdocs/myrna/
