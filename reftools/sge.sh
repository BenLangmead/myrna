#!/bin/sh

##
# sge.sh
#
# Start SGE jobs for all the pre-built Myrna jars, including both
# nucleotide-space and colorspace jars.  Extra script parameters are
# passed along to qsub.
#

for i in zebrafish chimp human rhesus mouse rat yeast dog chicken fly worm ; do
	
cat >.$i.sh <<EOF
	bash ${i}_ensembl.sh
EOF
cat >.$i.cs.sh <<EOF
	bash ${i}_ensembl.sh .cs -C
EOF
	echo qsub -l mem_free=8G -l cegs -cwd $* .$i.sh
	echo qsub -l mem_free=8G -l cegs -cwd $* .$i.cs.sh
done
