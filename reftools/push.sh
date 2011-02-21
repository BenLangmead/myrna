#!/bin/sh

[ -z "$S3CFG" ] && echo "Must specify S3CFG environment variable" && exit 1
[ ! -f "$S3CFG" ] && echo "No such s3cmd config file: $S3CFG" && exit 1

for i in zebrafish chimp human rhesus mouse rat yeast dog chicken fly worm ; do
	s3cmd -c $S3CFG put --acl-public $i*/*.jar s3://myrna-refs/
done
