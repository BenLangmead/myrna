#!/bin/sh

#
# fill_human_generic.sh
#
# Uses Applescript/Safari to fill in the Myrna Web UI form
# generically (i.e. with placeholders for AWS credentials and bucket
# name) for the Human example.
#

MYRNA_URL=http://ec2-75-101-218-11.compute-1.amazonaws.com/cgi-bin/myrna.pl

cat >.fill_human.applescript <<EOF
tell application "Safari"
	activate
	tell (make new document) to set URL to "$MYRNA_URL"
	delay 6
	set doc to document "$MYRNA_URL"
	log (doc's name)
	do JavaScript "document.forms['form']['AWSId'].value     = '<YOUR-AWS-ID>'" in doc
	do JavaScript "document.forms['form']['AWSSecret'].value = '<YOUR-AWS-SECRET-KEY>'" in doc
	do JavaScript "document.forms['form']['JobName'].value   = 'Myrna-Human'" in doc
	do JavaScript "document.forms['form']['InputURL'].value  = 's3n://<YOUR-BUCKET>/example/human/full.manifest'" in doc
	do JavaScript "document.forms['form']['OutputURL'].value = 's3n://<YOUR-BUCKET>/example/human/output_full'" in doc
	do JavaScript "document.forms['form']['InputType'][1].checked = 1" in doc
	do JavaScript "document.forms['form']['InputType'][0].checked = 0" in doc
	do JavaScript "document.forms['form']['QualityEncoding'].value = 'phred33'" in doc
	do JavaScript "document.forms['form']['SpecifyRef'].checked = '1'" in doc
	do JavaScript "document.forms['form']['Ref'].value = 's3n://<YOUR-BUCKET>/crossbow-refs/human_ensembl_58.jar'" in doc
	do JavaScript "document.forms['form']['NumNodes'].value = '9'" in doc
end tell
EOF

osascript .fill_human.applescript
rm -f .fill_human.applescript
