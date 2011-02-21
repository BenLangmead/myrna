#!/usr/bin/perl -w

# Used to convert MANUAL.markdown to MANUAL.

use strict;
use warnings;

my $lastBlank = 0;
my $inComment = 0;

while(<>) {
	# Skip comments
	if($inComment) {
		$inComment = 0 if /^\s*-->/;
		next;
	}
	if(/^\s*<!--/) {
		$inComment = 1;
		next;
	}
	next if /^\s*!/;
	# Skip internal links
	next if /\[.*\]: #/;
	# Skip HTML
	next if /^\s?\s?\s?<.*>\s*$/;
	# Skip HTML
	next if /^\s*<table/;
	next if /^\s*<\/td/;
	# Strip [`...`]
	s/\[`/`/g;
	s/`\]/`/g;
	# Strip [#...]
	s/\[#[^\]]*\]//g;
	# Strip (#...)
	s/\(#[^\)]*\)//g;
	# Turn hashes into spaces
	s/^####/   /;
	s/^###/ /;
	if(/^\s*$/) {
		next if $lastBlank;
		$lastBlank = 1;
	} else {
		$lastBlank = 0;
	}
	print $_;
}
