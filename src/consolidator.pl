#!/bin/env perl
#consolidator.pl: Consolidator for the iPlant TNRS batch service.
#Author: Naim Matasci <nmatasci@iplantcollaborative.org>
#
# The contents of this file are subject to the terms listed in the LICENSE file you received with this code.
# Copyright (c) 2012, The Arizona Board of Regents on behalf of
# The University of Arizona
#
###############################################################################

use strict;

process(@ARGV);

sub process {
	my $tmpfolder = shift;
	my $mapfile   = "$tmpfolder/map.tab";
	my $pidfile   = "$tmpfolder/pids.tab";
	my %map;
	my %pids;


	#Load the mapping of names to internal ids
	open my $MAP, "<$mapfile" or die "Cannot open the map file $mapfile: $!\n";
	while (<$MAP>) {
		chomp;
		my ( $id, $name ) = split /\t/, $_;
		$map{$id} = $name;
	}
	close $MAP;

	#Load the mapping of the original ids, if it exist
	if ( -e $pidfile ) {
		open my $PID, "<$pidfile"
		  or die "Cannot open the PID file $pidfile: $!\n";
		while (<$PID>) {
			chomp;
			my ( $name, $pid ) = split /\t/, $_;
			$pids{$name} = $pid;
		}
		close $PID;

	}

	#Load the list of files to consolidate
	opendir my $IND, $tmpfolder or die "Cannot find $tmpfolder: $!\n";
	my @files = grep { /out_\d+\.txt/ } readdir($IND); #the structure of the file is out_0.txt, out_1.txt. etc
	closedir $IND;
	
	#Header tracker
	my $header = 0;

	#process the files in the correct order
	for ( my $i = 0 ; $i < @files ; $i++ ) {
		my @consolidated;
		
		#Load the list of names
		open my $NL, "<$tmpfolder/out_$i.txt"
		  or die "Cannot open processed names file $tmpfolder/out_$i.txt: $!\n";
		my @names_list = <$NL>;
		close $NL;
		
		#If a header hasn't been written, create the file and write the header 
		if ( !$header ) {
			$header = shift(@names_list);
			open my $OF, ">$tmpfolder/output.csv"
			  or die "Cannot create output file $tmpfolder/output.csv: $!\n";
			print $OF "$header";
			close $OF;
		}
		else {
			shift @names_list; #The first line of every file is the header, so it always needs to be removed
		}
		
		#Go over the list of names
		for (@names_list) {
			chomp;
			
			my @fields = split /,/, $_; #the files are comma separated
			
			my $id     = "$i." . shift(@fields); #recreates the internal id
			
			my $ref    = $map{$id}; #Use the internal id to retrieve the original name
			if (%pids) {
				$ref = $pids{ $map{$id} } . "|$map{$id}"; #and the primary id, if present
			}
			
			push @consolidated, join ",", ( "\"$ref\"", @fields );
		}
		
		#Append the batch of processed names to the output file. 
		open my $OF, ">>$tmpfolder/output.csv"
		  or die
		  "Cannot write output file $tmpfolder/output.csv: $!\n";
		print $OF join( "\n", @consolidated ), "\n";
		close $OF;
	}
}

