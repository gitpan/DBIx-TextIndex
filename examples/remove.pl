#!/usr/local/bin/perl

use strict;

use DBI;
use DBIx::TextIndex;
##require "../TextIndex.pm";

my $DB = 'DBI:mysql:test';
##my $DB = 'DBI:mysql:test_full';

my $DBAUTH = ':';

my $document_dbh = DBI->connect($DB, split(':', $DBAUTH, 2)) or die $DBI::errstr;
my $index_dbh = DBI->connect($DB, split(':', $DBAUTH, 2),
	{	ShowErrorStatement => 1 }
) or die $DBI::errstr;

my $index = DBIx::TextIndex->new({
    document_dbh => $document_dbh,
    index_dbh => $index_dbh,
    collection => 'encantadas',
    print_activity => 0
});

print "Total words in index: ".$index->stat("total_words")."\n";
print "Enter document ids to remove, separate with a comma: ";

my $docs = <STDIN>;
chomp $docs;
my @docs = split(/,\s*/, $docs);
my $removed = $index->remove_document(\@docs);
print "Total $removed words were removed.\n";

$index_dbh->disconnect;
$document_dbh->disconnect;
