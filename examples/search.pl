#!/usr/local/bin/perl

use strict;

use DBI;
use DBIx::TextIndex;

my $DB = 'DBI:mysql:test';
my $DBAUTH = ':';

my $document_dbh = DBI->connect($DB, split(':', $DBAUTH, 2)) or die $DBI::errstr;
my $index_dbh = DBI->connect($DB, split(':', $DBAUTH, 2)) or die $DBI::errstr;

my $index = DBIx::TextIndex->new({
    document_dbh => $document_dbh,
    index_dbh => $index_dbh,
    collection => 'encantadas',
});

print "Enter a search string: ";

my $query = <STDIN>;

chomp $query;

my $results = $index->search({doc => $query});

if (ref $results) {
    foreach my $doc_id (sort {$$results{$b} <=> $$results{$a}} keys %$results)
    {
	print "Paragraph: $doc_id  Score: $$results{$doc_id}\n";
    }
} else {
    print "\n$results\n\n";
}

$index_dbh->disconnect;
$document_dbh->disconnect;
