#!/usr/local/bin/perl

use strict;

use DBI;
use DBIx::TextIndex;

my $DB = 'DBI:mysql:test';
my $DBAUTH = ':';


my $document_dbh = DBI->connect($DB, split(':', $DBAUTH, 2)) or die $DBI::errstr;
my $index_dbh = DBI->connect($DB, split(':', $DBAUTH, 2)) or die $DBI::errstr;

my $sth = $document_dbh->prepare('select max(doc_id) from textindex_doc');
$sth->execute;
my ($max_doc_id) = $sth->fetchrow_array;
$sth->finish;

my $index = DBIx::TextIndex->new({
    document_dbh => $document_dbh,
    document_table => 'textindex_doc',
    document_fields => ['doc'],
    document_id_field => 'doc_id',
    index_dbh => $index_dbh,
    collection => 'encantadas',
    print_activity => 1,
});

$index->initialize;

$index->add_document([1 .. $max_doc_id]);

$document_dbh->disconnect;
$index_dbh->disconnect;

print "Done.\n";
