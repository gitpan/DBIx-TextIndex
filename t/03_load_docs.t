use strict;

use Test::More;
use DBI;
use DBIx::TextIndex;

my $TESTDATA = 'testdata/encantadas.txt';

if (defined $ENV{DBI_DSN}) {
    plan tests => 5;
} else {
    plan skip_all => '$ENV{DBI_DSN} must be defined to run tests.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, { RaiseError => 1, PrintError => 0, AutoCommit => 1 });

ok( defined $dbh && $dbh->ping );

my $dbd = $dbh->{Driver}->{Name};

if ($dbd eq 'mysql' or $dbd eq 'SQLite' or $dbd eq 'Pg') {
    my @tables = $dbh->tables(undef, undef, 'textindex_doc', 'table');
    my $table_exists = 0;
    foreach my $table (@tables) {
	if ($table =~ m/^[\"\`]?textindex_doc[\"\`]?$/) {
	    $table_exists = 1;
	    last;
	}
    }
    if ($table_exists) {
        ok( defined($dbh->do('DROP TABLE textindex_doc')) );
    } else {
	ok(1);
    }
    ok( defined($dbh->do(<<END) ) );
CREATE TABLE textindex_doc(
doc_id INT NOT NULL PRIMARY KEY,
doc TEXT)
END

} else {
    print "Bail out! Unsupported DBD driver: $dbd\n";
}

{
    local $/ ="\n\n";

    my $sth = $dbh->prepare( qq(INSERT INTO textindex_doc (doc_id, doc) values (?, ?)) ) || die $dbh->errstr;

    open F, $TESTDATA or die "open file error $TESTDATA, $!, stopped";
    my $doc_id = 1;
    while (<F>) {
	$sth->execute($doc_id, $_) || die $dbh->errstr;
	$doc_id++;
    }
    close F;
}

ok ( (226) == $dbh->selectrow_array(qq(SELECT COUNT(*) from textindex_doc)) );

my $doc_226 =  qq("Oh, Brother Jack, as you pass by,\nAs you are now, so once was I.\nJust so game, and just so gay,\nBut now, alack, they've stopped my pay.\nNo more I peep out of my blinkers,\nHere I be -- tucked in with clinkers!"\n);

ok ( ($doc_226) eq $dbh->selectrow_array(qq(SELECT doc FROM textindex_doc where doc_id = ?), undef, 226) );

$dbh->disconnect;