use Test::More;
use DBI;
use DBIx::TextIndex;

use strict;

if (defined $ENV{DBI_DSN}) {
    plan tests => 1;
} else {
    plan skip_all => '$ENV{DBI_DSN} must be defined to run tests.';
}

my $dbh;
eval {
    $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS}, { RaiseError => 1, PrintError => 0, AutoCommit => 1 });
};
if ($@) {
    if (! $DBI::errstr) {
	print "Bail out! Could not connect to database: $@\n";
    } else {
	print "Bail out! Could not connect to database: $DBI::errstr\n";
    }
    exit;
}

ok( defined $dbh && $dbh->ping);
