package DBIx::TextIndex::DBD::mysql;

use strict;

our $VERSION = '0.25';

use base qw(DBIx::TextIndex::DBD);

sub insert_doc_key {
    my $self = shift;
    my $doc_key = shift;

    my $sql = <<END;
INSERT INTO $self->{DOC_KEY_TABLE} (doc_key) VALUES (?)
END

    $self->{INDEX_DBH}->do($sql, undef, $doc_key);
    my $doc_id = $self->{INDEX_DBH}->{mysql_insertid};
    return $doc_id;
}


1;

