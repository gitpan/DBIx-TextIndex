package DBIx::TextIndex::DBD::SQLite;

use strict;

our $VERSION = '0.24';

use base qw(DBIx::TextIndex::DBD);

sub create_collection_table {
    my $self = shift;
    my $collection_length = DBIx::TextIndex::COLLECTION_NAME_MAX_LENGTH;
    return <<END;
CREATE TABLE collection (
  collection varchar($collection_length) PRIMARY KEY default '',
  version numeric(10,2) NOT NULL default 0.00,
  max_indexed_id int NOT NULL default 0,
  doc_table varchar(30),
  doc_id_field varchar(30),
  doc_fields varchar(250) NOT NULL default '',
  charset varchar(50) NOT NULL default '',
  stoplist varchar(255) NOT NULL default '',
  proximity_index varchar(1) NOT NULL default '0',
  error_empty_query varchar(255) NOT NULL default '',
  error_quote_count varchar(255) NOT NULL default '',
  error_no_results varchar(255) NOT NULL default '',
  error_no_results_stop varchar(255) NOT NULL default '',
  error_wildcard_length varchar(255) NOT NULL default '',
  error_wildcard_expansion varchar(255) NOT NULL default '',
  max_word_length int NOT NULL default 0,
  result_threshold int NOT NULL default 0,
  phrase_threshold int NOT NULL default 0,
  min_wildcard_length int NOT NULL default 0,
  max_wildcard_term_expansion int NOT NULL default 0,
  decode_html_entities varchar(1) NOT NULL default '0',
  scoring_method varchar(20) NOT NULL default '',
  update_commit_interval int NOT NULL default 0
)
END

}

sub update_docweights_execute {
    my $self = shift;
    my ($sth, $fno, $avg_w_d, $packed_w_d) = @_;
    $packed_w_d =~ s/\\/\\\\/g;
    $packed_w_d =~ s/\0/\\0/g;

    $sth->execute($fno, $avg_w_d, $packed_w_d);
}

sub create_mask_table {
    my $self = shift;

    return <<END;
CREATE TABLE $self->{MASK_TABLE} (
  mask             varchar(100)    PRIMARY KEY,
  docs_vector text 	           NOT NULL
);
END

}

sub create_docweights_table {
    my $self = shift;
    return <<END;
CREATE TABLE $self->{DOCWEIGHTS_TABLE} (
  field_no 	   integer 	   PRIMARY KEY,
  avg_docweight    real            NOT NULL,
  docweights 	   blob 	   NOT NULL
)
END
}

sub create_all_docs_vector_table {
    my $self = shift;

    return <<END;
CREATE TABLE $self->{ALL_DOCS_VECTOR_TABLE} (
  id               integer           PRIMARY KEY,
  all_docs_vector  text              NOT NULL
)
END
}

sub create_delete_queue_table {
    my $self = shift;

    return <<END;
CREATE TABLE $self->{DELETE_QUEUE_TABLE} (
  id                   integer            PRIMARY KEY,
  delete_queue         text               NOT NULL
)
END
}

sub create_inverted_table {
    my $self = shift;
    my $table = shift;
    my $max_word = $self->{MAX_WORD_LENGTH};

    return <<END;
CREATE TABLE $table (
  term             varchar($max_word)      PRIMARY KEY,
  docfreq_t 	   int                     NOT NULL,
  term_docs	   blob 		   NOT NULL,
  term_pos         blob                    NOT NULL
)
END

}

sub create_doc_key_table {
    my $self = shift;
    my $doc_key_sql_type = $self->{DOC_KEY_SQL_TYPE};
    $doc_key_sql_type .= "($self->{DOC_KEY_LENGTH})"
	if $self->{DOC_KEY_LENGTH};

    return <<END;
CREATE TABLE $self->{DOC_KEY_TABLE} (
  doc_id           INTEGER               NOT NULL PRIMARY KEY,
  doc_key          $doc_key_sql_type     NOT NULL,
  UNIQUE (doc_key)
)
END

}



1;
