# PostgreSQL module for DBIx::TextIndex

use strict;
use DBD::Pg;

sub db_add_mask {
    my $self = shift;
    my ($mask, $vector_enum) = @_;
    my $sql = <<END;
DELETE FROM $self->{MASK_TABLE} where mask = ?;
INSERT into $self->{MASK_TABLE} (mask, docs_vector) values (?, ?)
END

    $self->{INDEX_DBH}->do($sql, undef, $mask, $mask, $vector_enum);
}

sub db_delete_mask {
    my $self = shift;
    return <<END;
delete from $self->{MASK_TABLE}
where mask = ?
END

}

sub db_drop_table {
    my $self = shift;
    my $table = shift;

	if( $self->{INDEX_DBH}->selectrow_array("SELECT tablename FROM pg_tables WHERE tablename = '$table'") ) {
		$self->{INDEX_DBH}->do("DROP TABLE $table");
	}
}

sub db_table_exists {
    my $self = shift;
    my $table = shift;

	return 1 if $self->{INDEX_DBH}->selectrow_array("SELECT tablename FROM pg_tables WHERE tablename = '$table'");
    return 0;
}

sub db_create_collection_table {
    my $self = shift;
    return <<END;
CREATE TABLE collection (
  collection varchar(30) PRIMARY KEY default '',
  version numeric(10,2) NOT NULL default 0.00,
  max_indexed_id int NOT NULL default 0,
  doc_table varchar(30) NOT NULL default '',
  doc_id_field varchar(30) NOT NULL default '',
  doc_fields varchar(250) NOT NULL default '',
  charset varchar(50) NOT NULL default '',
  stoplist varchar(255) NOT NULL default '',
  proximity_index varchar(1) NOT NULL default '0',
  error_empty_query varchar(255) NOT NULL default '',
  error_quote_count varchar(255) NOT NULL default '',
  error_no_results varchar(255) NOT NULL default '',
  error_no_results_stop varchar(255) NOT NULL default '',
  max_word_length int NOT NULL default 0,
  result_threshold int NOT NULL default 0,
  phrase_threshold int NOT NULL default 0,
  min_wildcard_length int NOT NULL default 0,
  decode_html_entities varchar(1) NOT NULL default '0',
  scoring_method varchar(20) NOT NULL default '',
  update_commit_interval int NOT NULL default 0
)
END

}

sub db_insert_collection_table_row {
    my $self = shift;
    my $row = shift;
    my @fields;
    my @values;
    while (my ($field, $value) = each %$row) {
	push @fields, $field;
	push @values, $value;
    }
    my $collection_fields = join ', ', @fields;
    my $place_holders = join ', ', (('?') x ($#fields + 1)); 
    my $sql = <<END;
insert into $self->{COLLECTION_TABLE}
($collection_fields)
values ($place_holders)
END
    $self->{INDEX_DBH}->do($sql, undef, @values);

}

sub db_fetch_max_indexed_id {
    my $self = shift;

    return <<END;
SELECT max_indexed_id
FROM $self->{COLLECTION_TABLE}
WHERE collection = ?
END

}

sub db_fetch_collection_version {
    my $self = shift;

    return <<END;
select max(version) from $self->{COLLECTION_TABLE}
END

}

sub db_collection_count {
    my $self = shift;

    return <<END;
select count(*) from $self->{COLLECTION_TABLE}
END

}

sub db_update_collection_info {
    my $self = shift;
    my $field = shift;

    return <<END;
update $self->{COLLECTION_TABLE}
set $field = ?
where collection = ?
END

}

sub db_delete_collection_info {
    my $self = shift;

    return <<END;
delete from $self->{COLLECTION_TABLE}
where collection = ?
END

}

sub db_store_collection_info {
    my $self = shift;

    my @collection_fields = @{$self->{COLLECTION_FIELDS}};
    my $collection_fields = join ', ', @collection_fields;
    my $place_holders = join ', ', (('?') x ($#collection_fields + 1)); 
    return <<END;
insert into $self->{COLLECTION_TABLE}
($collection_fields)
values
($place_holders)
END

}

sub db_fetch_collection_info {
    my $self = shift;

    my $collection_fields = join ', ', @{$self->{COLLECTION_FIELDS}};

    return <<END;
select
$collection_fields
from $self->{COLLECTION_TABLE}
where collection = ?
END

}

sub db_fetch_all_collection_rows {
    my $self = shift;

    return <<END;
select * from $self->{COLLECTION_TABLE}
END

}

sub db_phrase_scan_cz {
    my $self = shift;
    my $result_docs = shift;
    my $fno = shift;

    return <<END;
select $self->{DOC_ID_FIELD}, $self->{DOC_FIELDS}->[$fno]
from   $self->{DOC_TABLE}
where  $self->{DOC_ID_FIELD} in ($result_docs)
END

}

sub db_phrase_scan {
    my $self = shift;
    my $result_docs = shift;
    my $fno = shift;

    return <<END;
select $self->{DOC_ID_FIELD}
from   $self->{DOC_TABLE}
where  $self->{DOC_ID_FIELD} IN ($result_docs)
       and $self->{DOC_FIELDS}->[$fno] like ?
END

}

sub db_fetch_docweights {
    my $self = shift;
    my $fields = shift;

    return <<END;
select field_no, avg_docweight, docweights
from $self->{DOCWEIGHTS_TABLE}
where field_no in ($fields)
END

}

sub db_fetch_all_docs_vector {
    my $self = shift;
    return <<END;
SELECT all_docs_vector
from $self->{ALL_DOCS_VECTOR_TABLE}
END

}

sub db_update_all_docs_vector {
    my $self = shift;
    return <<END;
DELETE FROM $self->{ALL_DOCS_VECTOR_TABLE} WHERE id = 1;
INSERT INTO $self->{ALL_DOCS_VECTOR_TABLE}
(id, all_docs_vector)
VALUES (1, ?)
END
}

sub db_fetch_mask {
    my $self = shift;

    return <<END;
select docs_vector
from $self->{MASK_TABLE}
where mask = ?
END

}

sub db_fetch_term_pos {
    my $self = shift;
    my $table = shift;

    return <<END;
select term_pos
from $table
where word = ?
END

}

sub db_fetch_term_docs {
    my $self = shift;
    my $table = shift;

    return <<END;
select term_docs
from $table
where word = ?
END

}

sub db_fetch_term_freq_and_docs {
    my $self = shift;
    my $table = shift;
    return <<END;
select docfreq_t, term_docs
from $table
where word = ?
END

}

sub db_fetch_words {
    my $self = shift;
    my $table = shift;

    return <<END;
select word
from $table
where word like ?
END

}

sub db_ping_doc {
    my $self = shift;

    return <<END;
select 1
from $self->{DOC_TABLE}
where $self->{DOC_ID_FIELD} = ?
END

}

sub db_fetch_doc {
    my $self = shift;
    my $field = shift;

    return <<END;
select $field
from $self->{DOC_TABLE}
where $self->{DOC_ID_FIELD} = ?
END

}

sub db_update_docweights {
    my $self = shift;

    return <<END;
DELETE FROM $self->{DOCWEIGHTS_TABLE} WHERE field_no = ?;
INSERT into $self->{DOCWEIGHTS_TABLE} (field_no, avg_docweight, docweights) values (?, ?, ?)
END

}

sub db_update_docweights_execute {
    my $self = shift;
    my ($sth, $fno, $avg_w_d, $packed_w_d) = @_;
    $sth->bind_param( 1, $fno );
    $sth->bind_param( 2, $fno );
    $sth->bind_param( 3, $avg_w_d );
    $sth->bind_param( 4, $packed_w_d, { pg_type => DBD::Pg::PG_BYTEA } );
    $sth->execute();
}

sub db_inverted_replace {
    my $self = shift;
    my $table = shift;

    return <<END;
DELETE FROM $table WHERE word = ?;
INSERT into $table
(word, docfreq_t, term_docs, term_pos)
values (?, ?, ?, ?)
END

}

sub db_inverted_replace_execute {
    my $self = shift;
    my ($sth, $term, $docfreq_t, $term_docs, $term_pos) = @_;

    $sth->bind_param( 1, $term );
    $sth->bind_param( 2, $term );
    $sth->bind_param( 3, $docfreq_t );
    $sth->bind_param( 4, $term_docs, { pg_type => DBD::Pg::PG_BYTEA } );
    $sth->bind_param( 5, $term_pos, { pg_type => DBD::Pg::PG_BYTEA } );
    $sth->execute() or warn $self->{INDEX_DBH}->err;
}

sub db_inverted_remove {
    my $self = shift;
    my $table = shift;
    
    return <<END;
delete from $table
where word = ?
END

}

sub db_inverted_select {
    my $self = shift;
    my $table = shift;

    return <<END;
select docfreq_t, term_docs, term_pos
from $table
where word = ?
END

}

sub db_create_mask_table {
    my $self = shift;

    return <<END;
create table $self->{MASK_TABLE} (
  mask             varchar(100) primary key,
  docs_vector text 	           not null
);
END

}

sub db_create_docweights_table {
    my $self = shift;
    return <<END;
create table $self->{DOCWEIGHTS_TABLE} (
  field_no 	   integer 	   primary key,
  avg_docweight    float                   not null,
  docweights 	   bytea 		   not null
)
END
}

sub db_create_all_docs_vector_table {
    my $self = shift;

    return <<END;
CREATE TABLE $self->{ALL_DOCS_VECTOR_TABLE} (
  id               INT PRIMARY KEY,
  all_docs_vector  text              NOT NULL
)
END
}

sub db_create_inverted_table {
    my $self = shift;
    my $table = shift;
    my $max_word = $self->{MAX_WORD_LENGTH};

    return <<END;
create table $table (
  word             varchar($max_word)      primary key,
  docfreq_t 	   int                     not null,
  term_docs	   bytea 		   not null,
  term_pos         bytea                   not null
)
END

}

sub db_total_words {
    my $self = shift;
    my $table = shift;

    return <<END;
select SUM(docfreq_t)
from $table
END

}

1;

