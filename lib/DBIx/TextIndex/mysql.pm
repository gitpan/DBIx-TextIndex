#!/usr/bin/perl

# MySQL module for DBIx::TextIndex

use strict;

sub db_add_mask {
    my $self = shift;
    return <<END;
replace into $self->{MASK_TABLE} (mask, docs_vector)
values (?, ?)
END

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
    my $sql = <<END;
drop table if exists $table
END

    $self->{INDEX_DBH}->do($sql);

}

sub db_table_exists {
    my $self = shift;
    my $table = shift;
    # FIXME: $dbh->tables is marked deprecated in DBI 1.30 documentation
    my @tables = $self->{INDEX_DBH}->tables;
    for (@tables) {
	return 1 if $table eq $_;
    }
    return 0;
}

sub db_create_collection_table {
    my $self = shift;
    return <<END;
CREATE TABLE collection (
  collection varchar(30) NOT NULL default '',
  version decimal(10,2) NOT NULL default '0.00',
  max_indexed_id int(10) unsigned NOT NULL default '0',
  doc_table varchar(30) NOT NULL default '',
  doc_id_field varchar(30) NOT NULL default '',
  doc_fields varchar(250) NOT NULL default '',
  charset varchar(50) NOT NULL default '',
  stoplist varchar(255) NOT NULL default '',
  proximity_index enum('0', '1') NOT NULL default '0',
  error_empty_query varchar(255) NOT NULL default '',
  error_quote_count varchar(255) NOT NULL default '',
  error_no_results varchar(255) NOT NULL default '',
  error_no_results_stop varchar(255) NOT NULL default '',
  max_word_length int(10) unsigned NOT NULL default '0',
  result_threshold int(10) unsigned NOT NULL default '0',
  phrase_threshold int(10) unsigned NOT NULL default '0',
  min_wildcard_length int(10) unsigned NOT NULL default '0',
  decode_html_entities enum('0', '1') NOT NULL default '0',
  PRIMARY KEY collection_key (collection)
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

sub db_fetch_maxtf {
    my $self = shift;
    my $fields = shift;

    return <<END;
select field_no, maxtf
from $self->{MAXTF_TABLE}
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
REPLACE INTO $self->{ALL_DOCS_VECTOR_TABLE}
(id, all_docs_vector)
VALUES (1, ?)
END
}

sub db_freq_d {
    my $self = shift;
    my $table = shift;

    return <<END;
select freq_d from $table
where word = ?
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

sub db_fetch_docs {
    my $self = shift;
    my $table = shift;

    return <<END;
select docs
from $table
where word = ?
END

}

sub db_fetch_freq_and_docs_vector {
    my $self = shift;
    my $table = shift;

    return <<END;
select freq_d, docs_vector
from $table
where word = ?
END

}

sub db_fetch_docs_vector {
    my $self = shift;
    my $table = shift;

    return <<END;
select docs_vector
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

sub db_update_maxtf {
    my $self = shift;

    return <<END;
replace into $self->{MAXTF_TABLE} (field_no, maxtf)
values (?, ?)
END

}

sub db_inverted_replace {
    my $self = shift;
    my $table = shift;

    return <<END;
replace into $table
(word, freq_d, docs_vector, docs)
values (?, ?, ?, ?)
END

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
select freq_d, docs_vector, docs
from $table
where word = ?
END

}

sub db_create_mask_table {
    my $self = shift;

    return <<END;
create table $self->{MASK_TABLE} (
  mask             varchar(100)            not null,
  docs_vector mediumblob 	           not null,
  primary key 	   mask_key (mask)
)
END

}

sub db_create_maxterm_table {
    my $self = shift;

    return <<END;
create table $self->{MAXTF_TABLE} (
  field_no 	   smallint unsigned 	   not null,
  maxtf 	   mediumblob 		   not null,
  primary key 	   field_no_key (field_no)
)
END

}

sub db_create_all_docs_vector_table {
    my $self = shift;

    return <<END;
CREATE TABLE $self->{ALL_DOCS_VECTOR_TABLE} (
  id               INT UNSIGNED            NOT NULL,
  all_docs_vector  MEDIUMBLOB              NOT NULL,
  UNIQUE KEY       id_key (id)
)
END
}


sub db_create_inverted_table {
    my $self = shift;
    my $table = shift;
    my $max_word = $self->{MAX_WORD_LENGTH};

    return <<END;
create table $table (
  word             varchar($max_word)      not null,
  freq_d 	   int unsigned 	   not null,
  docs_vector mediumblob 		   not null,
  docs	   mediumblob 		   not null,
  PRIMARY KEY 	   word_key (word)
)
END

}

sub db_pindex_search {
    my $self = shift;
    my $fno = shift;
    my $words = shift;
    my $docs = shift;

    return <<END;
select word, doc, pos
from $self->{PINDEX_TABLES}->[$fno]
where doc in ($docs) and word in ($words)
order by doc
END

}

sub db_pindex_create {
    my $self = shift;
    my $table = shift;
    my $max_word = $self->{MAX_WORD_LENGTH};

    return <<END;
create table $table (
  word		   varchar($max_word)	   not null,
  doc	           integer                 not null,
  pos		   integer		   not null,
  index		   (doc, word)
)
END

}

sub db_pindex_add {
    my $self = shift;
    my $table = shift;

    return <<END;
insert into $table (word, doc, pos)
values (?, ?, ?)
END

}

sub db_pindex_remove {
    my $self = shift;
    my $table = shift;
    my $docs = shift;

    return <<END;
delete from $table
where doc in ($docs)
END

}


sub db_total_words {
    my $self = shift;
    my $table = shift;

    return <<END;
select SUM(freq_d)
from $table
END

}

1;

