#!/usr/bin/perl

# MySQL module for DBIx::TextIndex

use strict;

sub db_add_mask {
    my $self = shift;
    return <<END;
replace into $self->{MASK_TABLE} (mask, documents_vector)
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
	my $sql = "

		DROP TABLE IF EXISTS $table

    ";
	$self->{INDEX_DBH}->do($sql);
}

sub db_table_exists {
    my $self = shift;
    my $table = shift;
    my $sql = "desc $table";
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    $sth->{PrintError} = 0;
    $sth->{RaiseError} = 0;
    $sth->execute;
    my $table_exists = $sth->rows > 0 ? 1 : 0;
    $sth->finish;
    return $table_exists;
}

sub db_create_collection_table {
    my $self = shift;
    return <<END;
CREATE TABLE collection (
  collection varchar(30) NOT NULL default '',
  version decimal(10,2) NOT NULL default '0.00',
  max_indexed_id int(10) unsigned NOT NULL default '0',
  document_table varchar(30) NOT NULL default '',
  document_id_field varchar(30) NOT NULL default '',
  document_fields varchar(250) NOT NULL default '',
  language char(2) NOT NULL default '',
  stoplist varchar(255) NOT NULL default '',
  proximity_index enum('0', '1') NOT NULL default '0',
  error_quote_count varchar(255) NOT NULL default '',
  error_empty_query varchar(255) NOT NULL default '',
  error_no_results varchar(255) NOT NULL default '',
  error_no_results_stop varchar(255) NOT NULL default '',
  max_word_length int(10) unsigned NOT NULL default '0',
  result_threshold int(10) unsigned NOT NULL default '0',
  phrase_threshold int(10) unsigned NOT NULL default '0',
  min_wildcard_length int(10) unsigned NOT NULL default '0',
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
	my $result_documents = shift;
	my $fno = shift;

    return "

        SELECT $self->{DOCUMENT_ID_FIELD}, $self->{DOCUMENT_FIELDS}->[$fno]
        FROM   $self->{DOCUMENT_TABLE}
        WHERE  $self->{DOCUMENT_ID_FIELD} IN ($result_documents)

    ";
}

sub db_phrase_scan {
	my $self = shift;
	my $result_documents = shift;
	my $fno = shift;

    return "

        SELECT $self->{DOCUMENT_ID_FIELD}
        FROM   $self->{DOCUMENT_TABLE}
        WHERE  $self->{DOCUMENT_ID_FIELD} IN ($result_documents)
        	   AND $self->{DOCUMENT_FIELDS}->[$fno] LIKE ?

    ";
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

sub db_occurence {
	my $self = shift;
	my $table = shift;

    return "

		SELECT occurence FROM $table
		WHERE word = ?

    ";
}

sub db_fetch_mask {
	my $self = shift;

    return "

    	SELECT documents_vector
        FROM $self->{MASK_TABLE}
        WHERE mask = ?

     ";
}

sub db_fetch_documents {
	my $self = shift;
	my $table = shift;

    return "

    	SELECT documents
		FROM $table
        WHERE word = ?

	";
}

sub db_fetch_documents_vector {
    my $self = shift;
    my $table = shift;

    return <<END;
select documents_vector
from $table
where word = ?
END

}

sub db_fetch_words {
	my $self = shift;
	my $table = shift;

    return "

    	SELECT word
		FROM $table
        WHERE word LIKE ?

	";
}

sub db_ping_document {
	my $self = shift;

	return "

		SELECT 1
		FROM $self->{DOCUMENT_TABLE}
        WHERE $self->{DOCUMENT_ID_FIELD} = ?

	";
}

sub db_fetch_document {
	my $self = shift;
	my $field = shift;

	return "

		SELECT $field
        FROM $self->{DOCUMENT_TABLE}
        WHERE $self->{DOCUMENT_ID_FIELD} = ?

	";
}

sub db_update_maxtf {
	my $self = shift;

    return "

        REPLACE INTO $self->{MAXTF_TABLE} (field_no, maxtf)
        VALUES (?, ?)

    ";
}

sub db_inverted_replace {
	my $self = shift;
	my $table = shift;

	return "

        REPLACE INTO $table
        (word, occurence, documents_vector, documents)
        VALUES (?, ?, ?, ?)

	";
}

sub db_inverted_remove {
	my $self = shift;
	my $table = shift;

	return "

        DELETE FROM $table
		WHERE word = ?

	";
}

sub db_inverted_select {
	my $self = shift;
	my $table = shift;

	return "

		SELECT occurence, documents_vector, documents
		FROM $table
		WHERE word = ?

    ";
}

sub db_create_mask {
	my $self = shift;

	return "

    	CREATE TABLE $self->{MASK_TABLE} (
			mask 				varchar(100) 	not null,
			documents_vector 	mediumblob 		not null,
            PRIMARY KEY 		mask_key (mask)
		)

  ";
}

sub db_create_maxterm {
	my $self = shift;

	return "

        CREATE TABLE $self->{MAXTF_TABLE} (
			field_no 			smallint unsigned 	not null,
			maxtf 				mediumblob 			not null,
			PRIMARY KEY 		field_no_key (field_no)
		)

	";
}

sub db_create_inverted {
	my $self = shift;
	my $table = shift;
	my $max_word = $self->{MAX_WORD_LENGTH};

	return "

		CREATE TABLE $table (
 			word 				varchar($max_word) 	not null,
			occurence 			int unsigned 		not null,
			documents_vector 	mediumblob 			not null,
			documents			mediumblob 			not null,
			PRIMARY KEY 		word_key (word)
		)

	";
}

sub db_pindex_search {
	my $self = shift;
	my $fno = shift;
	my $words = shift;
	my $documents = shift;

	return "

		SELECT word, document, pos
		FROM $self->{PINDEX_TABLES}->[$fno]
		WHERE document IN ($documents) AND
			  word IN ($words)
		ORDER BY document

	";
}

sub db_pindex_create {
	my $self = shift;
	my $table = shift;
	my $max_word = $self->{MAX_WORD_LENGTH};

	return "

		CREATE TABLE $table (
			word		VARCHAR($max_word)		NOT NULL,
			document	INTEGER					NOT NULL,
			pos			INTEGER					NOT NULL,
			INDEX		(document, word)
		)
	";
}

sub db_pindex_add {
	my $self = shift;
	my $table = shift;

	return "

		INSERT INTO $table (word, document, pos)
		VALUES (?, ?, ?)

	";
}

sub db_pindex_remove {
	my $self = shift;
	my $table = shift;
	my $documents = shift;

	return "

		DELETE FROM $table
		WHERE document IN ($documents)

	";
}


sub db_total_words {
	my $self = shift;
	my $table = shift;
	
	return "

		SELECT SUM(occurence)
		FROM $table
		
	";
}

1;

