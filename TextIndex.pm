package DBIx::TextIndex;

use strict;

use Bit::Vector;
use Carp qw(carp croak);

$DBIx::TextIndex::VERSION = '0.04';

# Largest size word to be indexed
my $MAX_WORD = 30;

# Used to screen stop words from the scoring process
my $IDF_THRESHOLD = 0.2;

# What can be considered too many results
my $RESULT_THRESHOLD = 5000;

# Practical number of rows RDBMS can scan in acceptable amount of time
my $PHRASE_THRESHOLD = 1000;

my %ERROR = (
	     empty_query => "You must be searching for something!",
	     quote_count => "Quotes must be used in matching pairs.",
	     no_results  => "Your search did not produce any matching documents."
	     );

my $COLLECTION_TABLE = 'collection';

my @MASK_TYPE = qw(and_mask or_mask not_mask);


sub new {

    my $pkg = shift;

    my $args = shift;

    my $class = ref($pkg) || $pkg;

    my $self = bless {}, $class;

    foreach my $arg ('collection', 'index_dbh', 'document_dbh') {
	if ($args->{$arg}) {
	    $self->{uc $arg} = $args->{$arg};
	} else {
	    croak "new $pkg needs $arg argument";
	}
    }

    $self->{PRINT_ACTIVITY} = 0;
    $self->{PRINT_ACTIVITY} = $args->{'print_activity'};

    unless ($self->_fetch_collection_info) {

	$self->{DOCUMENT_TABLE} = $args->{document_table};
	$self->{DOCUMENT_FIELDS} = $args->{document_fields};
	$self->{DOCUMENT_ID_FIELD} = $args->{document_id_field};
	
    }

    $self->{MAXTF_TABLE} = $self->{COLLECTION} . '_maxtf';
    $self->{MASK_TABLE} = $self->{COLLECTION} . '_mask';

    my $field_no = 0;
    foreach my $field ( @{$self->{DOCUMENT_FIELDS}} ) {
	$self->{FIELD_NO}->{$field} = $field_no;
	push @{$self->{INVERTED_TABLES}}, ($self->{COLLECTION} . '_' . $field . '_inverted');
	$field_no++;
    }

    return $self;

}

sub add_mask {

    my $self = shift;
    my $mask = shift;
    my $ids = shift;

    my $max_indexed_id = $self->max_indexed_id;

    # Trim ids from end instead here.
    if ($ids->[-1] > $max_indexed_id) {
	carp "Greatest document_id in mask ($mask) is larger than greatest document_id in index";
	return 0;
    }

    my $vector = Bit::Vector->new($max_indexed_id + 1);
    $vector->Index_List_Store(@$ids);

    my $sql = qq(replace into $self->{MASK_TABLE} (mask, documents_vector) values (?, ?));

    $self->{INDEX_DBH}->do($sql, undef, $mask, $vector->to_Enum);

    return 1;

}

sub delete_mask {

    my $self = shift;
    my $mask = shift;

    my $sql = qq(delete from $self->{MASK_TABLE} where mask = ?);

    $self->{INDEX_DBH}->do($sql, undef, $mask);

}

sub add_document {

    my $self = shift;

    my $ids = shift;

    return if $#$ids < 0;

    if ($self->{PRINT_ACTIVITY}) {
	my $add_count = $#{$ids} + 1;
	print "Adding $add_count documents\n";
    }

    $self->{OLD_MAX_INDEXED_ID} = $self->max_indexed_id;

    $self->max_indexed_id($ids->[-1]);

    foreach my $document_id (@$ids) {

	print $document_id if $self->{PRINT_ACTIVITY};

	next unless $self->_ping_document($document_id);

	foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {

	    print " field$field_no" if $self->{PRINT_ACTIVITY};

	    my %frequency;
	    my $maxtf = 0;

	    my $word_count = 0;

	    foreach my $word ($self->_words($self->_fetch_document($document_id, $self->{DOCUMENT_FIELDS}->[$field_no]))) {
		$frequency{$word}++;
		$maxtf = $frequency{$word} if $frequency{$word} > $maxtf;
		$word_count++;
	    }
	    print " $word_count" if $self->{PRINT_ACTIVITY};

	    while (my ($word, $frequency) = each %frequency) {

		$self->_documents($field_no, $word, $document_id, $frequency);
	    }

	    $self->{NEW_MAXTF}->[$field_no]->[$document_id] = $maxtf;

	}

	print "\n" if $self->{PRINT_ACTIVITY};

    }

    $self->_commit_documents;


}

sub search {

    my $self = shift;
    my $query = shift;
    my $args = shift;

    return $ERROR{empty_query} unless $query;

    my @field_nos;
    foreach my $field (keys %$query) {
	push @field_nos, $self->{FIELD_NO}->{$field};
    }

    @{$self->{QUERY_FIELD_NOS}} = sort { $a <=> $b } @field_nos;

    foreach my $field (keys %$query) {
	$self->{QUERY}->[$self->{FIELD_NO}->{$field}] = $query->{$field};
    }

    if (my $error = $self->_parse_query) {
	return $error;
    }

    foreach my $mask_type (@MASK_TYPE) {
	if ($args->{$mask_type}) {
	    $self->{MASK}->{$mask_type} = $args->{$mask_type};
	    foreach my $mask (@{$args->{$mask_type}}) {
		if (ref $mask) {
		    $self->{VALID_MASK} = 1;
		} else {
		    push @{$self->{MASK_FETCH_LIST}}, $mask;
		}
	    }
	}
    }

    if ($args->{or_mask_set}) {
	$self->{MASK}->{or_mask_set} = $args->{or_mask_set};
	foreach my $mask_set (@{$args->{or_mask_set}}) {
	    foreach my $mask (@$mask_set) {
		if (ref $mask) {
		    $self->{VALID_MASK} = 1;
		} else {
		    push @{$self->{MASK_FETCH_LIST}}, $mask;
		}
	    }
	}
    }

    $self->_vector_search;
    $self->_boolean_compare;
    $self->_apply_mask;
    $self->_phrase_search;

    my $results = $self->_search;

    return $results;

}

sub highlight { $_[0]->{HIGHLIGHT} }

sub initialize {
    my $self = shift;

    $self->{MAX_INDEXED_ID} = 0;

    unless ($self->_collection_table_exists) {
	$self->_create_collection_table;
    }
    $self->_create_tables;
    $self->_delete_collection_info;
    $self->_store_collection_info;
}

sub max_indexed_id {

    my $self = shift;

    my $max_indexed_id = shift;
    if (defined $max_indexed_id) {
	$self->_update_collection_info('max_indexed_id', $max_indexed_id);
	return $self->{MAX_INDEXED_ID};
    } else {
	return $self->{MAX_INDEXED_ID};
    }
}

sub delete {

    my $self = shift;

    print "Deleting $self->{COLLECTION} from collection table\n"
	if $self->{PRINT_ACTIVITY};
    $self->_delete_collection_info;

    print "Dropping mask table ($self->{MASK_TABLE})\n"
	if $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do("drop table if exists $self->{MASK_TABLE}");

    print "Dropping max term frequency table ($self->{MAXTF_TABLE})\n"
	if $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do("drop table if exists $self->{MAXTF_TABLE}");

    foreach my $table ( @{$self->{INVERTED_TABLES}} ) {
	print "Dropping inverted table ($table)\n" if $self->{PRINT_ACTIVITY};
	$self->{INDEX_DBH}->do("drop table if exists $table");
    }

}

sub _collection_table_exists {

    my $self = shift;

    my $sql = qq(desc $COLLECTION_TABLE);

    my $sth = $self->{INDEX_DBH}->prepare($sql);
    $sth->{PrintError} = 0;

    my $table_exists = 0;

    $sth->execute;
    $table_exists = 1 if $sth->rows > 0;
    $sth->finish;

    return $table_exists;

}

sub _create_collection_table {

    my $self = shift;

    my $sql = <<END;
create table collection (
    collection char(30) default '' not null,
    max_indexed_id int(10) unsigned not null,
    document_table char(30) default '' not null,
    document_id_field char(30) default '' not null,
    document_fields char(250) default '' not null,
    primary key collection_key (collection)
)
END

    print "Creating collection table ($COLLECTION_TABLE)\n"
	if $self->{PRINT_ACTIVITY};

    $self->{INDEX_DBH}->do($sql) || croak $DBI::errstr;

}

sub _update_collection_info {

    my $self = shift;

    my ($field, $value) = @_;

    my $attribute = $field;
    $attribute =~ tr/[a-z]/[A-Z]/;

    my $sql = qq(update collection set $field = ? where collection = ?);

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($value, $self->{COLLECTION});

    $sth->finish;

    $self->{$attribute} = $value;

}

sub _delete_collection_info {

    my $self = shift;

    my $sql = qq(delete from collection where collection = ?);

    print qq(Deleting collection $self->{COLLECTION} from collection table\n)
	if $self->{PRINT_ACTIVITY};

    $self->{INDEX_DBH}->do($sql, undef, $self->{COLLECTION});

}

sub _store_collection_info {

    my $self = shift;

    print qq(Inserting collection $self->{COLLECTION} into collection table\n)
	if $self->{PRINT_ACTIVITY};

    my $sql = qq(
		 insert into collection
		 (collection, max_indexed_id, document_table,
		  document_id_field, document_fields)
		 values
		 (?, ?, ?, ?, ?)
		 );

    $self->{INDEX_DBH}->do($sql, undef,
			   $self->{COLLECTION},
			   $self->{MAX_INDEXED_ID},
			   $self->{DOCUMENT_TABLE},
			   $self->{DOCUMENT_ID_FIELD},
			   join ',', @{$self->{DOCUMENT_FIELDS}},
			   );

}


sub _fetch_collection_info {

    my $self = shift;

    return 0 unless $self->{COLLECTION};

    return 0 unless $self->_collection_table_exists;

    my $fetch_status = 0;

    my $sql = qq(select max_indexed_id, document_table, document_id_field, document_fields
		 from collection where collection = ?);

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($self->{COLLECTION});

    $fetch_status = 1 if $sth->rows;

    my $document_fields;

    $sth->bind_columns(
		       undef,
		       \$self->{MAX_INDEXED_ID},
		       \$self->{DOCUMENT_TABLE},
		       \$self->{DOCUMENT_ID_FIELD},
		       \$document_fields,
		       );

    $sth->fetch;

    $sth->finish;

    my @document_fields = split /,/, $document_fields;

    $self->{DOCUMENT_FIELDS} = \@document_fields;
    return $fetch_status;

}

sub _phrase_search {

    my $self = shift;

    my @result_documents = $self->{RESULT_VECTOR}->Index_List_Read;

    return if $#result_documents < 0;

    return if $#result_documents > $PHRASE_THRESHOLD;

    my ($sql, $sth);

    my $result_documents = join ',', @result_documents;

    my $vec_size = $self->{MAX_INDEXED_ID} + 1;

    my $phrase_vector;
    my $i = 0;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
	foreach my $phrase ( @{$self->{QUERY_PHRASES}->[$fno]} ) {
	    $sql = qq(
		      select $self->{DOCUMENT_ID_FIELD}
		      from $self->{DOCUMENT_TABLE}
		      where $self->{DOCUMENT_ID_FIELD} in ($result_documents)
		      and $self->{DOCUMENT_FIELDS}->[$fno] like 
		      ?
		      );

	    $sth = $self->{DOCUMENT_DBH}->prepare($sql);
	    $sth->execute("%$phrase%");
	    my $document_id;
	    $sth->bind_col(1, \$document_id);
	    my @phrase_result;
	    while ($sth->fetch) {
		push @phrase_result, $document_id;
	    }
	    $sth->finish;
	    
	    my $vector = Bit::Vector->new($vec_size);
	    $vector->Index_List_Store(@phrase_result);
	    if ($i == 0) {
		$phrase_vector = $vector;
	    } else {
		$phrase_vector->Union($phrase_vector, $vector);
	    }
	    $i++;
	}
    }
    return if $i < 1;
    $self->{RESULT_VECTOR}->Intersection($self->{RESULT_VECTOR},
					 $phrase_vector);
}

sub _vector_search {

    my $self = shift;

    foreach my $field_no ( @{$self->{QUERY_FIELD_NOS}} ) {

	foreach my $word ( @{$self->{QUERY_WORDS}->[$field_no]} ) {

	    $self->{VECTOR}->[$field_no]->{$word} = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);

	    $self->{VECTOR}->[$field_no]->{$word}->from_Enum( $self->_fetch_documents_vector($field_no, $word) );


	}

    }

}

sub _fetch_maxtf {

    my $self = shift;

    my $use_all_fields = shift;

    my $field_nos;
    if ($use_all_fields) {
	$field_nos = join ',', (0 .. $#{$self->{DOCUMENT_FIELDS}});
    } else {
	$field_nos = join ',', @{$self->{QUERY_FIELD_NOS}};
    }
    
    my $sql = qq(select field_no, maxtf from $self->{MAXTF_TABLE} where field_no in ($field_nos));

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute || warn $DBI::errstr;

    while (my $row = $sth->fetchrow_arrayref) {
	$self->{MAXTF}->[$row->[0]] = [(unpack 'w*', $row->[1])];
    }

    $sth->finish;

}

sub _search {

    my $self = shift;

    my @result_documents = $self->{RESULT_VECTOR}->Index_List_Read;

    return $ERROR{'no_results'} unless $#result_documents >= 0;

    my %score;

    if ($self->{OR_WORD_COUNT} == 1 && $self->{AND_WORD_COUNT} == 0
	&& $#result_documents > $RESULT_THRESHOLD) {
	my $field_no = $self->{QUERY_FIELD_NOS}->[0];
	my $word = $self->{QUERY_OR_WORDS}->[$field_no]->[0];

	my $occurence = $self->_occurence($field_no, $word);
		
	my $idf;
	if ($occurence) {
	    $idf = log($self->{MAX_INDEXED_ID}/$occurence);
	} else {
	    $idf = 0;
	}

	return $ERROR{'no_results'} if $idf < $IDF_THRESHOLD;

	my %raw_score = $self->_documents($field_no, $word);
	
	if ($self->{VALID_MASK}) {
	    foreach my $document_id (@result_documents) {
		$score{$document_id} = $raw_score{$document_id};
	    }
	    return \%score;
	} else {
	    return \%raw_score;
	}

    } else {
	$self->_fetch_maxtf;
	foreach my $field_no ( @{$self->{QUERY_FIELD_NOS}} ) {
	  WORD:
	    foreach my $word (@{$self->{QUERY_OR_WORDS}->[$field_no]},
			      @{$self->{QUERY_AND_WORDS}->[$field_no]} ) {

		my $occurence = $self->_occurence($field_no, $word);
		
		my $idf;
		if ($occurence) {
		    $idf = log($self->{MAX_INDEXED_ID}/$occurence);
		} else {
		    $idf = 0;
		}

		next WORD if $idf < $IDF_THRESHOLD;

		my %word_score = $self->_documents($field_no, $word);

	      DOCUMENT_ID:

		foreach my $document_id (@result_documents) {
		    next DOCUMENT_ID unless defined $word_score{$document_id};
		    my $maxtf = $self->{MAXTF}->[$field_no]->[$document_id];
		    if ($score{$document_id}) {
			$score{$document_id} *=
			    (1 + (($word_score{$document_id}/sqrt($maxtf)) * $idf));
		    } else {
			$score{$document_id} = (1 + (($word_score{$document_id}/sqrt($maxtf)) * $idf));
		    }
		}
	    }
	}
	return $ERROR{'no_results'} unless scalar keys %score;
	return \%score;

    }

}

sub _occurence {

    my $self = shift;
    my $field_no = shift;
    my $word = shift;

    my ($sql, $sth);

    $sql = qq(
	      select occurence from $self->{INVERTED_TABLES}->[$field_no]
	      where word = ?
	      );

    $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($word);

    my $occurence;

    $sth->bind_col(1, \$occurence);

    $sth->fetch;

    $sth->finish;		 

    return $occurence;

}

sub _boolean_compare {

    my $self = shift;

    my $vec_size = $self->{MAX_INDEXED_ID} + 1;

    my @vector;
    my $i = 0;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {

	my $or_word_count = 0; my $and_word_count = 0;

	$or_word_count = $#{ $self->{QUERY_OR_WORDS}->[$fno] } + 1;
	$and_word_count = $#{ $self->{QUERY_AND_WORDS}->[$fno] } + 1;

	$self->{OR_WORD_COUNT} += $or_word_count;
	$self->{AND_WORD_COUNT} += $and_word_count;

	$vector[$i] = Bit::Vector->new($vec_size);

	if ($or_word_count < 1) {
	    $vector[$i]->Fill;
	} else {
	    $vector[$i]->Empty;
	}

	foreach my $word ( @{$self->{QUERY_OR_WORDS}->[$fno]} ) {
	    $vector[$i]->Union($vector[$i],
				 $self->{VECTOR}->[$fno]->{$word});

	}

	foreach my $word ( @{$self->{QUERY_AND_WORDS}->[$fno]} ) {
	    $vector[$i]->Intersection($vector[$i],
					$self->{VECTOR}->[$fno]->{$word});

	}

	foreach my $word ( @{$self->{QUERY_NOT_WORDS}->[$fno]} ) {
	    $self->{VECTOR}->[$fno]->{$word}->Flip;
	    $vector[$i]->Intersection($vector[$i],
					$self->{VECTOR}->[$fno]->{$word});

	}
	$i++;
    }

    $self->{RESULT_VECTOR} = $vector[0];

    if ($#vector > 0) {
	foreach my $vector (@vector[1 .. $#vector]) {
	    $self->{RESULT_VECTOR}->Union($self->{RESULT_VECTOR}, $vector);
	}
    }
}

sub _apply_mask {

    my $self = shift;

    return unless $self->{MASK};

    if ($self->_fetch_mask) {
	$self->{VALID_MASK} = 1;
    }
    if ($self->{MASK}->{and_mask}) {
	foreach my $mask (@{$self->{MASK}->{and_mask}}) {
	    unless (ref $mask) {
		next unless ref $self->{MASK_VECTOR}->{$mask};
		$self->{RESULT_VECTOR}->Intersection(
		    $self->{RESULT_VECTOR}, $self->{MASK_VECTOR}->{$mask});
	    } else {
		my $vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);
		$vector->Index_List_Store(@$mask);
		$self->{RESULT_VECTOR}->Intersection(
		    $self->{RESULT_VECTOR}, $vector);
	    }
	}
    }
    if ($self->{MASK}->{not_mask}) {
	foreach my $mask (@{$self->{MASK}->{not_mask}}) {
	    unless (ref $mask) {
		next unless ref $self->{MASK_VECTOR}->{$mask};
		$self->{MASK_VECTOR}->{$mask}->Flip;
		$self->{RESULT_VECTOR}->Intersection(
		    $self->{RESULT_VECTOR}, $self->{MASK_VECTOR}->{$mask});
	    } else {
		my $vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);
		$vector->Index_List_Store(@$mask);
		$vector->Flip;
		$self->{RESULT_VECTOR}->Intersection(
		    $self->{RESULT_VECTOR}, $vector);
	    }
	}
    }
    if ($self->{MASK}->{or_mask}) {
	push @{$self->{MASK}->{or_mask_set}}, $self->{MASK}->{or_mask};
    }
    if ($self->{MASK}->{or_mask_set}) {
	foreach my $mask_set (@{$self->{MASK}->{or_mask_set}}) {
	    my $or_mask_count = 0;
	    my $union_vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);
	    foreach my $mask (@$mask_set) {
		unless (ref $mask) {
		    next unless ref $self->{MASK_VECTOR}->{$mask};
		    $or_mask_count++;
		    $union_vector->Union(
		        $union_vector, $self->{MASK_VECTOR}->{$mask});
		} else {
		    $or_mask_count++;
		    my $vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);
		    $vector->Index_List_Store(@$mask);
		    $union_vector->Union(
		        $union_vector, $self->{MASK_VECTOR}->{$mask});
		}
	    }
	    if ($or_mask_count) {
		$self->{RESULT_VECTOR}->Intersection(
		    $self->{RESULT_VECTOR}, $union_vector);
	    }  
	}
    }
}

sub _fetch_mask {

    my $self = shift;

    my $sql = qq(select documents_vector from $self->{MASK_TABLE} where mask = ?);

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    my $mask_count = 0;
    my $i = 0;

    foreach my $mask (@{$self->{MASK_FETCH_LIST}}) {

	$sth->execute($mask);

	next if $sth->rows < 1;
	$mask_count += $sth->rows;

	my $documents_vector;

	$sth->bind_col(1, \$documents_vector);

	$sth->fetch;

	$self->{MASK_VECTOR}->{$mask} = Bit::Vector->new_Enum(($self->{MAX_INDEXED_ID} + 1), $documents_vector);

	$i++;

    }

    $sth->finish;

    return $mask_count;


}



sub _parse_query {

    my $self = shift;

    my $error;

    foreach my $field_no ( @{$self->{QUERY_FIELD_NOS}} ) {

	my $query = $self->{QUERY}->[$field_no];

	return $ERROR{'empty_query'} unless $query;

	my $string_length = length($query);

	my $in_phrase = 0;
	my $phrase_count = 0;
	my $quote_count = 0;
	my $word_count = 0;
	my @raw_phrase = ();
	my $word = "";
	my (@phrase, @words, @and_words, @or_words, @not_words, @all_words);

	for my $position (0 .. ($string_length - 1)) {
	    my $char = substr($query, $position, 1);
	    if ($char eq '"') {
		$quote_count++
		}
	    $phrase_count = int(($quote_count - 1)/ 2);
	    if ($quote_count % 2 != 0 && $char ne '"') {
		$raw_phrase[$phrase_count] .= $char;
	    } else {
		 $word .= $char;
	    }
	}

	@words = split /\s+/, $word;
	@words = grep ! m/\"\"/, @words;

	foreach my $word (@words) {

	    if ($word =~ m/^\+(\w.*)/) {
		push @and_words, $1;
		push @all_words, $1;
	    } elsif ($word =~ m/^-(\w.*)/) {
		push @not_words, $1;
		push @all_words, $1;
	    } else {
		push @or_words, $word;
		push @all_words, $word;
	    }
	}

	foreach my $phrase (@raw_phrase) {
	    my @split_phrase = split/\s+/, $phrase;
	    $word_count = @split_phrase;
	    if ($word_count == 1) {
		push @or_words, $phrase;
		push @all_words, $phrase;
	    } elsif ($phrase =~ m/^\s*$/) {
		next;
	    } else {
		push @phrase, $phrase;
		push @and_words, @split_phrase;
		push @all_words, @split_phrase;
	    }
	}

	if ($quote_count % 2 != 0) {
	    $error = $ERROR{'$quote_count'};
	}

	$self->{QUERY_PHRASES}->[$field_no] = \@phrase;
	$self->{QUERY_OR_WORDS}->[$field_no] = \@or_words;
	$self->{QUERY_AND_WORDS}->[$field_no] = \@and_words;
	$self->{QUERY_NOT_WORDS}->[$field_no] = \@not_words;
	$self->{QUERY_WORDS}->[$field_no] = \@all_words;
	$self->{HIGHLIGHT} = join '|', @all_words;

    }

    return $error;

}

sub _documents {

    my $self = shift;

    my $field_no = shift;
    my $word = shift;

    local $^W = 0; # turn off silly uninitialized value warning
    if (@_) {
	my ($id, $frequency) = @_;
	$self->{DOCUMENTS}->[$field_no]->{$word} .= pack 'ww', ($id, $frequency);
	$self->{OCCURENCE}->[$field_no]->{$word}++; 
    } else {
	unpack 'w*', $self->_fetch_documents($field_no, $word);
    }

}

sub _fetch_documents {

    my $self = shift;
    my $field_no = shift;
    my $word = shift;

    my ($sql, $sth);

    $sql = qq(
	      select documents from $self->{INVERTED_TABLES}->[$field_no]
	      where word = ?
	      );

    $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($word);

    my $documents;

    $sth->bind_col(1, \$documents);

    $sth->fetch;

    $sth->finish;

    return $documents;

}

sub _fetch_documents_vector {

    my $self = shift;
    my $field_no = shift;
    my $word = shift;

    my ($sql, $sth);

    $sql = qq(
	      select documents_vector from $self->{INVERTED_TABLES}->[$field_no]
	      where word = ?
	      );

    $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($word);

    my $documents_vector;

    $sth->bind_col(1, \$documents_vector);

    $sth->fetch;

    $sth->finish;

    return $documents_vector;

}

sub _commit_documents {

    my $self = shift;

    print "Storing max term frequency for each document\n"
	if $self->{PRINT_ACTIVITY};

    my $use_all_fields = 1;
    $self->_fetch_maxtf($use_all_fields);

    my $sql = qq(replace into $self->{MAXTF_TABLE} (field_no, maxtf) values (?, ?));

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {
	my @maxtf;
	if ($#{$self->{MAXTF}->[$field_no]} >= 0) {
	    @maxtf = @{$self->{MAXTF}->[$field_no]};
	    @maxtf[($self->{OLD_MAX_INDEXED_ID} + 1) .. $self->{MAX_INDEXED_ID}] = @{$self->{NEW_MAXTF}->[$field_no]}[($self->{OLD_MAX_INDEXED_ID} + 1) .. $self->{MAX_INDEXED_ID}];
	} else {
	    @maxtf = @{$self->{NEW_MAXTF}->[$field_no]};
	}
	$maxtf[0] = 0 unless defined $maxtf[0];
	my $packed_maxtf = pack 'w' x ($#maxtf + 1), @maxtf;
	$sth->execute($field_no, $packed_maxtf);
    }

    $sth->finish;

    print "Committing inverted tables to database\n"
	if $self->{PRINT_ACTIVITY};

    foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {

	my ($sql, $i_sth);

	$sql = qq(
		  replace into $self->{INVERTED_TABLES}->[$field_no]
		  (word, occurence, documents_vector, documents) values (?, ?, ?, ?)
		  );

	$i_sth = $self->{INDEX_DBH}->prepare($sql);

	print("field$field_no ", scalar keys %{$self->{DOCUMENTS}->[$field_no]},
	       " distinct words\n") if $self->{PRINT_ACTIVITY};

	while (my ($word, $documents) = each %{$self->{DOCUMENTS}->[$field_no]}) {

	    print "$word\n" if $self->{PRINT_ACTIVITY} >= 2;

	    my $sql = qq(select occurence, documents_vector, documents
			 from $self->{INVERTED_TABLES}->[$field_no]
			 where word = ?);

	    my $s_sth = $self->{INDEX_DBH}->prepare($sql);

	    $s_sth->execute($word);

	    my $o_occurence = 0;
	    my $o_documents_vector = '';
	    my $o_documents = '';

	    $s_sth->bind_columns(undef, \$o_occurence, \$o_documents_vector, \$o_documents);

	    $s_sth->fetch;
	    $s_sth->finish;

	    my %frequencies = unpack 'w*', $documents;

	    my $o_vector = Bit::Vector->new_Enum(($self->{MAX_INDEXED_ID}+1), $o_documents_vector);

	    my $vector = Bit::Vector->new($self->{MAX_INDEXED_ID} + 1);

	    $vector->Index_List_Store(keys %frequencies);

	    $vector->Union($o_vector, $vector);

	    local $^W = 0;
	    $i_sth->execute($word,
			    ($self->{OCCURENCE}->[$field_no]->{$word} + $o_occurence),
			    $vector->to_Enum,
			    ($o_documents . $documents)) or warn $self->{INDEX_DBH}->err;

	}

	$i_sth->finish;

    }
}


sub _fetch_document {

    my $self = shift;

    my $id = shift;

    my $field = shift;

    my ($sql, $sth);

    $sql = qq(
	      select $field from $self->{DOCUMENT_TABLE}
	      where $self->{DOCUMENT_ID_FIELD} = ?
	      );

    $sth = $self->{DOCUMENT_DBH}->prepare($sql);

    my $document;

    $sth->execute($id);

    $sth->bind_col(1, \$document);

    $sth->fetch;

    $sth->finish;

    return $document;
	
}

sub _words {

    my $self = shift;

    my $document = shift;

    # kill tags
    $document =~ s/<.*?>//g;

    # split words on whitespace, hyphen (-),  slash (/) or ellipsis (...)
    local $^W = 0;
    my @word = (split /(?:\s|-|\/|\.\.\.)+/, $document);


    # Make words all lower case, kill possesive ending, clean up accented
    # characters.  This needs work.
    for (@word) {
        s/\xe8/e/g;    #e`
        s/\xe9/e/g;    #e'
        s/\xf1/n/g;    #n~
	s/\xe1/a/g;    #a'
        s/^[^A-Za-z0-9]+//;
        s/[^A-Za-z0-9]+$//;
        tr/[A-Z]/[a-z]/;
        s/\'s\s*$//;
    }

    return @word;


}

sub _ping_document {

    my $self = shift;

    my $id = shift;

    my ($sql, $sth);

    $sql = qq(
	      select 1 from
	      $self->{DOCUMENT_TABLE}
	      where $self->{DOCUMENT_ID_FIELD} = ?
	      );

    $sth = $self->{DOCUMENT_DBH}->prepare($sql);

    $sth->execute($id);

    return $sth->rows;

}

sub _create_tables {

    my $self = shift;

    my ($sql, $sth);

    $sql = qq(drop table if exists $self->{MASK_TABLE});
    print "Dropping mask table ($self->{MASK_TABLE})\n"
	if $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do($sql);

    $sql = qq(
	      create table
	      $self->{MASK_TABLE} (
				   mask varchar(100) not null,
				   documents_vector mediumblob not null,
				   primary key mask_key (mask)
				   )
	      );
    print "Creating mask table ($self->{MASK_TABLE})\n"
	if  $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do($sql);


    $sql = qq(drop table if exists $self->{MAXTF_TABLE});
    print "Dropping max term frequency table ($self->{MAXTF_TABLE})\n"
	if $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do($sql);

    $sql = qq(
	      create table
	      $self->{MAXTF_TABLE} (
				    field_no smallint unsigned not null,
				    maxtf mediumblob not null,
				    primary key field_no_key (field_no)
				    )
	      );
    print "Creating max term frequency table ($self->{MAXTF_TABLE})\n"
	if  $self->{PRINT_ACTIVITY};
    $self->{INDEX_DBH}->do($sql);


    foreach my $table ( @{$self->{INVERTED_TABLES}} ) {

	$sql = qq(drop table if exists $table);

	print "Dropping inverted table ($table)\n" if $self->{PRINT_ACTIVITY};

	$self->{INDEX_DBH}->do($sql);

	$sql = qq(
		  create table $table (
				       word varchar($MAX_WORD) not null,
				       occurence int unsigned not null,
				       documents_vector mediumblob not null,
				       documents mediumblob not null,
				       primary key word_key (word)
				       )
		  );

	print "Creating inverted table ($table)\n" if $self->{PRINT_ACTIVITY};

	$self->{INDEX_DBH}->do($sql);

    }

}


1;
__END__


=head1 NAME

DBIx::TextIndex - Perl extension for full-text searching in SQL databases

=head1 SYNOPSIS

use DBIx::TextIndex;

my $index = DBIx::TextIndex->new({
    document_dbh => $document_dbh,
    document_table => 'document_table',
    document_fields => ['column_1', 'column_2'],
    document_id_field => 'primary_key',
    index_dbh => $index_dbh,
    collection => 'collection_1',
});

$index->initialize;

$index->add_document(\@document_ids);

my $results = $index->search({
    column_1 => '"a phrase" +and -not or',
    column_2 => 'more words',
});

foreach my $document_id
    (sort {$$results{$b} <=> $$results{$a}} keys %$results ) 
{
    print "DocumentID: $document_id Score: $$results{$document_id} \n";  
}

$index->delete;

=head1 DESCRIPTION

DBIx::TextIndex was developed for doing full-text searches on BLOB columns
stored in a MySQL database.  Almost any database with BLOB and DBI support
should work with minor adjustments to SQL statements in the module.

Implements a crude parser for tokenizing a user input string into phrases,
can-include words, must-include words, and must-not-include words.

The following methods are available:

=head2 $index = DBIx::TextIndex->new(\%args)

Constructor method.  The first time an index is created, the following
arguments must be passed to new():

my $index = DBIx::TextIndex->new({
    document_dbh => $document_dbh,
    document_table => 'document_table',
    document_fields => ['column_1', 'column_2'],
    document_id_field => 'primary_key',
    index_dbh => $index_dbh,
    collection => 'collection_1',
});

=over 4

=item document_dbh

DBI connection handle to database containing text documents

=item document_table

Name of database table containing text documents

=item document_fields

Reference to a list of column names to be indexed from document_table

=item document_id_field

Name of a unique integer key column in document_table

=item index_dbh

DBI connection handle to database containing TextIndex tables.  I recommend
using a separate database for your TextIndex, because the module creates
and drops tables without warning.

=item collection

A name for the index.  Should contain only alpha-numeric characters or
underscores [A-Za-z0-9_]

=back

After creating a new TextIndex for the first time, and after calling
initialize(), only the index_dbh, document_dbh, and collection arguments 
are needed to create subsequent instances of a TextIndex.

=head2 $index->initialize

This method creates all the inverted tables for the TextIndex in the
database specified by document_dbh.  This method should be called only
once when creating a new index!  It drops all the inverted tables
before creating new ones.

initialize() also stores the document_table, document_fields, and
document_id_field attributes in a special table called "collection,"
so subsequent calls to new() for a given collection do not need
those arguments.

=head2 $index->add_document(\@document_ids)

Add all the @documents_ids from document_id_field to the
TextIndex.  @document_ids must be sorted from lowest to highest.  All
further calls to add_document() must use @document_ids higher than
those previously added to the index.  Reindexing previously-indexed
documents will yield unpredictable results!

=head2 $index->search(\%search_args)

search() returns $results, a reference to a hash.  The keys of the
hash are document ids, and the values are the relative scores of the
documents.  If an error occured while searching, $results will be
a scalar variable containing an error message.

$results = $index->search({
    first_field => '+andword -notword orword "phrase words"',
    second_field => ...
    ...
});

if (ref $results) {
    print "The score for $document_id is $results->{$document_id}\n";
} else {
    print "Error: $results\n";
}

=head2 $index->delete

delete() removes the tables associated with a TextIndex from index_dbh.

=head1 CHANGES

0.04 Bug fix: add_document() will return if passed empty array ref instead
of producing error.

     Changed _boolean_compare() and _phrase_search() so and_words and
phrases behave better in multiple-field searches. Result set for each
field is calculated first, then union of all fields is taken for
final result set.

     Scores are scaled lower in _search().

0.03 Added example scripts in examples/.

0.02 Added or_mask_set.

0.01 Initial public release.  Should be considered beta, and methods may be
added or changed until the first stable release.

=head1 AUTHOR

Daniel Koch, dkoch@amcity.com

=head1 COPYRIGHT

Copyright 1997, 1998, 1999 by Daniel Koch.
All rights reserved.

=head1 LICENSE

This package is free software; you can redistribute it and/or modify it
under the same terms as Perl itself, i.e., under the terms of the "Artistic
License" or the "GNU General Public License".

=head1 DISCLAIMER

This package is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the "GNU General Public License" for more details.

=head1 ACKNOWLEDGEMENTS

Thanks to Ulrich Pfeifer for ideas and code from Man::Index module
in "Information Retrieval, and What pack 'w' Is For" article from
The Perl Journal vol. 2 no. 2.  

Thanks to Steffen Beyer for the Bit::Vector module, which
enables fast set operations in this module. Version 5.3 or greater of
Bit::Vector is required by DBIx::TextIndex.

=head1 BUGS

Uses quite a bit of memory.

MySQL-specific SQL is used.

Parser is not very good.

Documentation is not complete.

Phrase searching relies on full-table scan.  Any suggestions for adding
word-proximity information to the index would be much appreciated.

No facility for deleting documents from an index.  Work-around: create
a new index. 

Please feel free to email me (dkoch@amcity.com) with any questions
or suggestions.

=head1 SEE ALSO

perl(1).

=cut



