package DBIx::TextIndex;

use strict;

use Bit::Vector ();
use Carp qw(carp croak);
use Data::Dumper qw(Dumper);

my $ME = "DBIx::TextIndex";

$DBIx::TextIndex::VERSION = '0.08';

# Version number when collection table definition last changed
my $LAST_COLLECTION_TABLE_UPGRADE = '0.07';

# Largest size word to be indexed
my $MAX_WORD_LENGTH = 12;

# Minimum size of word base before a "%" wildcard
my $MIN_WILDCARD_LENGTH = 5;

# Used to screen stop words from the scoring process
my $IDF_THRESHOLD = 0.2;

# What can be considered too many results
my $RESULT_THRESHOLD = 5000;

# Practical number of rows RDBMS can scan in acceptable amount of time
my $PHRASE_THRESHOLD = 1000;

my %ERROR = (
    empty_query     => "You must be searching for something!",
    quote_count     => "Quotes must be used in matching pairs.",
    no_results      => "Your search did not produce any matching documents.",
    no_results_stop => "Your search did not produce any matching " .
        "documents. These common words were not included in the search:",
	     );

my $COLLECTION_TABLE = 'collection';

my @MASK_TYPE = qw(and_mask or_mask not_mask);

my $DB_DEFAULT = 'mysql';

my $LANGUAGE_DEFAULT = 'en';

my @COLLECTION_FIELDS = qw(
    collection
    version
    max_indexed_id
    document_table
    document_id_field
    document_fields
    language
    stoplist
    proximity_index
    error_quote_count
    error_empty_query
    error_no_results
    error_no_results_stop
    max_word_length
    result_threshold
    phrase_threshold
    min_wildcard_length
);

my %COLLECTION_FIELD_DEFAULT = (
    collection => '',
    version => $DBIx::TextIndex::VERSION,
    max_indexed_id => '0',
    document_table => '',
    document_id_field => '',
    document_fields => '',
    language => $LANGUAGE_DEFAULT,
    stoplist => '',
    proximity_index => '0',
    error_quote_count => $ERROR{quote_count},
    error_empty_query => $ERROR{empty_query},
    error_no_results => $ERROR{no_results},
    error_no_results_stop => $ERROR{no_results_stop},
    max_word_length => $MAX_WORD_LENGTH,
    result_threshold => $RESULT_THRESHOLD,
    phrase_threshold => $PHRASE_THRESHOLD,
    min_wildcard_length => $MIN_WILDCARD_LENGTH,
);


my $PA = 0;		# just a shortcut to $self->{PRINT_ACTIVITY}

sub new {
    my $pkg = shift;
    my $args = shift;

    my $class = ref($pkg) || $pkg;
    my $self = bless {}, $class;

    $self->{COLLECTION_TABLE} = $COLLECTION_TABLE;
    $self->{COLLECTION_FIELDS} = \@COLLECTION_FIELDS;

    foreach my $arg ('collection', 'index_dbh', 'document_dbh') {
	if ($args->{$arg}) {
	    $self->{uc $arg} = $args->{$arg};
	}
	else {
	    croak "new $pkg needs $arg argument";
	}
    }

    $self->{PRINT_ACTIVITY} = 0;
    $self->{PRINT_ACTIVITY} = $args->{'print_activity'};
    $PA = $self->{PRINT_ACTIVITY};

    $args->{db} = $args->{db} ? $args->{db} : $DB_DEFAULT;
    my $db = 'DBIx/TextIndex/' . $args->{db} . '.pm';
    require "$db";

    unless ($self->_fetch_collection_info) {
	$self->{DOCUMENT_TABLE} = $args->{document_table};
	$self->{DOCUMENT_FIELDS} = $args->{document_fields};
	$self->{DOCUMENT_ID_FIELD} = $args->{document_id_field};
	$self->{LANGUAGE} = $args->{language} || $LANGUAGE_DEFAULT;
    	$self->{STOPLIST} = $args->{stoplist};
    	$self->{PINDEX} = $args->{proximity_index} || 0;
	# overiding default error messages
	while (my($error, $msg) = each %{$args->{errors}}) {
	    $ERROR{$error} = $msg;
	}
	$self->{MAX_WORD_LENGTH} = $args->{max_word_length}
	    || $MAX_WORD_LENGTH;
	$self->{RESULT_THRESHOLD} = $args->{result_threshold}
	    || $RESULT_THRESHOLD;
	$self->{PHRASE_THRESHOLD} = $args->{phrase_threshold}
	    || $PHRASE_THRESHOLD;
	$self->{MIN_WILDCARD_LENGTH} = $args->{min_wildcard_length}
	    || $MIN_WILDCARD_LENGTH;
    }
    $self->{CZECH_LANGUAGE} = $self->{LANGUAGE} eq 'cz' ? 1 : 0;
    $self->{MAXTF_TABLE} = $self->{COLLECTION} . '_maxtf';
    $self->{MASK_TABLE} = $self->{COLLECTION} . '_mask';

    my $field_no = 0;
    foreach my $field ( @{$self->{DOCUMENT_FIELDS}} ) {
	$self->{FIELD_NO}->{$field} = $field_no;
	push @{$self->{INVERTED_TABLES}},
	    ($self->{COLLECTION} . '_' . $field . '_inverted');
	if ($self->{PINDEX}) {
	    push @{$self->{PINDEX_TABLES}},
		($self->{COLLECTION} . '_' . $field . '_pindex');
	}	
    	$field_no++;
    }

    # Initialize stoplists
    if ($self->{STOPLIST} and ref($self->{STOPLIST})) {
	$self->{STOPLISTED_WORDS} = {};
    	foreach my $stoplist (@{$self->{STOPLIST}}) {
	    my $stopfile = 'DBIx/TextIndex/stop-' . $stoplist . '.pm';
	    print "initializing stoplist: $stopfile\n" if $PA;
	    require "$stopfile";
            foreach my $word (@DBIx::TextIndex::stop::words) {
            	$self->{STOPLISTED_WORDS}->{$word} = 1;
            }
        }
        $self->{STOPLISTED_QUERY} = [];
    }
    
    # Initialize Czech language support
    require CzFast if $self->{CZECH_LANGUAGE};

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

    print "Adding mask ($mask) to table $self->{MASK_TABLE}\n" if $PA > 1;
    $self->{INDEX_DBH}->do($self->db_add_mask, undef, $mask, $vector->to_Enum);
    return 1;
}

sub delete_mask {
    my $self = shift;
    my $mask = shift;
    print "Deleting mask ($mask) from table $self->{MASK_TABLE}\n" if $PA > 1;
    $self->{INDEX_DBH}->do($self->db_delete_mask, undef, $mask);
}

sub add_document {
    my $self = shift;
    my $ids = shift;
    return if $#$ids < 0;

    if ($PA) {
	my $add_count = $#{$ids} + 1;
	print "Adding $add_count documents\n";
    }

    $self->{OLD_MAX_INDEXED_ID} = $self->max_indexed_id;
    $self->max_indexed_id($ids->[-1]);
    foreach my $document_id (@$ids) {
	print $document_id if $PA;
	next unless $self->_ping_document($document_id);
	
	foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {
	    print " field$field_no" if $PA;

	    my %frequency;
	    my $maxtf = 0;
	    my $word_count = 0;
            my $table = $self->{PINDEX_TABLES}->[$field_no];
	    my @words = $self->_words($self->_fetch_document($document_id,
				      $self->{DOCUMENT_FIELDS}->[$field_no]));

	    foreach my $word (@words) {
		$frequency{$word}++;
		$maxtf = $frequency{$word} if $frequency{$word} > $maxtf;
                if ($self->{PINDEX}) {
		    my $sql = $self->db_pindex_add($table);
		    $self->{INDEX_DBH}->do($sql, undef,
					   $word, $document_id, $word_count);
		    print "pindex: adding $document_id, word: $word, pos: $word_count\n"
			if $PA > 1;
		}
		$word_count++;
	    }
	    print " $word_count" if $PA;
	    
	    while (my ($word, $frequency) = each %frequency) {
		$self->_documents($field_no, $word, $document_id, $frequency);
	    }
	    $self->{NEW_MAXTF}->[$field_no]->[$document_id] = $maxtf;
	}	# end of field indexing

	print "\n" if $PA;
    }	# end of document indexing

    $self->_commit_documents;
}

sub remove_document {
    my $self = shift;
    my $ids = shift;
    return if $#$ids < 0;

    if ($PA) {
	my $remove_count = $#{$ids} + 1;
	print "Removing $remove_count documents\n";
    }

    my $total_words = 0;
    my @remove_words;
    foreach my $document_id (@$ids) {
	print $document_id if $PA;
	croak "$ME: document's content must be accessible to remove a document"
	    unless $self->_ping_document($document_id);
	foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {
	    print " field$field_no" if $PA;
	    my @words = $self->_words($self->_fetch_document($document_id,
				      $self->{DOCUMENT_FIELDS}->[$field_no]));
	    my %words;
	    foreach my $word (@words) {
		$remove_words[$field_no]->{$word}++ if (not $words{$word});
		$words{$word} = 1;
		$total_words++;
	    }
	}	# end of each field
    }	# end of each document

    if ($self->{PINDEX}) {
	print "Removing documents from proximity index\n" if $PA;
	$self->_pindex_remove($ids);
    }

    print "Removing documents from max term frequency table\n" if $PA;
    $self->_maxtf_remove($ids);

    print "Removing documents from inverted tables\n" if $PA;
    $self->_inverted_remove($ids, \@remove_words);

    return $total_words; 	# return count of removed words
}

sub _pindex_remove {
    my $self = shift;
    my $documents_ref = shift;

    my $documents = join(', ', @{$documents_ref});
    foreach my $table (@{$self->{PINDEX_TABLES}}) {
	my $sql = $self->db_pindex_remove($table, $documents);
        print "pindex_remove: removing documents: $documents\n" if $PA;
        $self->{INDEX_DBH}->do($sql);
    }
}

sub _maxtf_remove {
    my $self = shift;
    my $documents_ref = shift;
    
    my @documents = @{$documents_ref};
    my $use_all_fields = 1;
    $self->_fetch_maxtf($use_all_fields);
    
    my $sql = $self->db_update_maxtf;
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {
	my @maxtf = @{$self->{MAXTF}->[$field_no]};
	foreach my $doc_id (@documents) {
	    $maxtf[$doc_id] = 0;
	}
	my $packed_maxtf = pack 'w' x ($#maxtf + 1), @maxtf;
	$sth->execute($field_no, $packed_maxtf);
    }
}

sub _inverted_remove {
    my $self = shift;
    my $documents_ref = shift;
    my $words = shift;
	
    my @documents = @{$documents_ref};
    foreach my $fno (0..$#{$self->{DOCUMENT_FIELDS}}) {
	my $field = $self->{DOCUMENT_FIELDS}->[$fno];
	my $table = $self->{INVERTED_TABLES}->[$fno];
	my $sql;
	
	$sql = $self->db_inverted_replace($table);
	my $sth_replace = $self->{INDEX_DBH}->prepare($sql);

	$sql = $self->db_inverted_select($table);
	my $sth_select = $self->{INDEX_DBH}->prepare($sql);
	
	foreach my $word (keys %{$words->[$fno]}) {
            $sth_select->execute($word);
	    my($o_occurence, $o_documents_vector, $o_documents);
	    $sth_select->bind_columns(\$o_occurence, \$o_documents_vector,
				      \$o_documents);
	    $sth_select->fetch;
	    
	    print "inverted_remove: field: $field: word: $word\n" if $PA;
	    # @documents_all contains a document id for each word that
	    # we are removing
	    my $occurence = $o_occurence - $words->[$fno]->{$word};
	    print "inverted_remove: old occurence: $o_occurence\n" if $PA;
	    print "inverted_remove: new occurence: $occurence\n" if $PA;
	    
	    # if new occurence is zero, then we should remove the record
            # of this word completely
	    if ($occurence < 1) {
		my $sql = $self->db_inverted_remove($table);
                $self->{INDEX_DBH}->do($sql, undef, $word);
                print qq(inverted_remove: removing "$word" completely\n)
		    if $PA;
            	next;
            }

	    # now comes the BIG PROBLEM - we cannot modify the documents
	    # vector by shrinking it only in rows that correspond to
	    # words found in that document, because that would break
	    # the matching process
	    #
	    # obviously we also cannot loop over all those thousands
	    # of rows
	    #
	    # so we just set to zero all the bits that correspond to words
	    # found in this document
	    
	    my $vec = Bit::Vector->new_Enum($self->max_indexed_id + 1,
					    $o_documents_vector);
	    $vec->Index_List_Remove(@documents);
	    if ($PA) {
		local $, = ',';
                print "inverted_remove: removing documents: ";
                print @documents;
                print "\n";
            }
	    # now we will remove the document from the "documents" field
	    my %new_documents = unpack 'w*', $o_documents;
	    foreach my $doc_id (@documents) {
		delete $new_documents{$doc_id};
	    }
	    my $new_documents;
	    while ( my($document, $frequency) = each %new_documents) {
		$new_documents .= pack 'ww', ( $document, $frequency );
	    }
	    
	    $sth_replace->execute($word, $occurence, $vec->to_Enum,
				  $new_documents);
	}
    }
}

sub stat {
    my $self = shift;
    my $query = shift;

    if (lc($query) eq 'total_words') {
	my $total_words = 0;
	foreach my $table (@{$self->{INVERTED_TABLES}}) {
	    my $sql = $self->db_total_words($table);
	    $total_words += scalar $self->{INDEX_DBH}->selectrow_array($sql);
	}
	return $total_words;
    }

    return undef;
}

sub unscored_search {
    my $self = shift;
    my $query = shift;
    my $args = shift;
    $args->{unscored_search} = 1;
    return $self->search($query, $args);
}

sub search {

    my $self = shift;
    my $query = shift;
    my $args = shift;

    return $ERROR{empty_query} unless $query;

    my @field_nos;
    foreach my $field (keys %$query) {
	croak "$ME: invalid field ($field) in search()"
	    unless exists $self->{FIELD_NO}->{$field};
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

    if ($args->{unscored_search}) {
	my @result_documents = $self->{RESULT_VECTOR}->Index_List_Read;
	return $ERROR{'no_results'} if $#result_documents < 0;
	return \@result_documents;
    }

    my $results = $self->_search;
    return $results;
}

sub highlight {
    return $_[0]->{HIGHLIGHT};
}

sub html_highlight {
    my $self = shift;
    my $field = shift;
    
    my $fno = $self->{FIELD_NO}->{$field};

    my @words = @{$self->{QUERY_HIGHLIGHT}->[$fno]};
    push (@words, @{$self->{QUERY_PHRASES}->[$fno]});

    return (\@words, $self->{QUERY_WILDCARDS}->[$fno]);
}

sub initialize {
    my $self = shift;

    $self->{MAX_INDEXED_ID} = 0;

    if ($self->_collection_table_exists) {
	if ($self->_collection_table_upgrade_required ||
	    $self->collection_count < 1)
	{
	    $self->upgrade_collection_table;
	}
    } else {
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

    print "Deleting $self->{COLLECTION} from collection table\n" if $PA;
    $self->_delete_collection_info;

    print "Dropping mask table ($self->{MASK_TABLE})\n" if $PA;
    $self->db_drop_table($self->{MASK_TABLE});
    
    print "Dropping max term frequency table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->db_drop_table($self->{MAXTF_TABLE});

    foreach my $table ( @{$self->{INVERTED_TABLES}} ) {
	print "Dropping inverted table ($table)\n" if $PA;
	$self->db_drop_table($table);
    }

    if ($self->{PINDEX}) {
	foreach my $table ( @{$self->{PINDEX_TABLES}} ) {
	    print "Dropping proximity table ($table)\n"	if $PA;
	    $self->db_drop_table($table);
    	}    
    }

}

sub _collection_table_exists {
    my $self = shift;

    return $self->db_table_exists($COLLECTION_TABLE);
}

sub _create_collection_table {
    my $self = shift;
    my $sql = $self->db_create_collection_table;
    $self->{INDEX_DBH}->do($sql);
    print "Creating collection table ($COLLECTION_TABLE)\n" if $PA;
}

sub collection_count {
    my $self = shift;
    my $collection_count = $self->{INDEX_DBH}->selectrow_array(
					         $self->db_collection_count);
    croak $DBI::errstr if $DBI::errstr;
    return $collection_count;
}

sub _collection_table_upgrade_required {
    my $self = shift;
    my $version = 0;
    print "Checking if collection table upgrade required ...\n" if $PA > 1;
    unless ($self->collection_count) {
	print "... Collection table contains no rows\n" if $PA > 1;
	return 0;	
    }
    eval {
	local $SIG{__DIE__};
	local $SIG{__WARN__} = sub { die $_[0] };
	$version = $self->{INDEX_DBH}->selectrow_array(
			                 $self->db_fetch_collection_version);
	die $DBI::errstr if $DBI::errstr;
    };
    if ($@) {
	print "... Problem fetching version column, must upgrade\n" if $PA > 1;
	return 1;
    }
    if ($version && ($version < $LAST_COLLECTION_TABLE_UPGRADE)) {
	print "... Collection table version too low, must upgrade\n"
	    if $PA > 1;
	return 1;
    }
    print "... Collection table up-to-date\n" if $PA > 1;	
    return 0;
}

sub upgrade_collection_table {
    my $self = shift;
    my $sth = $self->{INDEX_DBH}->prepare($self->db_fetch_all_collection_rows);
    $sth->execute;
    croak $sth->errstr if $sth->errstr;
    if ($sth->rows < 1) {
	print "No rows in collection table, dropping collection table ($self->{COLLECTION_TABLE})\n" if $PA;
	$self->db_drop_table($self->{COLLECTION_TABLE});
	$self->_create_collection_table;
	return 1;
    } 
    my @table;
    while (my $row = $sth->fetchrow_hashref) {
	push @table, $row;
    }

    print "Upgrading collection table ...\n" if $PA;
    print "... Dropping old collection table ...\n" if $PA;
    $self->db_drop_table($self->{COLLECTION_TABLE});
    print "... Recreating collection table ...\n" if $PA;
    $self->_create_collection_table;

    foreach my $old_row (@table) {
	my %new_row;
	foreach my $field (@COLLECTION_FIELDS) {
	    $new_row{$field} = exists $old_row->{$field} ?
		$old_row->{$field} : $COLLECTION_FIELD_DEFAULT{$field};
	}
	# 'czech_language' option replaced with generic 'language'
	if (exists $old_row->{czech_language}) {
	    $new_row{language} = 'cz' if $old_row->{czech_language};
	}
	print "... Inserting collection ($new_row{collection})\n" if $PA;
	$self->db_insert_collection_table_row(\%new_row)
    }
    return 1;
}

sub _update_collection_info {
    my $self = shift;
    my ($field, $value) = @_;

    my $attribute = $field;
    $attribute =~ tr/[a-z]/[A-Z]/;
    my $sql = $self->db_update_collection_info($field);
    $self->{INDEX_DBH}->do($sql, undef, $value, $self->{COLLECTION});
    $self->{$attribute} = $value;
}

sub _delete_collection_info {
    my $self = shift;

    my $sql = $self->db_delete_collection_info;
    $self->{INDEX_DBH}->do($sql, undef, $self->{COLLECTION});
    print "Deleting collection $self->{COLLECTION} from collection table\n"
	if $PA;
}

sub _store_collection_info {

    my $self = shift;

    print qq(Inserting collection $self->{COLLECTION} into collection table\n)
	if $PA;

    my $sql = $self->db_store_collection_info;
    my $document_fields = join (',', @{$self->{DOCUMENT_FIELDS}});
    my $stoplists = ref $self->{STOPLIST} ?
	join (',', @{$self->{STOPLIST}}) : '';

    $self->{INDEX_DBH}->do($sql, undef,

			   $self->{COLLECTION},
			   $DBIx::TextIndex::VERSION,
			   $self->{MAX_INDEXED_ID},
			   $self->{DOCUMENT_TABLE},
			   $self->{DOCUMENT_ID_FIELD},

			   $document_fields,
			   $self->{LANGUAGE},
			   $stoplists,
			   $self->{PINDEX},

			   $ERROR{quote_count},
			   $ERROR{empty_query},
			   $ERROR{no_results},
			   $ERROR{no_results_stop},

			   $self->{MAX_WORD_LENGTH},
			   $self->{RESULT_THRESHOLD},
			   $self->{PHRASE_THRESHOLD},
			   $self->{MIN_WILDCARD_LENGTH},
			   ) || croak $DBI::errstr;

}

sub _fetch_collection_info {

    my $self = shift;

    return 0 unless $self->{COLLECTION};

    return 0 unless $self->_collection_table_exists;

    if ($self->_collection_table_upgrade_required) {
	carp "$ME: Collection table must be upgraded, call \$index->upgrade_collection_table() or create a new() \$index and call \$index->initialize() to upgrade the collection table";
	return 0;
    }
    
    my $fetch_status = 0;

    my $sql = $self->db_fetch_collection_info;
    my $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute($self->{COLLECTION});

    $fetch_status = 1 if $sth->rows;

    my $document_fields;
    my $stoplists;

    my $null;
    $sth->bind_columns(
		       \$null,
		       \$self->{VERSION},
		       \$self->{MAX_INDEXED_ID},
		       \$self->{DOCUMENT_TABLE},
		       \$self->{DOCUMENT_ID_FIELD},

		       \$document_fields,
		       \$self->{LANGUAGE},
		       \$stoplists,
		       \$self->{PINDEX},

		       \$ERROR{empty_query},
		       \$ERROR{quote_count},
		       \$ERROR{no_results},
		       \$ERROR{no_results_stop},

		       \$self->{MAX_WORD_LENGTH},
		       \$self->{RESULT_THRESHOLD},
		       \$self->{PHRASE_THRESHOLD},
		       \$self->{MIN_WILDCARD_LENGTH},
		       );

    $sth->fetch;
    $sth->finish;

    my @document_fields = split /,/, $document_fields;
    my @stoplists = split (/,\s*/, $stoplists);

    $self->{DOCUMENT_FIELDS} = \@document_fields;
    $self->{STOPLIST} = \@stoplists;

    $self->{CZECH_LANGUAGE} = $self->{LANGUAGE} eq 'cz' ? 1 : 0;

    return $fetch_status;

}

sub _phrase_search {
    my $self = shift;
    my @result_documents = $self->{RESULT_VECTOR}->Index_List_Read;
    return if $#result_documents < 0;
    return if $#result_documents > $self->{PHRASE_THRESHOLD};

    my $vec_size = $self->max_indexed_id + 1;
    my $phrase_vector;
    my $i = 0;

    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
    	my $phrase_c = 0;
	foreach my $phrase ( @{$self->{QUERY_PHRASES}->[$fno]} ) {
	    my @found;
	    if (not $self->{PINDEX}) {
                # full content scan
                print "phrase search: '$phrase' using full content scan\n"
                    if $PA;
		@found = @{$self->_phrase_fullscan(\@result_documents, $fno,
						   $phrase)};
	    } else {
                # proximity scan
		my $proximity = $self->{QUERY_PROXIMITY}->[$fno]->[$phrase_c] ?
		    $self->{QUERY_PROXIMITY}->[$fno]->[$phrase_c] : 1;
		print "phrase search: '$phrase' using proximity index, proximity: $proximity\n"
		    if $PA;				
		@found = @{$self->_phrase_proximity(\@result_documents, $fno,
						    $phrase, $proximity)};
	    }
	    
	    my $vector = Bit::Vector->new($vec_size);
	    $vector->Index_List_Store(@found);
	    if ($i == 0) {
		$phrase_vector = $vector;
	    } else {
		$phrase_vector->Union($phrase_vector, $vector);
	    }
	    $i++;
	    $phrase_c++;
	}
    }
    return if $i < 1;
    $self->{RESULT_VECTOR}->Intersection($self->{RESULT_VECTOR},
					 $phrase_vector);
}

sub _phrase_fullscan {
	my $self = shift;
	my $docref = shift;
	my $fno = shift;
	my $phrase = shift;
	
	my @documents = @{$docref};
	my $documents = join(',', @documents);
	my @found;

	my $sql = $self->{CZECH_LANGUAGE} ? 
	    $self->db_phrase_scan_cz($documents, $fno) :
	    $self->db_phrase_scan($documents, $fno);
        		  
	my $sth = $self->{DOCUMENT_DBH}->prepare($sql);
        
	if ($self->{CZECH_LANGUAGE}) {
	    $sth->execute;
	} else {
	    $sth->execute("%$phrase%");
	}

	my ($document_id, $content);
	if ($self->{CZECH_LANGUAGE}) {
	    $sth->bind_columns(\$document_id, \$content);
	} else {
	    $sth->bind_columns(\$document_id);
	}
    
	while($sth->fetch) {
	    if ($self->{CZECH_LANGUAGE}) {
		$content = $self->_trans($content);
		push(@found, $document_id) if (index($content, $phrase) != -1);
		print "content scan for $document_id, phrase = $phrase\n"
		    if $PA > 1;
	    } else {
		push(@found, $document_id);
	    }
	}

	return \@found;
}

sub _phrase_proximity {
    my $self = shift;
    my $docref = shift;
    my $fno = shift;
    my $phrase = shift;
    my $proximity = shift;

    my @documents = @{$docref};
    my $documents = join(',', @documents);
    my @found;

    my @pwords = grep { length($_) > 0 } split(/[^a-zA-Z0-9]+/, $phrase);

    if ($PA) {
	print "phrase search: proximity scan for words: ";
	local $, = ', ';
	print @pwords;
	print "\n";
    }

    my $pwords = join(',', map { $self->{INDEX_DBH}->quote($_) } @pwords);
    my $sql = $self->db_pindex_search($fno,	$pwords, $documents);

    my $sth = $self->{INDEX_DBH}->prepare($sql);
    my $rows = $sth->execute;
    my($word, $document, $pos);
    my %document;
    my $last_document = 0;
    $sth->bind_columns(\$word, \$document, \$pos);
    my $i = 0;

    while($sth->fetch) {
	$i++;
	if ( ($document != $last_document && $last_document != 0) || $i == $rows) {
	    
            # process the previous/last document
	    
	    push(@{$document{$word}}, $pos) if ($i == $rows);	# last document

	    if ($self->_proximity_match($proximity, $last_document, \@pwords,
					\%document)) {
		push(@found, $last_document);
		print "phrase: proximity MATCHED document $last_document\n"
		    if $PA;
	    } else {
		print "phrase: proximity NOT matched document $last_document\n"
		    if $PA;
	    }
	    %document = 0;	# remove all words from this document
    	}

	push(@{$document{$word}}, $pos);
	$last_document = $document;
    }

    return \@found;
}

sub _proximity_match {
    my $self = shift;
    my ($proximity, $doc_id, $pwords, $document) = @_;

    my $occur = 1;
    my $match = 0;
    print "phrase: proximity searching document $doc_id\n" if $PA;

    foreach my $fword_pos (@{$document->{$pwords->[0]}}) {
        $match = 0;
        print qq(base phrase word "$pwords->[0]",if occurency $occur at $fword_pos\n)
	    if $PA > 1;
        for(my $i = 0; $i < @{$pwords} - 1; $i++) {
            $match = 0;
            my $window = $i + $proximity;
            foreach my $sword_pos (@{$document->{$pwords->[$i+1]}}) {
                print "sequence word $pwords->[$i+1] at $sword_pos\n"
                    if $PA > 1;
                if ($sword_pos > $fword_pos &&
                    $sword_pos <= $fword_pos + $window) {
                    $match = 1;
                    print "sequence word $pwords->[$i+1] at $sword_pos matched\n"
                        if $PA > 1;
                    last;
                }
            }
            last if (not $match);
        }	# end of all neccessary scan
        if ($match) {
            print "phrase: doc $doc_id, occurency $occur matched\n"
                if $PA > 1;
            last;
        }

        $occur++;
    } # end of occurencies

    return $match;
}

sub _vector_search {
    my $self = shift;
    my $maxid = $self->max_indexed_id + 1;
    foreach my $field_no ( @{$self->{QUERY_FIELD_NOS}} ) {
	foreach my $word ( @{$self->{QUERY_WORDS}->[$field_no]} ) {
	    if (not $self->{VECTOR}->[$field_no]->{$word}) {
            	# vector for this word has not been yet defined in _parse_query
		$self->{VECTOR}->[$field_no]->{$word} = Bit::Vector->new_Enum(
                  $maxid, $self->_fetch_documents_vector($field_no, $word));
	    }
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
    
    my $sql = $self->db_fetch_maxtf($field_nos);

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

    my %score;

    return $ERROR{'no_results'} if $#result_documents < 0;

    if ($self->{OR_WORD_COUNT} == 1 && $self->{AND_WORD_COUNT} == 0
	&& $#result_documents > $self->{RESULT_THRESHOLD}) {

	my $field_no = $self->{QUERY_FIELD_NOS}->[0];
	my $word = $self->{QUERY_OR_WORDS}->[$field_no]->[0];

	my $occurence = $self->_occurence($field_no, $word);

	# idf should use a collection size instead of max_indexed_id

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
		# next WORD unless defined $occurence;
		$occurence = 1 unless defined $occurence;

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
		    # next DOCUMENT_ID unless defined $word_score{$document_id};
		    $word_score{$document_id} = 1 unless defined $word_score{$document_id};

		    my $maxtf = $self->{MAXTF}->[$field_no]->[$document_id];
		    my $sqrt_maxtf = sqrt($maxtf);
		    $sqrt_maxtf = 1 unless $sqrt_maxtf;
		    if ($score{$document_id}) {
			$score{$document_id} *=
			    (1 + (($word_score{$document_id}/$sqrt_maxtf) * $idf));
		    } else {
			$score{$document_id} = (1 + (($word_score{$document_id}/$sqrt_maxtf) * $idf));
		    }
		}
	    }
	}
	unless (scalar keys %score) {
		if (not @{$self->{STOPLISTED_QUERY}}) {
			return $ERROR{no_results};
		}
		else {
			return $self->_stoplisted_error;
		}
	}
	return \%score;

    }

}

sub _stoplisted_error {
	my $self = shift;
	my $stoped = join(', ', @{$self->{STOPLISTED_QUERY}});
	return $ERROR{no_results_stop}.' '.$stoped.'.';
}

sub _occurence {
    my $self = shift;
    my $field_no = shift;
    my $word = shift;
    return undef unless $word;
    my $sql = $self->db_occurence($self->{INVERTED_TABLES}->[$field_no]);
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    $sth->execute($word);
    my ($occurence) = $sth->fetchrow_array;
    return undef unless $occurence;
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
    my $sql = $self->db_fetch_mask;
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

	$self->{MASK_VECTOR}->{$mask} =
		Bit::Vector->new_Enum(($self->{MAX_INDEXED_ID} + 1), $documents_vector);

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
    	my (@phrase, @words, @and_words, @or_words, @not_words, @all_words,
    		@proximity, @wildcards, @highlight);

	$query = $self->_trans($query);

    	for my $position (0 .. ($string_length - 1)) {
    	    my $char = substr($query, $position, 1);
    	    if ($char eq '"') {
		$quote_count++;
	    }
    	    $phrase_count = int(($quote_count - 1)/ 2);
    	    if ($quote_count % 2 != 0 && $char ne '"') {
		$raw_phrase[$phrase_count] .= $char;
    	    } else {
		$word .= $char;
    	    }
    	}

    	@words = grep {

	    $_ =~ tr/[a-zA-Z0-9*%+-]//cd;
            length($_) > 0;

    	} split(/\s+/, $word);

    	foreach my $word (@words) {
	    $word =~ m/^([+-])?(\w+)([%*])?$/;
            my $op = $1;
            my $pword = $2;
            my $wild = $3;

	    $pword = substr($pword, 0, $self->{MAX_WORD_LENGTH});
            next if ($self->_stoplisted($pword));

            print "parsed word: $pword".($wild ? "; wildcard: $wild" : '')."\n"
            	if $PA;
	    my @vec;
	    my $setvector = 0;
	    my $maxid = $self->max_indexed_id + 1;
            if ($wild eq '%') {
            	next if (length($pword) < $self->{MIN_WILDCARD_LENGTH});

                my $table = $self->{INVERTED_TABLES}->[$field_no];
		my $sql = $self->db_fetch_words($table);
       	        my $words = $self->{INDEX_DBH}->selectcol_arrayref($sql,
						  undef, "$pword%");

                foreach my $word (@{$words}) {
		    if ($op eq '+') {
			push(@vec, Bit::Vector->new_Enum($maxid,
			    $self->_fetch_documents_vector($field_no, $word)));
			$setvector = 1;
                    	push(@all_words, $word);
                        next;
		    } elsif ($op eq '-') {
                        push @not_words, $word;
                    } else {
                        push @or_words, $word;
                    }
                    push (@all_words, $word);
		    push(@wildcards, $wild);
               	}
		push(@highlight, $pword);
                if ($setvector) {
		    push(@and_words, $pword);
                    push(@all_words, $pword);
		    push(@wildcards, $wild);
                    my $unionvec = Bit::Vector->new($maxid);
                    foreach my $vec (@vec) {
			$unionvec->Union($unionvec, $vec);
                    }
		    $self->{VECTOR}->[$field_no]->{$pword} = $unionvec;
                }
            } elsif ($wild eq '*') {
                if ($op eq '+') {
		    push(@vec, Bit::Vector->new_Enum($maxid,
			 $self->_fetch_documents_vector($field_no, $pword)));
		    push(@vec, Bit::Vector->new_Enum($maxid,
		       $self->_fetch_documents_vector($field_no, $pword.'s')));

                    push(@and_words, $pword);

                    my $unionvec = Bit::Vector->new($maxid);
                    foreach my $vec (@vec) {
			$unionvec->Union($unionvec, $vec);
                    }
		    $self->{VECTOR}->[$field_no]->{$pword} = $unionvec;
                } elsif ($op eq '-') {
                    push (@not_words, $pword);
                    push (@not_words, $pword.'s');
                } else {
                    push (@or_words, $pword);
                    push (@or_words, $pword.'s');
                }

                push(@all_words, $pword);
                push(@all_words, $pword.'s');
		push(@highlight, $pword);
                push(@wildcards, $wild);
	    } else {  # Not wild
                if ($op eq '+') {
                    push @and_words, $pword;
                } elsif ($op eq '-') {
                    push @not_words, $pword;
                } else {
                    push @or_words, $pword;
                }

                push @all_words, $pword;
		push(@highlight, $pword);
                push(@wildcards, undef);
            }
    	}

    	foreach my $phrase (@raw_phrase) {
	    $phrase =~ s/^:(\d+)\s+(.+)$/$2/;
	    my $proximity = $1;
    	    my @split_phrase = split/\s+/, $phrase;
    	    $word_count = @split_phrase;
    	    if ($word_count == 1) {
    	    	if (not $self->_stoplisted($phrase)) {
		    push @or_words, $phrase;
		    push @all_words, $phrase;
		    push(@highlight, $phrase);
		    push(@wildcards, undef);
                 }
    	    } elsif ($phrase =~ m/^\s*$/) {
		next;
    	    } else {
            	my $stop = 0;
		my @p_words;
                foreach my $word (@split_phrase) {
		    if (not $self->_stoplisted($word)) {
			push @p_words, $word;
		    } else {
			$stop = 1;
                        last;
                    }
		}
		if (not $stop) {
		    push @and_words, @p_words;
		    push @all_words, @p_words;
		    push (@phrase, $phrase);
		    push (@proximity, $proximity) if ($self->{PINDEX});
		}
    	    }
    	}	#  end of phrase processing

    	if ($quote_count % 2 != 0) {
    	    $error = $ERROR{'$quote_count'};
    	}

    	$self->{QUERY_PHRASES}->[$field_no] = \@phrase;
    	$self->{QUERY_PROXIMITY}->[$field_no] = \@proximity
	    if ($self->{PINDEX});

	$self->{QUERY_WILDCARDS}->[$field_no] = \@wildcards;
	$self->{QUERY_HIGHLIGHT}->[$field_no] = \@highlight;

    	$self->{QUERY_OR_WORDS}->[$field_no] = \@or_words;
    	$self->{QUERY_AND_WORDS}->[$field_no] = \@and_words;
    	$self->{QUERY_NOT_WORDS}->[$field_no] = \@not_words;
    	$self->{QUERY_WORDS}->[$field_no] = \@all_words;

    	$self->{HIGHLIGHT} = join '|', @all_words;
	
    }	# end of field processing

    return $error;
}

# here come all translations related to case, accented characters
# or diacritics, we must normalize everything to lower case

sub _trans {
	my $self = shift;
	my $s = shift;

	if ($self->{CZECH_LANGUAGE}) {
		$s = &CzFast::czrecode('iso-8859-2', 'ascii', $s)
	}
	else {
    	# accents
		$s =~ tr/\xe8\xe9\xf1\xe1/eena/;
	}

	$s = lc($s );
	return $s;
}

sub _documents {

    my $self = shift;
    my $field_no = shift;
    my $word = shift;

    local $^W = 0; # turn off silly uninitialized value warning
    if (@_) {
		my ($id, $frequency) = @_;
		$self->{DOCUMENTS}->[$field_no]->{$word} .=
			pack 'ww', ($id, $frequency);
		$self->{OCCURENCE}->[$field_no]->{$word}++; 
    } else {
		unpack 'w*', $self->_fetch_documents($field_no, $word);
    }

}

sub _fetch_documents {
    my $self = shift;
    my $field_no = shift;
    my $word = shift;

    my $sql = $self->db_fetch_documents($self->{INVERTED_TABLES}->[$field_no]);
    my $sth = $self->{INDEX_DBH}->prepare($sql);
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

    print qq(Fetching vector for "$word"\n) if $PA;

    my $sql = $self->db_fetch_documents_vector(
        $self->{INVERTED_TABLES}->[$field_no]);
    return scalar $self->{INDEX_DBH}->selectrow_array($sql, undef, $word);
}

sub _commit_documents {

    my $self = shift;

    print "Storing max term frequency for each document\n" if $PA;

    my $use_all_fields = 1;
    $self->_fetch_maxtf($use_all_fields);

    my $sql = $self->db_update_maxtf;
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

    print "Committing inverted tables to database\n" if $PA;

    foreach my $field_no ( 0 .. $#{$self->{DOCUMENT_FIELDS}} ) {

	my $sql = $self->db_inverted_replace($self->{INVERTED_TABLES}->[$field_no]);
	my $i_sth = $self->{INDEX_DBH}->prepare($sql);

	print("field$field_no ", scalar keys %{$self->{DOCUMENTS}->[$field_no]},
	       " distinct words\n") if $PA;

	while (my ($word, $documents) = each %{$self->{DOCUMENTS}->[$field_no]}) {

	    print "$word\n" if $PA >= 2;

	    my $sql = $self->db_inverted_select($self->{INVERTED_TABLES}->[$field_no]);
	    my $s_sth = $self->{INDEX_DBH}->prepare($sql);

	    $s_sth->execute($word);

	    my $o_occurence = 0;
	    my $o_documents_vector = '';
	    my $o_documents = '';

	    $s_sth->bind_columns(\$o_occurence, \$o_documents_vector,
	    	\$o_documents);

	    $s_sth->fetch;
	    $s_sth->finish;

	    my %frequencies = unpack 'w*', $documents;

	    my $o_vector = Bit::Vector->new_Enum(($self->{MAX_INDEXED_ID} + 1),
	    	$o_documents_vector);
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

	my $sql = $self->db_fetch_document($field);
    return scalar $self->{DOCUMENT_DBH}->selectrow_array($sql, undef, $id);
}

sub _words {
    my $self = shift;
    my $document = shift;

    # kill tags
    $document =~ s/<.*?>//g;

    # kill junk
    $document =~ s/&gt;
				  |&lt;
				  |&amp;
				  |&quot;
				  |&apos;
				  |&copy;
				  //xg;

    $document = $self->_trans($document);

    # split words on any non-word character or on underscore

    return grep {
	
	$_ = substr($_, 0, $self->{MAX_WORD_LENGTH});
	$_ =~ /[a-z]+/ && not $self->_stoplisted($_)

    } split(/[^a-zA-Z0-9]+/, $document);
}

sub _ping_document {
    my $self = shift;
    my $id = shift;

    my $sql = $self->db_ping_document;
    my $sth = $self->{DOCUMENT_DBH}->prepare($sql);
    $sth->execute($id);
    return $sth->rows;
}

sub _create_tables {
    my $self = shift;
    my ($sql, $sth);

    # mask table

    $self->db_drop_table($self->{MASK_TABLE});
    print "Dropping mask table ($self->{MASK_TABLE})\n"	if $PA;

    $sql = $self->db_create_mask;
    print "Creating mask table ($self->{MASK_TABLE})\n" if $PA;
    $self->{INDEX_DBH}->do($sql);

    # max term frequency table

    $self->db_drop_table($self->{MAXTF_TABLE});
    print "Dropping max term frequency table ($self->{MAXTF_TABLE})\n" if $PA;

    $sql = $self->db_create_maxterm;
    print "Creating max term frequency table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->{INDEX_DBH}->do($sql);


    # inverted tables

    foreach my $table ( @{$self->{INVERTED_TABLES}} ) {
	$self->db_drop_table($table);
	print "Dropping inverted table ($table)\n" if $PA;

	$sql = $self->db_create_inverted($table);
	print "Creating inverted table ($table)\n" if $PA;
	$self->{INDEX_DBH}->do($sql);
    }

    if ($self->{PINDEX}) {
	foreach my $table ( @{$self->{PINDEX_TABLES}} ) {
	    $self->db_drop_table($table);
	    print "Dropping proximity table ($table)\n"	if $PA;

	    $sql = $self->db_pindex_create($table);
	    print "Creating proximity table ($table)\n" if $PA;
	    $self->{INDEX_DBH}->do($sql);
	}
    }
}

sub _stoplisted {
    my $self = shift;
	my $word = shift;
	
    if ($self->{STOPLIST} and $self->{STOPLISTED_WORDS}->{$word}) {
	push(@{$self->{STOPLISTED_QUERY}}, $word);
	print "stoplisting: $word\n" if $PA > 1;
	return 1;
    }
    else {
	return 0;
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
    db => 'mysql',
    proximity_index => 0,
    errors => {
        empty_query => "your query was empty",
        quote_count => "phrases must be quoted correctly",
    	no_results => "your seach did not produce any results",
    	no_results_stop => "no results, these words were stoplisted: "
    },
    language => 'en', # cz or en
    stoplist => [ 'en' ],
    max_word_length => 12,
    result_threshold => 5000,
    phrase_threshold => 1000,
    min_wildcard_length => 5,
    print_activity => 0
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

DBIx::TextIndex was developed for doing full-text searches on BLOB
columns stored in a database.  Almost any database with BLOB and DBI
support should work with minor adjustments to SQL statements in the
module.

Implements a crude parser for tokenizing a user input string into
phrases, can-include words, must-include words, and must-not-include
words.

Operates in case insensitive manner.

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
    collection => 'collection_1'
});

Other arguments are optional.

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

DBI connection handle to database containing TextIndex tables.  Using
a separate database for your TextIndex is recommended, because the
module creates and drops tables without warning.

=item collection

A name for the index.  Should contain only alpha-numeric characters or
underscores [A-Za-z0-9_]

=item proximity_index

Activates a proximity index for faster phrase searches and word
proximity based matching. Disabled by default. Only efficient for
bigger documents.  Takes up a lot of space and slows down the indexing
proccess.  Proximity based matching is activated by a query containing
a phrase in form of:

	":2 some phrase" => matches "some nice phrase"
	":1 some phrase" => matches only exact "some phrase"
	":10 some phrase" => matches "some [1..9 words] phrase"
		
	Defaults to ":1" when omitted.

The proximity matches work only forwards, not backwards, that means:

  	":3 some phrase" does not match "phrase nice some" or "phrase some"

=item db

SQL used in this module is database specific in some aspects.  In
order to use this module with a variety of databases, so called
"database module" can be specified. Default is the B<mysql> module.
Another modules have yet to be written.

Names of the database modules correspond to the names of DBI drivers
and are case sensitive.

=item errors

This hash reference can be used to override default error messages.
Please refer to the SYNOPSIS for meaning of the particular keys and
values.

=item language
Accepts a value of 'en' or 'cz'. Default is 'en'.

Passing 'cz' to language activates support for the Czech language.
Operates in a diacritics insensitive manner. This option may also be
usable for other iso-8859-2 based Slavic languages. Basically it
converts both indices data and queries from iso-8859-2 to pure ASCII.

Requires module B<CzFast> that is available on CPAN in a directory of
author "TRIPIE".

=item stoplist

Activates stoplisting of very common words that are present in almost
every document. Default is not to use stoplisting.  Value of the
parameter is a reference to array of two-letter language codes in
lower case.  Currently only two stoplists exist:

	en => English
	cz => Czech

=item max_word_length

Specifies maximum word length resolution. Defaults to 12 characters.

=item result_threshold

Defaults to 5000 documents.

=item phrase_threshold

Defaults to 1000 documents.

=item print_activity

Activates STDOUT debugging. Higher value increases verbosity.

=back

After creating a new TextIndex for the first time, and after calling
initialize(), only the index_dbh, document_dbh, and collection
arguments are needed to create subsequent instances of a TextIndex.

=head2 $index->initialize

This method creates all the inverted tables for the TextIndex in the
database specified by document_dbh. This method should be called only
once when creating a new index! It drops all the inverted tables
before creating new ones.

initialize() also stores the document_table, document_fields,
document_id_field, language, stoplist, error attributes,
proximity_index, max_word_length, result_threshold, phrase_threshold
and min_wildcard_length preferences in a special table called
"collection," so subsequent calls to new() for a given collection do
not need those arguments.

Calling initialize() will upgrade the collection table created by
earlier versions of DBIx::TextIndex if necessary.

=head2 $index->upgrade_collection_table

Upgrades the collection table to the latest format. Usually does not
need to be called by the programmer, because initialize() handles
upgrades automatically.

=head2 $index->add_document(\@document_ids)

Add all the @documents_ids from document_id_field to the TextIndex.
@document_ids must be sorted from lowest to highest.  All further
calls to add_document() must use @document_ids higher than those
previously added to the index.  Reindexing previously-indexed
documents will yield unpredictable results!

=head2 $index->remove_document(\@document_ids)

This method accepts a reference to an array of document ids as its
parameter. The specified documents will be removed from the index, but
not from the actual documents table that is being indexed. The
documents itself must be accessible when you remove them from the
index. The ids should be sorted from lowest to highest.

It's actually not possible to completely recover the space taken by
the documents that are removed, therefore it's recommended to rebuild
the index when you remove a significant amount of documents.

All space reserved in the proximity index is recovered.  Approx. 75%
of space reserved in the inverted tables and max term frequency table
is recovered.

=head2 $index->disable_document(\@document_ids)

This method can be used to disable documents. Disabled documents are
not included in search results. This method should be used to "remove"
documents from the index. Disabled documents are not actually removed
from the index, therefore its size will remain the same. It's
recommended to rebuild the index when you remove a significant amount
of documents.

=head2 $index->search(\%search_args)

search() returns $results, a reference to a hash.  The keys of the
hash are document ids, and the values are the relative scores of the
documents.  If an error occured while searching, $results will be a
scalar containing an error message.

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

=head2 $index->unscored_search(\%search_args)

unscored_search() returns $document_ids, a reference to an array.  Since
the scoring algorithm is skipped, this method is much faster than search().
If an error occured while searching $document_ids will be a scalar
containing an error message.

$document_ids = $index->unscored_search({
    first_field => '+andword -notword orword "phrase words"',
    second_field => ...
});

if (ref $document_ids) {
    print "Here's all the document ids:\n";
    map { print "$_\n" } @$document_ids;
} else {
    print "Error: $document_ids\n";
}

=head2 $index->stat

Allows you to obtain some meta information about the index. Accepts one
parameter that specifies what you want to obtain.

	$index->stat('total_words')

Returns a total count of words in the index. This number
may differ from the total count of words in the documents
itself.

=head2 $index->delete

delete() removes the tables associated with a TextIndex from index_dbh.

=head1 SUPPORT FOR SEARCH MASKS

DBIx::TextIndex can apply boolean operations on arbitrary lists of
document ids to search results.

Take this table:

doc_id  category  doc_full_text

1       green     full text here ...

2       green     ...

3       blue      ...

4       red       ...

5       blue      ...

6       green     ...

Masks that represent document ids for in each the three categories can
be created:

=head2 $index->add_mask($mask_name, \@document_ids);

$index->add_mask('green_category', [ 1, 2, 6 ]);
$index->add_mask('blue_category', [ 3, 5 ]);
$index->add_mask('red_category', [ 4 ]);

The first argument is an arbitrary string, and the second is a
reference to any array of documents ids that the mask name identifies.

mask operations are passed in a second argument hash reference to
$index->search():

%query_args = (
    first_field => '+andword -notword orword "phrase words"',
    second_field => ...
    ...
);

%args = (
    not_mask => \@not_mask_list,
    and_mask => \@and_mask_list,
    or_mask  => \@or_mask_list,
    or_mask_set => [ \@or_mask_list_1, \@or_mask_list_2, ... ],
);

$index->search(\%query_args, \%args);

=over 4

=item not_mask

For each mask in the not_mask list, the intersection of the search query results and all documents not in the mask is calculated.

From our example above, to narrow search results to documents not in
green category:

$index->search(\%query_args, { not_mask => ['green_category'] });

=item and_mask

For each mask in the and_mask list, the intersection of the search
query results and all documents in the mask is calculated.

This would give return results only in blue category:

$index->search(\%query_args,
               { and_mask => ['blue_category'] });

Instead of using named masks, lists of document ids can be passed on
the fly as array references.  This would give the same results as the
previous example:

my @blue_ids = (3, 5);
$index->search(\%query_args,
               { and_mask => [ \@blue_ids ] });

=item or_mask_set

With the or_mask_set argument, the union of all the masks in each list
is computed individually, and then the intersection of each union set
with the query results is calculated.

=item or_mask

An or_mask is treated as an or_mask_set with only one list. In
this example, the union of blue_category and red_category is taken,
and then the intersection of that union with the query results is
calculated:

$index->search(\%query_args,
               { or_mask => [ 'blue_category', 'red_category' ] });

=head2 $index->delete_mask($mask_name);

Deletes a single mask from the mask table in the database.

=head1 PARTIAL PATTERN MATCHING USING WILDCARDS

You can use wildcard characters "%" or "*" at end of a word
to match all words that begin with that word. Example:

    the "%" character means "match any characters"

    car%	==> matches "car", "cars", "careful", "cartel", ....


    the "*" character means "match also the plural form"

    car*	==> matches only "car" or "cars"

The option B<min_wildcard_length> is used to set the minimum length of
word base appearing before the "%" wildcard character.
Defaults to five characters to avoid
selection of excessive amounts of word combinations. Unless this option
is set to a lower value, the examle above (car%) wouldn't produce
any results.

=head1 HIGHLIGHTING OF QUERY WORDS OR PATTERNS IN RESULTING DOCUMENTS

A module HTML::Highlight can be used either
independently or together with DBIx::TextIndex for this task.

The HTML::Highlight module provides a very nice Google-like
highligting using different colors for different words or phrases and also
can be used to preview a context in which the query words appear in
resulting documents.
		
The module works together with DBIx::TextIndex using its new method
html_highlight().

Check example script 'html_search.cgi' in the 'examples/' directory of
DBIx::TextIndex distribution or refer to the documentation of HTML::Highlight
for more information.

=head1 CZECH LANGUAGE SUPPORT

For czech diacritics insensitive operation you need to set the
B<language> option to 'cz'.

	my $index = DBIx::TextIndex->new({
		....
		language => 'cz',
		....
	});

This option MUST be set for correct czech language proccessing.
Diacritics sensitive operation is not possible.

B<Requires the module "CzFast" that is available on CPAN in directory
of author "TRIPIE".>

=head1 AUTHORS

Daniel Koch, dkoch@bizjournals.com.
Contributions by Tomas Styblo, tripie@cpan.org.

=head1 COPYRIGHT

Copyright 1997, 1998, 1999, 2000, 2001 by Daniel Koch.
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

Special thanks to Tomas Styblo, for proximity index support, Czech
language support, stoplists, highlighting, document removal and many
other improvements.

Thanks to Ulrich Pfeifer for ideas and code from Man::Index module
in "Information Retrieval, and What pack 'w' Is For" article from
The Perl Journal vol. 2 no. 2.

Thanks to Steffen Beyer for the Bit::Vector module, which
enables fast set operations in this module. Version 5.3 or greater of
Bit::Vector is required by DBIx::TextIndex.

=head1 BUGS

Uses quite a bit of memory.

Parser is not very good.

Documentation is not complete.

Please feel free to email me (dkoch@bizjournals.com) with any questions
or suggestions.

=head1 SEE ALSO

perl(1).

=cut
