package DBIx::TextIndex;

use strict;

our $VERSION = '0.15';

require XSLoader;
XSLoader::load('DBIx::TextIndex', $VERSION);

use Bit::Vector ();
use Carp qw(carp croak);
use DBIx::TextIndex::Exception;
use DBIx::TextIndex::TermDocsCache;
use HTML::Entities ();
use Text::Unaccent qw(unac_string);

my $GEN = 'DBIx::TextIndex::Exception::General';
my $DA  = 'DBIx::TextIndex::Exception::DataAccess';
my $QRY = 'DBIx::TextIndex::Exception::Query';

my $ME  = 'DBIx::TextIndex"';

# Version number when collection table definition last changed
my $LAST_COLLECTION_TABLE_UPGRADE = '0.12';

# Largest size word to be indexed
my $MAX_WORD_LENGTH = 20;

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

my $CHARSET_DEFAULT = 'iso-8859-1';

my @COLLECTION_FIELDS = qw(
    collection
    version
    max_indexed_id
    doc_table
    doc_id_field
    doc_fields
    charset
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
    decode_html_entities
);

my %COLLECTION_FIELD_DEFAULT = (
    collection => '',
    version => $DBIx::TextIndex::VERSION,
    max_indexed_id => '0',
    doc_table => '',
    doc_id_field => '',
    doc_fields => '',
    charset => $CHARSET_DEFAULT,
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
    decode_html_entities => '1',
);


my $PA = 0;		# just a shortcut to $self->{PRINT_ACTIVITY}

sub new {
    my $pkg = shift;
    my $args = shift;

    my $class = ref($pkg) || $pkg;
    my $self = bless {}, $class;

    $self->{COLLECTION_TABLE} = $COLLECTION_TABLE;
    $self->{COLLECTION_FIELDS} = \@COLLECTION_FIELDS;

    foreach my $arg ('collection', 'index_dbh', 'doc_dbh') {
	if ($args->{$arg}) {
	    $self->{uc $arg} = $args->{$arg};
	} else {
	    throw $GEN( error => "new $pkg needs $arg argument" );
	}
    }

    # term_docs field can have character 32 at end of string,
    # so DBI ChopBlanks must be turned off
    $self->{INDEX_DBH}->{ChopBlanks} = 0;

    $self->{PRINT_ACTIVITY} = 0;
    $self->{PRINT_ACTIVITY} = $args->{'print_activity'};
    $PA = $self->{PRINT_ACTIVITY};

    $args->{db} = $args->{db} ? $args->{db} : $DB_DEFAULT;
    my $db = 'DBIx/TextIndex/' . $args->{db} . '.pm';
    require "$db";

    unless ($self->_fetch_collection_info) {
	$self->{DOC_TABLE} = $args->{doc_table};
	$self->{DOC_FIELDS} = $args->{doc_fields};
	$self->{DOC_ID_FIELD} = $args->{doc_id_field};
	$self->{CHARSET} = $args->{charset} || $CHARSET_DEFAULT;
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
	$self->{DECODE_HTML_ENTITIES} = $args->{decode_html_entities}
	    || 1;
    }
    $self->{CZECH_LANGUAGE} = $self->{CHARSET} eq 'iso-8859-2' ? 1 : 0;
    $self->{MAXTF_TABLE} = $self->{COLLECTION} . '_maxtf';
    $self->{MASK_TABLE} = $self->{COLLECTION} . '_mask';
    $self->{ALL_DOCS_VECTOR_TABLE} = $self->{COLLECTION} . "_all_docs_vector";

    # Field number, assign each field a number 0 .. N
    my $fno = 0;

    foreach my $field ( @{$self->{DOC_FIELDS}} ) {
	$self->{FIELD_NO}->{$field} = $fno;
	push @{$self->{INVERTED_TABLES}},
	    ($self->{COLLECTION} . '_' . $field . '_inverted');
	if ($self->{PINDEX}) {
	    push @{$self->{PINDEX_TABLES}},
		($self->{COLLECTION} . '_' . $field . '_pindex');
	}
    	$fno++;
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

    # Cache for term_doc postings
    $self->{C} = DBIx::TextIndex::TermDocsCache->new({
	db => $args->{db},
	dbh => $self->{INDEX_DBH},
        max_indexed_id => $self->max_indexed_id,
	inverted_tables => $self->{INVERTED_TABLES},
    });

    return $self;
}

sub add_mask {

    my $self = shift;
    my $mask = shift;
    my $ids = shift;

    my $max_indexed_id = $self->max_indexed_id;

    # Trim ids from end instead here.
    if ($ids->[-1] > $max_indexed_id) {
	throw $GEN( error => "Greatest doc_id in mask ($mask) is larger than greatest doc_id in index" );
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

# Stub method for older deprecated name
sub add_document { shift->add_doc(@_) }

sub add_doc {
    my $self = shift;
    my @ids = @_;

    my $ids;
    if (ref $ids[0] eq 'ARRAY') {
	$ids = $ids[0];
    } elsif ($ids[0] =~ m/^\d+$/) {
	$ids = \@ids;
    }

    return if $#$ids < 0;

    my $add_count = $#$ids + 1;
    if ($PA) {
	print "Adding $add_count docs\n";
    }

    my @sort_ids = sort { $a <=> $b } @$ids;

    $self->{OLD_MAX_INDEXED_ID} = $self->max_indexed_id;
    $self->max_indexed_id($sort_ids[-1]);

    my @added_ids;

    foreach my $doc_id (@sort_ids) {
	print $doc_id if $PA;
	unless ($self->_ping_doc($doc_id)) {
	    print " skipped, no doc $doc_id found\n";
	    next;
	}

	foreach my $fno ( 0 .. $#{$self->{DOC_FIELDS}} ) {
	    print " field$fno" if $PA;

	    my %frequency;
	    my $maxtf = 0;
	    my $word_count = 0;
            my $table = $self->{PINDEX_TABLES}->[$fno];
	    my @words = $self->_words($self->_fetch_doc($doc_id,
				      $self->{DOC_FIELDS}->[$fno]));

	    foreach my $word (@words) {
		$frequency{$word}++;
		$maxtf = $frequency{$word} if $frequency{$word} > $maxtf;
                if ($self->{PINDEX}) {
		    my $sql = $self->db_pindex_add($table);
		    $self->{INDEX_DBH}->do($sql, undef,
					   $word, $doc_id, $word_count);
		    print "pindex: adding $doc_id, word: $word, pos: $word_count\n"
			if $PA > 1;
		}
		$word_count++;
	    }
	    print " $word_count" if $PA;
	    
	    while (my ($word, $frequency) = each %frequency) {
		$self->_docs($fno, $word, $doc_id, $frequency);
	    }
	    $self->{NEW_MAXTF}->[$fno]->[$doc_id] = $maxtf;
	}	# end of field indexing
	print "\n" if $PA;

	push @added_ids, $doc_id;

    }	# end of doc indexing

    $self->all_doc_ids(@added_ids);

    $self->_commit_docs;

    return $add_count;

}

# Stub method for older deprecated name
sub remove_document { shift->remove_doc(@_) }

sub remove_doc {
    my $self = shift;
    my @ids = @_;

    my $ids;
    if (ref $ids[0] eq 'ARRAY') {
	$ids = $ids[0];
    } elsif ($ids[0] =~ m/^\d+$/) {
	$ids = \@ids;
    }

    return if $#$ids < 0;

    if ($PA) {
	my $remove_count = $#{$ids} + 1;
	print "Removing $remove_count docs\n";
    }

    my $total_words = 0;
    my @remove_words;
    foreach my $doc_id (@$ids) {
	print $doc_id if $PA;
	croak "$ME: doc's content must be accessible to remove a doc"
	    unless $self->_ping_doc($doc_id);
	foreach my $fno ( 0 .. $#{$self->{DOC_FIELDS}} ) {
	    print " field$fno" if $PA;
	    my @words = $self->_words($self->_fetch_doc($doc_id,
				      $self->{DOC_FIELDS}->[$fno]));
	    my %words;
	    foreach my $word (@words) {
		$remove_words[$fno]->{$word}++ if (not $words{$word});
		$words{$word} = 1;
		$total_words++;
	    }
	}	# end of each field
    }	# end of each doc

    if ($self->{PINDEX}) {
	print "Removing docs from proximity index\n" if $PA;
	$self->_pindex_remove($ids);
    }

    print "Removing docs from max term frequency table\n" if $PA;
    $self->_maxtf_remove($ids);

    print "Removing docs from inverted tables\n" if $PA;
    $self->_inverted_remove($ids, \@remove_words);

    $self->_all_doc_ids_remove($ids);

    return $total_words; 	# return count of removed words
}

sub _pindex_remove {
    my $self = shift;
    my $docs_ref = shift;

    my $docs = join(', ', @{$docs_ref});
    foreach my $table (@{$self->{PINDEX_TABLES}}) {
	my $sql = $self->db_pindex_remove($table, $docs);
        print "pindex_remove: removing docs: $docs\n" if $PA;
        $self->{INDEX_DBH}->do($sql);
    }
}

sub _maxtf_remove {
    my $self = shift;
    my $docs_ref = shift;
    
    my @docs = @{$docs_ref};
    my $use_all_fields = 1;
    $self->_fetch_maxtf($use_all_fields);
    
    my $sql = $self->db_update_maxtf;
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    foreach my $fno ( 0 .. $#{$self->{DOC_FIELDS}} ) {
	my @maxtf = @{$self->{MAXTF}->[$fno]};
	foreach my $doc_id (@docs) {
	    $maxtf[$doc_id] = 0;
	}
	my $packed_maxtf = pack 'w' x ($#maxtf + 1), @maxtf;
	$sth->execute($fno, $packed_maxtf);
    }
}

sub _inverted_remove {
    my $self = shift;
    my $docs_ref = shift;
    my $words = shift;
	
    my @docs = @{$docs_ref};
    foreach my $fno (0..$#{$self->{DOC_FIELDS}}) {
	my $field = $self->{DOC_FIELDS}->[$fno];
	my $table = $self->{INVERTED_TABLES}->[$fno];
	my $sql;
	
	$sql = $self->db_inverted_replace($table);
	my $sth_replace = $self->{INDEX_DBH}->prepare($sql);

	$sql = $self->db_inverted_select($table);
	my $sth_select = $self->{INDEX_DBH}->prepare($sql);
	
	foreach my $word (keys %{$words->[$fno]}) {
            $sth_select->execute($word);
	    my($o_docfreq_t, $o_docs);
	    $sth_select->bind_columns(\$o_docfreq_t, \$o_docs);
	    $sth_select->fetch;
	    
	    print "inverted_remove: field: $field: word: $word\n" if $PA;
	    # @docs_all contains a doc id for each word that
	    # we are removing
	    my $docfreq_t = $o_docfreq_t - $words->[$fno]->{$word};
	    print "inverted_remove: old docfreq_t: $o_docfreq_t\n" if $PA;
	    print "inverted_remove: new docfreq_t: $docfreq_t\n" if $PA;
	    
	    # if new docfreq_t is zero, then we should remove the record
            # of this word completely
	    if ($docfreq_t < 1) {
		my $sql = $self->db_inverted_remove($table);
                $self->{INDEX_DBH}->do($sql, undef, $word);
                print qq(inverted_remove: removing "$word" completely\n)
		    if $PA;
            	next;
            }

	    # now we will remove the doc from the "docs" field
	    my $term_docs = term_docs_arrayref($o_docs);
	    my %delete_doc;
	    @delete_doc{@docs} = (1) x @docs;
	    my @new_term_docs;
	    for (my $i = 0; $i < $#$term_docs ; $i += 2) {
		next if $delete_doc{$term_docs->[$i]};
		push @new_term_docs, ($term_docs->[$i], $term_docs->[$i + 1]);
	    }

	    $sth_replace->execute($word, $docfreq_t,
				  pack_term_docs(\@new_term_docs));
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

    $self->{OR_WORD_COUNT} = 0;
    $self->{AND_WORD_COUNT} = 0;

    throw $QRY( error => $ERROR{empty_query}) unless $query;

    my @field_nos;

    foreach my $field (keys %$query) {
	throw $GEN( error => "invalid field ($field) in search()" )
	    unless exists $self->{FIELD_NO}->{$field};
	push @field_nos, $self->{FIELD_NO}->{$field};
    }

    @{$self->{QUERY_FIELD_NOS}} = sort { $a <=> $b } @field_nos;

    foreach my $field (keys %$query) {
	$self->{QUERY}->[$self->{FIELD_NO}->{$field}] = $query->{$field};
    }

    $self->_parse_query_fields;

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

    $self->_fetch_vectors;
    $self->_optimize_or_search;
    $self->_boolean_compare;
    $self->_apply_mask;
    $self->_phrase_search;

    if ($args->{unscored_search}) {
	my @result_docs = $self->{RESULT_VECTOR}->Index_List_Read;
	throw $QRY( error => $ERROR{'no_results'} ) if $#result_docs < 0;
	return \@result_docs;
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

    return $self;
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

    print "Dropping docs vector table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->db_drop_table($self->{ALL_DOCS_VECTOR_TABLE});

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
	    $new_row{version} = $COLLECTION_FIELD_DEFAULT{version};
	}
	# 'czech_language', 'language' options replaced with 'charset'
	if (exists $old_row->{czech_language}) {
	    $new_row{charset} = 'iso-8859-2' if $old_row->{czech_language};
	}
	if (exists $old_row->{language}) {
	    if ($old_row->{language} eq 'cz') {
		$new_row{charset} = 'iso-8859-2';
	    } else {
		$new_row{charset} = $CHARSET_DEFAULT;
	    }
	}
	if (exists $old_row->{document_table}) {
	    $new_row{doc_table} = $old_row->{document_table};
	}
	if (exists $old_row->{document_id_field}) {
	    $new_row{doc_id_field} = $old_row->{document_id_field};
	}
	if (exists $old_row->{document_fields}) {
	    $new_row{doc_fields} = $old_row->{document_fields};
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
    my $doc_fields = join (',', @{$self->{DOC_FIELDS}});
    my $stoplists = ref $self->{STOPLIST} ?
	join (',', @{$self->{STOPLIST}}) : '';

    $self->{INDEX_DBH}->do($sql, undef,

			   $self->{COLLECTION},
			   $DBIx::TextIndex::VERSION,
			   $self->{MAX_INDEXED_ID},
			   $self->{DOC_TABLE},
			   $self->{DOC_ID_FIELD},

			   $doc_fields,
			   $self->{CHARSET},
			   $stoplists,
			   $self->{PINDEX},

			   $ERROR{empty_query},
			   $ERROR{quote_count},
			   $ERROR{no_results},
			   $ERROR{no_results_stop},

			   $self->{MAX_WORD_LENGTH},
			   $self->{RESULT_THRESHOLD},
			   $self->{PHRASE_THRESHOLD},
			   $self->{MIN_WILDCARD_LENGTH},

			   $self->{DECODE_HTML_ENTITIES},
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

    my $doc_fields;
    my $stoplists;

    my $null;
    $sth->bind_columns(
		       \$null,
		       \$self->{VERSION},
		       \$self->{MAX_INDEXED_ID},
		       \$self->{DOC_TABLE},
		       \$self->{DOC_ID_FIELD},

		       \$doc_fields,
		       \$self->{CHARSET},
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

		       \$self->{DECODE_HTML_ENTITIES},
		       );

    $sth->fetch;
    $sth->finish;

    my @doc_fields = split /,/, $doc_fields;
    my @stoplists = split (/,\s*/, $stoplists);

    $self->{DOC_FIELDS} = \@doc_fields;
    $self->{STOPLIST} = \@stoplists;

    $self->{CZECH_LANGUAGE} = $self->{CHARSET} eq 'iso-8859-2' ? 1 : 0;

    return $fetch_status;

}

sub _phrase_search {
    my $self = shift;
    my @result_docs = $self->{RESULT_VECTOR}->Index_List_Read;
    return if $#result_docs < 0;
    return if $#result_docs > $self->{PHRASE_THRESHOLD};

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
		@found = @{$self->_phrase_fullscan(\@result_docs, $fno,
						   $phrase)};
	    } else {
                # proximity scan
		my $proximity = $self->{QUERY_PROXIMITY}->[$fno]->[$phrase_c] ?
		    $self->{QUERY_PROXIMITY}->[$fno]->[$phrase_c] : 1;
		print "phrase search: '$phrase' using proximity index, proximity: $proximity\n"
		    if $PA;				
		@found = @{$self->_phrase_proximity(\@result_docs, $fno,
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

	my @docs = @{$docref};
	my $docs = join(',', @docs);
	my @found;

	my $sql = $self->{CZECH_LANGUAGE} ? 
	    $self->db_phrase_scan_cz($docs, $fno) :
	    $self->db_phrase_scan($docs, $fno);

	my $sth = $self->{DOC_DBH}->prepare($sql);

	if ($self->{CZECH_LANGUAGE}) {
	    $sth->execute;
	} else {
	    $sth->execute("%$phrase%");
	}

	my ($doc_id, $content);
	if ($self->{CZECH_LANGUAGE}) {
	    $sth->bind_columns(\$doc_id, \$content);
	} else {
	    $sth->bind_columns(\$doc_id);
	}
    
	while($sth->fetch) {
	    if ($self->{CZECH_LANGUAGE}) {
		$content = $self->_lc_and_unac($content);
		push(@found, $doc_id) if (index($content, $phrase) != -1);
		print "content scan for $doc_id, phrase = $phrase\n"
		    if $PA > 1;
	    } else {
		push(@found, $doc_id);
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

    my @docs = @{$docref};
    my $docs = join(',', @docs);
    my @found;

    my @pwords = grep { length($_) > 0 } split(/[^a-zA-Z0-9]+/, $phrase);

    if ($PA) {
	print "phrase search: proximity scan for words: ";
	local $, = ', ';
	print @pwords;
	print "\n";
    }

    my $pwords = join(',', map { $self->{INDEX_DBH}->quote($_) } @pwords);
    my $sql = $self->db_pindex_search($fno,	$pwords, $docs);

    my $sth = $self->{INDEX_DBH}->prepare($sql);
    my $rows = $sth->execute;
    my($word, $doc, $pos);
    my %doc;
    my $last_doc = 0;
    $sth->bind_columns(\$word, \$doc, \$pos);
    my $i = 0;

    while($sth->fetch) {
	$i++;
	if ( ($doc != $last_doc && $last_doc != 0) || $i == $rows) {
	    
            # process the previous/last doc
	    
	    push(@{$doc{$word}}, $pos) if ($i == $rows);	# last doc

	    if ($self->_proximity_match($proximity, $last_doc, \@pwords,
					\%doc)) {
		push(@found, $last_doc);
		print "phrase: proximity MATCHED doc $last_doc\n"
		    if $PA;
	    } else {
		print "phrase: proximity NOT matched doc $last_doc\n"
		    if $PA;
	    }
	    %doc = 0;	# remove all words from this doc
    	}

	push(@{$doc{$word}}, $pos);
	$last_doc = $doc;
    }

    return \@found;
}

sub _proximity_match {
    my $self = shift;
    my ($proximity, $doc_id, $pwords, $doc) = @_;

    my $occur = 1;
    my $match = 0;
    print "phrase: proximity searching doc $doc_id\n" if $PA;

    foreach my $fword_pos (@{$doc->{$pwords->[0]}}) {
        $match = 0;
        print qq(base phrase word "$pwords->[0]",if occurency $occur at $fword_pos\n)
	    if $PA > 1;
        for(my $i = 0; $i < @{$pwords} - 1; $i++) {
            $match = 0;
            my $window = $i + $proximity;
            foreach my $sword_pos (@{$doc->{$pwords->[$i+1]}}) {
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

sub _fetch_vectors {
    my $self = shift;
    my $maxid = $self->max_indexed_id + 1;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
	foreach my $term ( @{$self->{QUERY_WORDS}->[$fno]} ) {
	    if (not $self->{VECTOR}->[$fno]->{$term}) {
            	# vector for this word has not been yet defined in _parse_query
		$self->{VECTOR}->[$fno]->{$term} =
		    $self->{C}->vector($fno, $term);
		# FIXME: redundant storage
		$self->{DOCFREQ_T}->[$fno]->{$term} =
		    $self->{C}->docfreq_t($fno, $term);
	    }
	}
    }
}

sub _fetch_maxtf {

    my $self = shift;

    my $use_all_fields = shift;

    my $fnos;
    if ($use_all_fields) {
	$fnos = join ',', (0 .. $#{$self->{DOC_FIELDS}});
    } else {
	$fnos = join ',', @{$self->{QUERY_FIELD_NOS}};
    }
    
    my $sql = $self->db_fetch_maxtf($fnos);

    my $sth = $self->{INDEX_DBH}->prepare($sql);

    $sth->execute || warn $DBI::errstr;

    while (my $row = $sth->fetchrow_arrayref) {
	$self->{MAXTF}->[$row->[0]] = [(unpack 'w*', $row->[1])];
    }

    $sth->finish;

}

sub _search {

    my $self = shift;

    my @result_docs = $self->{RESULT_VECTOR}->Index_List_Read;

    my %score;

    throw $QRY( error => $ERROR{'no_results'} ) if $#result_docs < 0;

    # Special case: only one word in query and word is not very selective,
    # so don't bother tf-idf scoring, just return results sorted by docfreq_tt
    my $and_plus_or = $self->{OR_WORD_COUNT} + $self->{AND_WORD_COUNT};
    if ($and_plus_or == 1 && $#result_docs > $self->{RESULT_THRESHOLD}) {

	my $fno = $self->{QUERY_FIELD_NOS}->[0];
	my $word = $self->{QUERY_OR_WORDS}->[$fno]->[0] || $self->{QUERY_AND_WORDS}->[$fno]->[0];

	my $docfreq_t = $self->_docfreq_t($fno, $word);

	# idf should use a collection size instead of max_indexed_id

	my $idf;
	if ($docfreq_t) {
	    $idf = log($self->{MAX_INDEXED_ID}/$docfreq_t);
	} else {
	    $idf = 0;
	}

	throw $QRY( error => $ERROR{'no_results'} ) if $idf < $IDF_THRESHOLD;

	my $raw_score = $self->_docs($fno, $word);

	if ($self->{VALID_MASK}) {
	    foreach my $doc_id (@result_docs) {
		$score{$doc_id} = $raw_score->{$doc_id};
	    }
	    return \%score;
	} else {
	    return $raw_score;
	}

    }

    # Otherwise do tf-idf
    $self->_fetch_maxtf;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
      WORD:
	foreach my $word (@{$self->{QUERY_OR_WORDS}->[$fno]},
			  @{$self->{QUERY_AND_WORDS}->[$fno]} ) {
	    
	    my $docfreq_t = $self->_docfreq_t($fno, $word);
	    # next WORD unless defined $docfreq_t;
	    $docfreq_t = 1 unless defined $docfreq_t;

	    my $idf;
	    if ($docfreq_t) {
		$idf = log($self->{MAX_INDEXED_ID}/$docfreq_t);
	    } else {
		$idf = 0;
	    }

	    next WORD if $idf < $IDF_THRESHOLD;

	    my $word_score = $self->_docs($fno, $word);

	  DOC_ID:

	    foreach my $doc_id (@result_docs) {
		# next DOC_ID unless defined $word_score->{$doc_id};
		$word_score->{$doc_id} = 1 unless
		    defined $word_score->{$doc_id};
		
		my $maxtf = $self->{MAXTF}->[$fno]->[$doc_id];
		my $sqrt_maxtf = sqrt($maxtf);
		$sqrt_maxtf = 1 unless $sqrt_maxtf;
		if ($score{$doc_id}) {
		    $score{$doc_id} *=
			(1 + (($word_score->{$doc_id}/$sqrt_maxtf) * $idf));
		} else {
		    $score{$doc_id} = (1 + (($word_score->{$doc_id}/$sqrt_maxtf) * $idf));
		}
	    }
	}
    }
    unless (scalar keys %score) {
	if (not @{$self->{STOPLISTED_QUERY}}) {
	    throw $QRY( error => $ERROR{no_results} );
	} else {
	    throw $QRY( error => $self->_format_stoplisted_error );
	}
    }
    return \%score;

}

sub _format_stoplisted_error {
    my $self = shift;
    my $stopped = join(', ', @{$self->{STOPLISTED_QUERY}});
    return qq($ERROR{no_results_stop} $stopped.);
}

sub _docfreq_t {
    my $self = shift;
    my $fno = shift;
    my $word = shift;
    return undef unless $word;
    if ($self->{DOCFREQ_T}->[$fno]->{$word}) {
	return $self->{DOCFREQ_T}->[$fno]->{$word};
    }
    my $sql = $self->db_docfreq_t($self->{INVERTED_TABLES}->[$fno]);
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    $sth->execute($word);
    my ($docfreq_t) = $sth->fetchrow_array;
    return undef unless $docfreq_t;
    return $docfreq_t;
}

######################################################################
#
# _optimize_or_search()
#
#   If query contains large number of OR words,
#   turn the rarest words into AND words to reduce result set size
#   before scoring.
#
#   Algorithm: if there are four or less query words turn the two
#   least frequent OR words into AND words. For five or more query
#   words, make the three least frequent words OR words into AND words.
#
#   Does nothing if AND words already exist
#

sub _optimize_or_search {
    my $self = shift;

    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {

	my $or_word_count = 0; my $and_word_count = 0;

	$or_word_count = $#{ $self->{QUERY_OR_WORDS}->[$fno] } + 1;
	$and_word_count = $#{ $self->{QUERY_AND_WORDS}->[$fno] } + 1;

	return if $and_word_count > 0;
	return if $or_word_count < 1;

	# sort in order of docfreq_t
	my @or_words = sort { $self->{DOCFREQ_T}->[$fno]->{$a} <=> $self->{DOCFREQ_T}->[$fno]->{$b} } @{$self->{QUERY_OR_WORDS}->[$fno]};
	
	my @new_and_words;
	if ($or_word_count == 1) {
	    push @new_and_words, shift @or_words;
	    $or_word_count--;
	    $and_word_count++;
	} elsif ($or_word_count >=2 && $or_word_count <= 4) {
	    push @new_and_words, shift @or_words;
	    push @new_and_words, shift @or_words;
	    $or_word_count  -= 2;
	    $and_word_count += 2;
	} elsif ($or_word_count > 4) {
	    push @new_and_words, shift @or_words;
	    push @new_and_words, shift @or_words;
	    push @new_and_words, shift @or_words;
	    $or_word_count  -= 3;
	    $and_word_count += 3;
	}

	$self->{QUERY_OR_WORDS}->[$fno] = \@or_words;
	push @{ $self->{QUERY_AND_WORDS}->[$fno] }, @new_and_words;

	# FIXME: should these be cumulative across fields?
	$self->{OR_WORD_COUNT} += $or_word_count;
	$self->{AND_WORD_COUNT} += $and_word_count;

    }
}

sub _boolean_compare {

    my $self = shift;

    my $vec_size = $self->{MAX_INDEXED_ID} + 1;

    my @vectors;
    my $i = 0;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {

	$vectors[$i] = Bit::Vector->new($vec_size);
	if ($self->{OR_WORD_COUNT} < 1) {
	    $vectors[$i]->Fill;
	} else {
	    $vectors[$i]->Empty;
	}

	foreach my $word ( @{$self->{QUERY_OR_WORDS}->[$fno]} ) {
	    $vectors[$i]->Union($vectors[$i],
				 $self->{VECTOR}->[$fno]->{$word});

	}

	foreach my $word ( @{$self->{QUERY_AND_WORDS}->[$fno]} ) {
	    $vectors[$i]->Intersection($vectors[$i],
					$self->{VECTOR}->[$fno]->{$word});

	}

	foreach my $word ( @{$self->{QUERY_NOT_WORDS}->[$fno]} ) {
	    $self->{VECTOR}->[$fno]->{$word}->Flip;
	    $vectors[$i]->Intersection($vectors[$i],
					$self->{VECTOR}->[$fno]->{$word});

	}
	$i++;
    }

    $self->{RESULT_VECTOR} = Bit::Vector->new($vec_size);
    $self->{RESULT_VECTOR}->Index_List_Store($self->all_doc_ids);

    foreach my $vector (@vectors) {
	$self->{RESULT_VECTOR}->Intersection($self->{RESULT_VECTOR}, $vector);
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

	my $docs_vector;
	$sth->bind_col(1, \$docs_vector);
	$sth->fetch;

	$self->{MASK_VECTOR}->{$mask} =
	    Bit::Vector->new_Enum(($self->{MAX_INDEXED_ID} + 1), $docs_vector);

	$i++;

    }

    $sth->finish;
    return $mask_count;
}

sub _parse_query_fields {
    my $self = shift;

    # Quick check to make sure we have a query in at least on field
    my $have_query = 0;
    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
	my $query = $self->{QUERY}->[$fno];
	$have_query++ if $query =~ m/\S+/;

    }
    throw $QRY( error => $ERROR{'empty_query'} ) unless $have_query;

    foreach my $fno ( @{$self->{QUERY_FIELD_NOS}} ) {
	$self->_parse_query( $self->{QUERY}->[$fno], $fno );
    }

}

sub _parse_query {

    my $self = shift;
    my ($query, $fno) = @_;

    my $string_length = length($query);

    my $in_phrase = 0;
    my $phrase_count = 0;
    my $quote_count = 0;
    my $word_count = 0;
    my @raw_phrase = ();
    my $term = "";
    my (@phrase, @words, @and_words, @or_words, @not_words, @all_words,
	@proximity, @wildcards, @highlight);

    $query = $self->_lc_and_unac($query);

    for my $position (0 .. ($string_length - 1)) {
	my $char = substr($query, $position, 1);
	if ($char eq '"') {
	    $quote_count++;
	}
	$phrase_count = int(($quote_count - 1)/ 2);
	if ($quote_count % 2 != 0 && $char ne '"') {
	    $raw_phrase[$phrase_count] .= $char;
	} else {
	    $term .= $char;
	}
    }

    if ($quote_count % 2 != 0) {
	throw $QRY( error => $ERROR{'quote_count'} );
    }

    @words = grep {

	$_ =~ tr/[a-zA-Z0-9*%+-]//cd;
	length($_) > 0;

    } split(/\s+/, $term);

    foreach my $term (@words) {
	$term =~ m/^([+-])?(\w+)([%*])?$/;
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

	    my $table = $self->{INVERTED_TABLES}->[$fno];
	    my $sql = $self->db_fetch_words($table);
	    my $words = $self->{INDEX_DBH}->selectcol_arrayref($sql,
				       undef, "$pword%");

	    foreach my $term (@{$words}) {
		if ($op eq '+') {
		    push(@vec, $self->{C}->vector($fno, $term));
		    $setvector = 1;
		    push @all_words, $term;
		    next;
		} elsif ($op eq '-') {
		    push @not_words, $term;
		} else {
		    push @or_words, $term;
		}
		push @all_words, $term;
		push @wildcards, $wild;
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
		$self->{VECTOR}->[$fno]->{$pword} = $unionvec;
	    }
	} elsif ($wild eq '*') {
	    if ($op eq '+') {
		push(@vec, $self->{C}->vector($fno, $pword) );
		push(@vec, $self->{C}->vector($fno, $pword.'s') );

		push(@and_words, $pword);

		my $unionvec = Bit::Vector->new($maxid);
		foreach my $vec (@vec) {
		    $unionvec->Union($unionvec, $vec);
		}
		$self->{VECTOR}->[$fno]->{$pword} = $unionvec;
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
	    foreach my $term (@split_phrase) {
		if (not $self->_stoplisted($term)) {
		    push @p_words, $term;
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

    $self->{QUERY_PHRASES}->[$fno] = \@phrase;
    $self->{QUERY_PROXIMITY}->[$fno] = \@proximity
	if ($self->{PINDEX});

    $self->{QUERY_WILDCARDS}->[$fno] = \@wildcards;
    $self->{QUERY_HIGHLIGHT}->[$fno] = \@highlight;

    $self->{QUERY_OR_WORDS}->[$fno] = \@or_words;
    $self->{QUERY_AND_WORDS}->[$fno] = \@and_words;
    $self->{QUERY_NOT_WORDS}->[$fno] = \@not_words;
    $self->{QUERY_WORDS}->[$fno] = \@all_words;

    $self->{HIGHLIGHT} = join '|', @all_words;

}

# Set everything to lowercase and change accented characters to
# unaccented equivalents
sub _lc_and_unac {
    my $self = shift;
    my $s = shift;
    $s = unac_string($self->{CHARSET}, $s);
    $s = lc($s);
    return $s;
}

sub _docs {

    my $self = shift;
    my $fno = shift;
    my $word = shift;

    local $^W = 0; # turn off uninitialized value warning
    if (@_) {
	$self->{TERM_DOCS_VINT}->[$fno]->{$word} .= pack 'w*', @_;
	$self->{DOCFREQ_T}->[$fno]->{$word}++; 
    } else {
	term_docs_hashref($self->_fetch_docs($fno, $word));
    }

}

sub _fetch_docs {
    my $self = shift;
    my $fno = shift;
    my $word = shift;

    my $sql = $self->db_fetch_docs($self->{INVERTED_TABLES}->[$fno]);
    my $sth = $self->{INDEX_DBH}->prepare($sql);
    $sth->execute($word);

    my $docs;
    $sth->bind_col(1, \$docs);
    $sth->fetch;
    $sth->finish;
    return $docs;
}

sub _commit_docs {
    my $self = shift;

    print "Storing max term frequency for each doc\n" if $PA;

    my $use_all_fields = 1;
    $self->_fetch_maxtf($use_all_fields);

    my $sql = $self->db_update_maxtf;
	my $sth = $self->{INDEX_DBH}->prepare($sql);

    foreach my $fno ( 0 .. $#{$self->{DOC_FIELDS}} ) {
	my @maxtf;
	if ($#{$self->{MAXTF}->[$fno]} >= 0) {
	    @maxtf = @{$self->{MAXTF}->[$fno]};
	    @maxtf[($self->{OLD_MAX_INDEXED_ID} + 1) .. $self->{MAX_INDEXED_ID}] = @{$self->{NEW_MAXTF}->[$fno]}[($self->{OLD_MAX_INDEXED_ID} + 1) .. $self->{MAX_INDEXED_ID}];
	} else {
	    @maxtf = @{$self->{NEW_MAXTF}->[$fno]};
	}
	$maxtf[0] = 0 unless defined $maxtf[0];
	my $packed_maxtf = pack 'w' x ($#maxtf + 1), @maxtf;
	$sth->execute($fno, $packed_maxtf);
    }

    $sth->finish;

    print "Committing inverted tables to database\n" if $PA;

    foreach my $fno ( 0 .. $#{$self->{DOC_FIELDS}} ) {

	print("field$fno ", scalar keys %{$self->{TERM_DOCS_VINT}->[$fno]}, " distinct words\n") if $PA;

	my $i_sth = $self->{INDEX_DBH}->prepare( $self->db_inverted_replace($self->{INVERTED_TABLES}->[$fno]) );

	my $wc = 0;
	while (my ($word, $term_docs_vint) = each %{$self->{TERM_DOCS_VINT}->[$fno]}) {

	    print "$word\n" if $PA >= 2;
	    if ($PA && $wc > 0) {
		print "committed $wc words\n" if $wc % 500 == 0;
	    }

	    my $s_sth = $self->{INDEX_DBH}->prepare( $self->db_inverted_select($self->{INVERTED_TABLES}->[$fno]) );
	    
	    my $o_docfreq_t = 0;
	    my $o_term_docs = '';

	    $s_sth->execute($word);

	    $s_sth->bind_columns(\$o_docfreq_t, \$o_term_docs);

	    $s_sth->fetch;

	    my @term_docs = unpack 'w*', $term_docs_vint;

	    # unpack $o_term_docs into arrayref
	    my $old_term_docs = term_docs_arrayref($o_term_docs);
	    my $packed_term_docs;
	    $packed_term_docs = pack_term_docs( [ @$old_term_docs, @term_docs ] );

	    local $^W = 0;
	    $i_sth->execute($word,
			    ($self->{DOCFREQ_T}->[$fno]->{$word} + $o_docfreq_t),
			    $packed_term_docs
			    ) or warn $self->{INDEX_DBH}->err;

	    delete($self->{TERM_DOCS_VINT}->[$fno]->{$word});
	    $wc++;
	}
	print "committed $wc words\n" if $PA && $wc > 0;
	# Flush temporary hashes after data is stored
	delete($self->{TERM_DOCS_VINT}->[$fno]);
	delete($self->{DOCFREQ_T}->[$fno]);
    }
}

sub _all_doc_ids_remove {
    my $self = shift;
    my @ids = @_;
    # doc_id bits to unset
    if (ref $ids[0] eq 'ARRAY') {
	@ids = @{$ids[0]};
    }

    unless (ref $self->{ALL_DOCS_VECTOR}) {
	$self->{ALL_DOCS_VECTOR} = Bit::Vector->new_Enum(
               $self->max_indexed_id + 1,
	       $self->_fetch_all_docs_vector
	   );
    }

    if (@ids) {
	$self->{ALL_DOCS_VECTOR}->Index_List_Remove(@ids);
	$self->{INDEX_DBH}->do($self->db_update_all_docs_vector, undef, $self->{ALL_DOCS_VECTOR}->to_Enum);
    }

}

sub all_doc_ids {
    my $self = shift;
    my @ids = @_;

    # doc_id bits to set
    if (ref $ids[0] eq 'ARRAY') {
	@ids = @{$ids[0]};
    }

    unless (ref $self->{ALL_DOCS_VECTOR}) {
	$self->{ALL_DOCS_VECTOR} = Bit::Vector->new_Enum(
               $self->max_indexed_id + 1,
	       $self->_fetch_all_docs_vector
	   );
    }

    if (@ids) {
	if ($self->{ALL_DOCS_VECTOR}->Size() < $self->max_indexed_id + 1) {
	    $self->{ALL_DOCS_VECTOR}->Resize($self->max_indexed_id + 1);
	}
	$self->{ALL_DOCS_VECTOR}->Index_List_Store(@ids);
	$self->{INDEX_DBH}->do($self->db_update_all_docs_vector, undef, $self->{ALL_DOCS_VECTOR}->to_Enum);
    }

    return $self->{ALL_DOCS_VECTOR}->Index_List_Read;
}


sub _fetch_all_docs_vector {
    my $self = shift;
    my $fno = shift;
    my $word = shift;

    my $sql = $self->db_fetch_all_docs_vector;
    return scalar $self->{INDEX_DBH}->selectrow_array($sql);
}


sub _fetch_doc {
    my $self = shift;
    my $id = shift;
    my $field = shift;

    my $sql = $self->db_fetch_doc($field);
    return scalar $self->{DOC_DBH}->selectrow_array($sql, undef, $id);
}

sub _words {
    my $self = shift;
    my $doc = shift;

    # kill tags
    $doc =~ s/<.*?>/ /g;

    # Decode HTML entities
    if ($self->{DECODE_HTML_ENTITIES}) {
	$doc = HTML::Entities::decode($doc);
    }

    $doc = $self->_lc_and_unac($doc);

    # split words on any non-word character or on underscore

    return grep {
	$_ = substr($_, 0, $self->{MAX_WORD_LENGTH});
	$_ =~ /[a-z0-9]+/ && not $self->_stoplisted($_)
    } split(/[^a-zA-Z0-9]+/, $doc);
}

sub _ping_doc {
    my $self = shift;
    my $id = shift;

    my $sql = $self->db_ping_doc;
    my $sth = $self->{DOC_DBH}->prepare($sql);
    $sth->execute($id);
    return $sth->rows;
}

sub _create_tables {
    my $self = shift;
    my ($sql, $sth);

    # mask table

    print "Dropping mask table ($self->{MASK_TABLE})\n"	if $PA;
    $self->db_drop_table($self->{MASK_TABLE});

    $sql = $self->db_create_mask_table;
    print "Creating mask table ($self->{MASK_TABLE})\n" if $PA;
    $self->{INDEX_DBH}->do($sql);

    # max term frequency table

    print "Dropping max term frequency table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->db_drop_table($self->{MAXTF_TABLE});

    $sql = $self->db_create_maxterm_table;
    print "Creating max term frequency table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->{INDEX_DBH}->do($sql);

    # docs vector table

    print "Dropping docs vector table ($self->{MAXTF_TABLE})\n" if $PA;
    $self->db_drop_table($self->{ALL_DOCS_VECTOR_TABLE});

    $sql = $self->db_create_all_docs_vector_table;
    print "Creating docs vector table ($self->{ALL_DOCS_VECTOR_TABLE})\n" if $PA;
    $self->{INDEX_DBH}->do($sql);

    # inverted tables

    foreach my $table ( @{$self->{INVERTED_TABLES}} ) {
	$self->db_drop_table($table);
	print "Dropping inverted table ($table)\n" if $PA;

	$sql = $self->db_create_inverted_table($table);
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
    } else {
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
     doc_dbh => $doc_dbh,
     doc_table => 'doc_table',
     doc_fields => ['column_1', 'column_2'],
     doc_id_field => 'primary_key',
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
     decode_html_entities => 1,
     print_activity => 0
 });

 $index->initialize;

 $index->add_doc(\@doc_ids);

 my $results = $index->search({
     column_1 => '"a phrase" +and -not or',
     column_2 => 'more words',
 });

 foreach my $doc_id
     (sort {$$results{$b} <=> $$results{$a}} keys %$results ) 
 {
     print "DocID: $doc_id Score: $$results{$doc_id} \n";
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
     doc_dbh => $doc_dbh,
     doc_table => 'doc_table',
     doc_fields => ['column_1', 'column_2'],
     doc_id_field => 'primary_key',
     index_dbh => $index_dbh,
     collection => 'collection_1'
 });

Other arguments are optional.

=over 4

=item doc_dbh

DBI connection handle to database containing text documents

=item doc_table

Name of database table containing text documents

=item doc_fields

Reference to a list of column names to be indexed from doc_table

=item doc_id_field

Name of a unique integer key column in doc_table

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
process.  Proximity based matching is activated by a query containing
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

=item charset
Default is 'iso-8859-1'.

Accented characters are converted to ASCII equivalents based on the charset.

Pass 'iso-8859-2' for Czech or other Slavic languages.

=item stoplist

Activates stoplisting of very common words that are present in almost
every document. Default is not to use stoplisting.  Value of the
parameter is a reference to array of two-letter language codes in
lower case.  Currently only two stoplists exist:

	en => English
	cz => Czech

=item max_word_length

Specifies maximum word length resolution. Defaults to 20 characters.

=item result_threshold

Defaults to 5000 documents.

=item phrase_threshold

Defaults to 1000 documents.

=item decode_html_entities

Decode html entities before indexing documents (e.g. &amp; -> &). 
Default is 1.

=item print_activity

Activates STDOUT debugging. Higher value increases verbosity.

=back

After creating a new TextIndex for the first time, and after calling
initialize(), only the index_dbh, doc_dbh, and collection
arguments are needed to create subsequent instances of a TextIndex.

=head2 $index->initialize

This method creates all the inverted tables for the TextIndex in the
database specified by doc_dbh. This method should be called only
once when creating a new index! It drops all the inverted tables
before creating new ones.

initialize() also stores the doc_table, doc_fields,
doc_id_field, language, stoplist, error attributes,
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

=head2 $index->add_doc(\@doc_ids)

Add all the @docs_ids from doc_id_field to the TextIndex.
All further calls to add_doc() must use @doc_ids higher than
those previously added to the index.  Reindexing previously-indexed
documents will yield unpredictable results!

=head2 $index->remove_doc(\@doc_ids)

This method accepts a reference to an array of doc ids as its
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

=head2 $index->disable_doc(\@doc_ids)

This method can be used to disable documents. Disabled documents are
not included in search results. This method should be used to "remove"
documents from the index. Disabled documents are not actually removed
from the index, therefore its size will remain the same. It's
recommended to rebuild the index when you disable a significant amount
of documents.

=head2 $index->search(\%search_args)

search() returns $results, a hash reference.  The keys of the
hash are doc ids, and the values are the relative scores of the
documents.  If an error occured while searching, search will throw
a DBIx::TextIndex::Exception::Query object.

 eval {
     $results = $index->search({
         first_field => '+andword -notword orword "phrase words"',
         second_field => ...
         ...
     });
 };
 if ($@) {
     if ($@->isa('DBIx::TextIndex::Exception::Query') {
         print "No results: " . $@->error . "\n";
     } else {
         # Something more drastic happened
         $@->rethrow;
     }
 } else {
     print "The score for $doc_id is $results->{$doc_id}\n";
 }

=head2 $index->unscored_search(\%search_args)

unscored_search() returns $doc_ids, a reference to an array.  Since
the scoring algorithm is skipped, this method is much faster than search().
A DBIx::TextIndex::Exception::Query object will be thrown if the query is
bad or no results are found.

 eval {
     $doc_ids = $index->unscored_search({
         first_field => '+andword -notword orword "phrase words"',
         second_field => ...
     });
 };
 if ($@) {
     if ($@->isa('DBIx::TextIndex::Exception::Query') {
         print "No results: " . $@->error . "\n";
     } else {
         # Something more drastic happened
         $@->rethrow;
     }
 } else {
     print "Here's all the doc ids:\n";
     map { print "$_\n" } @$doc_ids;
 }

=head2 @doc_ids = $index->all_docs_ids

all_doc_ids() return a list of all doc_ids currently in the index.

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
doc ids to search results.

Take this table:

 doc_id  category  doc_full_text
 1       green     full text here ...
 2       green     ...
 3       blue      ...
 4       red       ...
 5       blue      ...
 6       green     ...

Masks that represent doc ids for in each the three categories can
be created:

=head2 $index->add_mask($mask_name, \@doc_ids);

 $index->add_mask('green_category', [ 1, 2, 6 ]);
 $index->add_mask('blue_category', [ 3, 5 ]);
 $index->add_mask('red_category', [ 4 ]);

The first argument is an arbitrary string, and the second is a
reference to any array of doc ids that the mask name identifies.

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

Instead of using named masks, lists of doc ids can be passed on
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

Copyright 1997-2003 by Daniel Koch.
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
