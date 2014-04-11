=head1 NAME

App::Basis::Queue

=head1 SYNOPSIS

 use App::Basis::Queue;

 my $queue = App::Basis::Queue->new( dbh => $dbh) ;
 
 # save some application audit data
 $queue->add( 'app_start', {
    ip => 12.12.12.12, 
    session_id => 12324324345, 
    client_id => 248296432984,
    appid => 2, 
    app_name => 'twitter'
 }) ;

 # in another process, we want to process that data

 my $queue = App::Basis::Queue->new( dbh => $dbh) ;

 $queue->process( 'app_start', count => 10, callback => \&processing_callback) ;

=head1 DESCRIPTION

Why have another queuing system? Well for me I wanted a queuing system that did not mean
I needed to install and maintain another server (ie RabbitMQ). Something that could run
against existing DBs (eg PostgreSQL). PGQ was an option, but as it throws away queued items
if there is not a listener, then this was useless! Some of the Job/Worker systems required you to create
classes and plugins to process the queue. Queue::DBI almost made the grade but only has one queue. 
I need multiple queues!

So I created this simple/basic system. You need to expire items, clean the queue and do things like that by hand,
there is no automation. You process items in the queue in chunks, not via a nice iterator.

There is no queue polling, there can only be one consumer of a record.

=head1 NOTES

I would use msgpack instead of JSON to store the data, but processing BLOBS in PostgreSQL is tricky.

To make the various inserts/queries work faster I cache the prepared statement handles against
a key and the fields that are being inserted, this speeds up the inserts roughly by 3 times
Adding 1000 records was taking 30+ seconds, now its more like 7 on PostgreSQL.

=head1 AUTHOR

kmulholland, moodfarm@cpan.org

=head1 VERSIONS

v0.1  2013-08-02, initial work

=head1 TODO

Currently the processing functions only process the earliest $MAX_PROCESS_ITEMS but
by making use of the counter in the info table, then we could procss the entire table
or at least a much bigger number and do it in chunks of $MAX_PROCESS_ITEMS

Processing could be by date

Add a method to move processed items to queue_name/processed and failures to queue_name/failures or
add them to these queues when marking them as processed or failed, will need a number of other methods to 
be updated but keeps less items in the unprocessed queue

=head1 See Also 

L<Queue::DBI>, L<AnyMQ::Queue>

=head1 API

=cut

package App::Basis::Queue;

use 5.10.0;
use feature 'state';
use strict;
use warnings;
use Moo;
use MooX::Types::MooseLike::Base qw/InstanceOf HashRef Str/;
use JSON;
use Data::UUID;
use Try::Tiny;
use POSIX qw( strftime);
use Time::HiRes qw(gettimeofday tv_interval );

my $MAX_PROCESS_ITEMS = 100;

# -----------------------------------------------------------------------------
## class initialisation
## instancation variables
# -----------------------------------------------------------------------------

has 'dbh' => (
    is  => 'ro',
    isa => InstanceOf ['DBI::db']
);

has 'prefix' => (
    is      => 'ro',
    isa     => Str,
    default => sub { 'qsdb'; },
);

has 'debug' => (
    is      => 'rw',
    default => sub { 0; },
    writer  => 'set_debug'
);

has 'skip_table_check' => (
    is      => 'ro',
    default => sub { 0; },
) ;

# -----------------------------------------------------------------------------
# once the class in instanciated then we need to ensure that we have the
# tables created

=head2 B<new>

Create a new instance of a queue

prefix - set a prefix name of the tables, allows you to have dev/test/live versions in the same database
debug - set basic STDERR debugging on or off
skip_table_check - don't check to see if the tables need creating

    my $queue = App::Basis::Queue->new( dbh => $dbh ) ;

=cut

sub BUILD {
    my $self = shift;

    $self->_set_db_type( $self->{dbh}->{Driver}->{Name} );
    die("Valid Database connection required") if ( !$self->_db_type() );

    # if we are using sqlite then we need to set a pragma to allow
    # cascading deletes on FOREIGN keys
    if ( $self->_db_type() eq 'SQLite' ) {
        $self->{dbh}->do("PRAGMA foreign_keys = ON");
    }

    # ensue we have the tables created (if wanted)
    $self->_create_tables() if( !$self->skip_table_check);

    # get the first list of queues we have
    $self->list_queues();
}

# -----------------------------------------------------------------------------
## class private variables
# -----------------------------------------------------------------------------

has _queue_list => (
    is       => 'rwp',               # like ro, but creates _set_queue_list too
    lazy     => 1,
    default  => sub { {} },
    writer   => '_set_queue_list',
    init_arg => undef                # dont allow setting in constructor ;
);

has _db_type => (
    is       => 'rwp',               # like ro, but creates _set_queue_list too
    lazy     => 1,
    default  => sub {''},
    writer   => '_set_db_type',
    init_arg => undef                # dont allow setting in constructor ;
);

has _processor => (
    is       => 'ro',
    lazy     => 1,
    default  => sub { my $hostname = `hostname`; $hostname =~ s/\s//g; $hostname . "::$ENV{USER}" . "::" . $$ },
    init_arg => undef                                                                                              # dont allow setting in constructor ;
);

# -----------------------------------------------------------------------------
## class private methods
# -----------------------------------------------------------------------------

sub _debug {
    my $self = shift;

    return if ( !$self->{debug} );

    my $msg = shift;
    $msg =~ s/^/    /gsm;

    say STDERR $msg;
}

# -----------------------------------------------------------------------------
sub _build_sql_stmt {
    my ( $query, $p ) = @_;
    our @params = $p ? @$p : ();
    $query =~ s/\s+$//;
    $query .= ' ;' if ( $query !~ /;$/ );

    # make sure we repesent NULL properly, do quoting - only basic its only for debug
    our $i = 0;
    {

        sub _repl {
            my $out = 'NULL';

            # quote strings, leave numbers untouched, not doing floats
            if ( defined $params[$i] ) {
                $out = $params[$i] =~ /^\d+$/ ? $params[$i] : "'$params[$i]'";
            }
            $i++;

            return $out;
        }
        $query =~ s/\?/_repl/gex if ( @params && scalar(@params) );
    }

    return $query;
}

# -----------------------------------------------------------------------------
sub _query_db {
    state $sth_map = {};
    my $self = shift;
    my ( $query, $p, $no_results ) = @_;
    my @params = $p ? @$p : ();
    my %result;

    $query =~ s/\s+$//;
    $query .= ' ;' if ( $query !~ /;$/ );

    if ( $self->{debug} ) {

        $self->_debug( "ACTUAL QUERY: $query\nQUERY PARAMS: " . to_json(@params) );
        my $sql = _build_sql_stmt( $query, $p );
        $self->_debug( 'BUILT QUERY : ' . $sql . "\n" );
    }

    try {
        my $sth;

        # key based on query and fields we are using
        my $key = "$query." . join( '.', @params );
        if ( $sth_map->{$key} ) {
            $sth = $sth_map->{$key};
        }
        else {

            $sth = $self->{dbh}->prepare($query);

            # save the handle for next time
            $sth_map->{$key} = $sth;

        }
        my $rv = $sth->execute(@params);
        if ( !$no_results ) {

            # so as to get an array of hashes
            $result{rows}      = $sth->fetchall_arrayref( {} );
            $result{row_count} = scalar( @{ $result{rows} } );
            $result{success}   = 1;

            $self->_debug( 'QUERY RESPONSE: ' . to_json( $result{rows} ) . "\n" );
        }
        else {
            if ($rv) {
                $result{row_count} = $sth->rows;
                $result{success}   = 1;
            }
        }

    }
    catch {
        $result{error} = "Failed to prepare/execute query: $query\nparams: " . to_json($p) . "\nerror: $@\n";

        # $self->_debug( $result{error} );
    };
    return \%result;
}

# -----------------------------------------------------------------------------
sub _update_db {
    my $self = shift;
    my ( $table, $query, $params ) = @_;

    $query = "UPDATE $table $query";

    my $resp = $self->_query_db( $query, $params, 1 );

    return $resp;
}

# -----------------------------------------------------------------------------
# we will hold onto statement handles to speed up inserts

sub _insert_db {
    state $sth_map = {};
    my $self = shift;
    my ( $table, $f, $p ) = @_;
    my @params = $p ? @$p : ();

    # key based on table and fields we are inserting
    my $key = "$table." . join( '.', @$f );
    my ( $query, $sql, $sth );

    if ( $sth_map->{$key} ) {
        $sth = $sth_map->{$key};
    }
    else {
        $query = "INSERT INTO $table (" . join( ',', @$f ) . ") values (" . join( ',', map {'?'} @$f ) . ") ;";

        $self->_debug($query);
        $sth = $self->{dbh}->prepare($query);

        # cache the handle for next time
        $sth_map->{$key} = $sth;
    }
    my $rv = $sth->execute(@params);

    return { row_count => $rv, error => 0 };
}

# -----------------------------------------------------------------------------

sub _delete_db_record {
    my $self = shift;
    my ( $table, $q, $v ) = @_;
    my $query = "DELETE FROM $table $q ;";

    # run the delte and don't fetch results
    my $resp = $self->_query_db( $query, $v, 1 );
    return $resp;
}

# -----------------------------------------------------------------------------
# as all the indexes are constructued the same, lets have a helper
sub _create_index_str {
    my ( $table, $field ) = @_;

    return sprintf( "CREATE INDEX %s_%s_idx on %s(%s) ;", $table, $field, $table, $field );
}

# -----------------------------------------------------------------------------
sub _create_sqlite_table {
    my $self  = shift;
    my $table = shift;
    $self->_debug("Creating SQLite tables");

    my $sql = "CREATE TABLE $table (
        counter         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        id              VARCHAR(128) NOT NULL UNIQUE,
        queue_name      VARCHAR(128) NOT NULL,
        added           TIMESTAMP DEFAULT current_timestamp,
        processed       BOOLEAN DEFAULT 0,
        processor       VARCHAR(128),
        process_start   TIMESTAMP,
        processing_time FLOAT,
        process_failure SMALLINT DEFAULT 0,
        data            TEXT   ) ;";

    $self->_debug($sql);
    try { $self->{dbh}->do($sql); } catch {};
}

# -----------------------------------------------------------------------------
sub _create_postgres_table {
    my $self  = shift;
    my $table = shift;
    $self->_debug("Creating PostgreSQL tables");

    # big/serial creates an auto incrementing column in PostgreSQL
    my $sql = "CREATE TABLE $table (
        counter         BIGSERIAL PRIMARY KEY UNIQUE,
        id              VARCHAR(128) NOT NULL UNIQUE,
        queue_name      VARCHAR(128) NOT NULL,
        added           TIMESTAMP WITH TIME ZONE DEFAULT now(),
        processed       SMALLINT DEFAULT 0,
        processor       VARCHAR(128),
        process_start   TIMESTAMP,
        processing_time FLOAT,
        process_failure SMALLINT DEFAULT 0,
        data            TEXT  ) ;";

    $self->_debug($sql);
    try { $self->{dbh}->do($sql); } catch {};
}

# -----------------------------------------------------------------------------
sub _create_mysql_table {
    my $self  = shift;
    my $table = shift;
    $self->_debug("Creating MySQL tables");

    my $sql = "CREATE TABLE $table (
        counter         INT NOT NULL PRIMARY KEY AUTO_INCREMENT UNIQUE,
        id              VARCHAR(128) NOT NULL UNIQUE,
        queue_name      VARCHAR(128) NOT NULL,
        added           TIMESTAMP DEFAULT current_timestamp,
        processed       SMALLINT DEFAULT 0,
        processor       VARCHAR(128),
        process_start   TIMESTAMP,
        processing_time FLOAT,
        process_failure SMALLINT DEFAULT 0,
        data            TEXT  ) ;";

    $self->_debug($sql);
    try { $self->{dbh}->do($sql); } catch {};

}

# -----------------------------------------------------------------------------
# create all the tables and indexes
sub _create_tables {
    my $self = shift;
    my $sql;
    my $table = $self->{prefix} . '_queue';

    # as the checking for tables and indexes is fraught with issues over multiple
    # databases its easier to not print the errors and catch the creation failures
    # and ignore them!
    my $p = $self->{dbh}->{PrintError};
    $self->{dbh}->{PrintError} = 0;

    # I am assuming either table does not exist then nor does the other and we should
    # create both
    if ( $self->_db_type() eq 'SQLite' ) {
        $self->_create_sqlite_table($table);
    }
    elsif ( $self->_db_type() eq 'Pg' ) {
        $self->_create_postgres_table($table);
    }
    elsif ( $self->_db_type() eq 'mysql' ) {
        $self->_create_mysql_table($table);
    }
    else {
        die "Unhandled database type " . $self->_db_type();
    }

    foreach my $field (qw/counter id added queue_name processed process_failure/) {
        my $sql = _create_index_str( $table, $field );

        $self->_debug($sql);
        try { $self->{dbh}->do($sql); } catch {};
    }

    # restore the PrintError setting
    $self->{dbh}->{PrintError} = $p;
}

# -----------------------------------------------------------------------------

=head2 add

Add some data into a named queue. 

    my $queue = App::Basis::Queue->new( dbh => $dbh) ;
     
    # save some application audit data
    $queue->add( 'app_start', {
        ip => 12.12.12.12, session_id => 12324324345, client_id => 248296432984,
        appid => 2, app_name => 'twitter'
    }) ;

=cut

sub add {
    state $uuid = Data::UUID->new();
    my $self  = shift;
    my $qname = shift;
    my $data  = @_ % 2 ? shift : {@_};

    if ( ref($data) ne 'HASH' ) {
        die "add accepts a hash or a hashref of parameters";
    }
    my $status = 0;
    my $resp;
    if ( !$qname || !$data ) {
        my $err = "Missing queue name or data";
        $self->_debug($err);
        warn $err;
        return $status;
    }

    try {
        my $json_str = encode_json($data);

        # we manage the id's for the queue entries as we cannot depend on a common SQL method of adding
        # a record and getting its uniq ID back
        #my $uuid       = Data::UUID->new();
        my $message_id = $uuid->create_b64();
        $resp = $self->_insert_db(
            $self->{prefix} . '_queue',
            [qw(id queue_name added data)],
            [ $message_id, $qname, strftime( "%Y-%m-%d %H:%M:%S", localtime() ), $json_str ]
        );

        $status = $message_id if ( !$resp->{error} );
    }
    catch {
        my $e = $@;
        warn $e;
    };

    return $status;
}

# -----------------------------------------------------------------------------

=head2 process

process up to 100 from the queue

a refrence to the queue object is passed to the callback along with the name of the queue
and the record that is to be procssed.

If the callback returns a non-zero value then the record will be marked as processed.
If the callback returns a zero value, then the processing is assumed to have failed and the
failure count will be incremented by 1. If the failue count matches our maximum
allowed limit then the item will not be available for any further processing.

    sub processing_callback {
        my ( $queue, $qname, $record ) = @_;

        return 1;
    }

    $queue->process( 'queue_name', count => 5, callback => \&processing_callback) ;

=cut

sub process {
    my $self            = shift;
    my $qname           = shift;
    my $params          = @_ % 2 ? die "method: process - Odd number of values passed where even is expected.\n" : {@_};
    my $processed_count = 0;

    # update queue list
    $self->list_queues();

    # if the queue does not exist
    return 0 if ( !$self->{_queue_list}->{$qname} );

    $params->{count} ||= 1;
    die __PACKAGE__ . " process requires a callback function" if ( !$params->{callback} || ref( $params->{callback} ) ne 'CODE' );

    if ( $params->{count} > $MAX_PROCESS_ITEMS ) {
        warn "Reducing process count from $params->{count} to $MAX_PROCESS_ITEMS";
        $params->{count} = $MAX_PROCESS_ITEMS;
    }

    # get list of IDs we can process, as SQLite has an issue with ORDER BY and LIMIT in an UPDATE call
    # so we have to do things in 2 stages, which means it is not easy to mark lots of records to be processed
    # but that its possibly a good thing
    my $sql = sprintf(
        "SELECT id FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0
            AND process_failure = 0  
            ORDER BY added ASC 
            LIMIT ?;", $self->{prefix}
    );
    my $ids = $self->_query_db( $sql, [ $qname, $params->{count} ] );
    my @t;
    foreach my $row ( @{ $ids->{rows} } ) {
        push @t, "'$row->{id}'";
    }

    # if there are no items to update, return
    return $processed_count if ( !scalar(@t) );
    my $id_list = join( ',', @t );

    # mark items that I am going to process
    my $update = "SET processor=?
            WHERE id IN ( $id_list) AND processed = 0 ;";
    my $resp = $self->_update_db( $self->{prefix} . "_queue", $update, [ $self->_processor() ] );
    return $processed_count if ( !$resp->{row_count} );

    # refetch the list to find out which ones we are going to process, incase another system was doing things
    # at the same time
    $sql = sprintf(
        "SELECT * FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0
            AND processor = ?
            AND process_failure = 0
            ORDER BY added ASC 
            LIMIT ?;", $self->{prefix}
    );
    my $info = $self->_query_db( $sql, [ $qname, $self->_processor(), $params->{count} ] );

    foreach my $row ( @{ $info->{rows} } ) {

        # unpack the data
        $row->{data} = decode_json( $row->{data} );
        my $state   = 0;
        my $start   = strftime( "%Y-%m-%d %H:%M:%S", localtime() );
        my $st = [gettimeofday] ;
        my $invalid = 0;
        my $elapsed ;
        try {
            $state = $params->{callback}->( $self, $qname, $row );
        }
        catch {
            warn "invalid callback $@";
            $invalid++;
        };
        $elapsed = tv_interval ( $st);

        if ($invalid) {

            # if the callback was invalid then we should not mark this as a process failure
            # just clear the processor
            $update = "SET processor=?, WHERE id = ? AND processed = 0 ;";
            $info = $self->_update_db( $self->{prefix} . "_queue", $update, [ '', $row->{id} ] );
        }
        elsif ($state) {
            # show we have processed it
            $update = "SET processed=1, process_start=?, processing_time=?  WHERE id = ? AND processed = 0 ;";
            $info = $self->_update_db( $self->{prefix} . "_queue", $update, [ $start, $elapsed, $row->{id} ] );
            $processed_count++;
        }
        else {
            # mark the failure
            $update = "SET process_failure=1, processing_time=? WHERE id = ? AND processed = 0 ;";
            $info = $self->_update_db( $self->{prefix} . "_queue", $update, [ $elapsed, $row->{id} ] );
        }
    }

    return $processed_count;
}

# -----------------------------------------------------------------------------

=head2 process_failures

process up to 100 from the queue
a refrence to the queue object is passed to the callback along with the name of the queue
and the record that is to be procssed. As these are failures we are not interested
in an value of the callback function.

    sub processing_failure_callback {
        my ( $queue, $qname, $record ) = @_;

        # items before 2013 were completely wrong so we can delete
        if( $record->{added} < '2013-01-01') {
            $queue->delete_record( $record) ;
        } else {
            # failures in 2013 was down to a bad processing function
            $queue->reset_record( $record) ;
        }
    }

    $queue->process( 'queue_name', count => 5, callback => \&processing_callback) ;

=cut

sub process_failures {
    my $self            = shift;
    my $qname           = shift;
    my $params          = @_ % 2 ? die "method: process - Odd number of values passed where even is expected.\n" : {@_};
    my $processed_count = 0;

    # update queue list
    $self->list_queues();

    # if the queue does not exist
    return 0 if ( !$self->{_queue_list}->{$qname} );

    $params->{count} ||= 1;
    die __PACKAGE__ . " process requires a callback function" if ( !$params->{callback} || ref( $params->{callback} ) ne 'CODE' );

    if ( $params->{count} > $MAX_PROCESS_ITEMS ) {
        warn "Reducing process count from $params->{count} to $MAX_PROCESS_ITEMS";
        $params->{count} = $MAX_PROCESS_ITEMS;
    }

    # get list of IDs we can process
    my $sql = sprintf(
        "SELECT id FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0
            AND process_failure = 1 
            ORDER BY added ASC 
            LIMIT ?;", $self->{prefix}
    );
    my $ids = $self->_query_db( $sql, [ $qname, $params->{count} ] );
    my @t;
    foreach my $row ( @{ $ids->{rows} } ) {
        push @t, "'$row->{id}'";
    }

    # if there are no items to update, return
    return $processed_count if ( !scalar(@t) );
    my $id_list = join( ',', @t );

    # mark items that I am going to process
    my $update = "SET processor=?
            WHERE id IN ( $id_list) AND processed = 0 ;";
    my $resp = $self->_update_db( $self->{prefix} . "_queue", $update, [ $self->_processor() ] );
    return $processed_count if ( !$resp->{row_count} );

    # refetch the list to find out which ones we are going to process, incase another system was doing things
    # at the same time
    $sql = sprintf(
        "SELECT * FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0
            AND processor = ?
            AND process_failure = 1 
            ORDER BY added ASC 
            LIMIT ?;", $self->{prefix}
    );
    my $info = $self->_query_db( $sql, [ $qname, $self->_processor(), $params->{count} ] );

    foreach my $row ( @{ $info->{rows} } ) {

        # unpack the data
        $row->{data} = decode_json( $row->{data} );

        my $state = 0;
        try {
            $state = $params->{callback}->( $self, $qname, $row );
        }
        catch {
            warn "invalid callback $@";
        };

        # we don't do anything else with the record, we assume that the callback
        # function will have done something like delete it or re-write it
    }

    return $processed_count;
}

# -----------------------------------------------------------------------------

=head2 queue_size

get the count of unprocessed items in the queue

    my $count = $queue->queue_size( 'queue_name') ;
    say "there are $count unprocessed items in the queue" ;

=cut

sub queue_size {
    my $self  = shift;
    my $qname = shift;

    # # update queue list
    # $self->list_queues();

    # # if the queue does not exist then it must be empty!
    # return 0 if ( !$self->{_queue_list}->{$qname} );

    my $sql = sprintf(
        "SELECT count(*) as count FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0
            AND process_failure = 0 ;", $self->{prefix}
    );
    my $resp = $self->_query_db( $sql, [ $qname ] );

    return $resp->{row_count} ? $resp->{rows}->[0]->{count} : 0;
}

# -----------------------------------------------------------------------------

=head2 list_queues

obtains a list of all the queues used by this database

    my $qlist = $queue->list_queues() ;
    foreach my $q (@$qlist) {
        say $q ;
    }

=cut

sub list_queues {
    my $self = shift;
    my %ques;

    my $result = $self->_query_db( sprintf( 'SELECT DISTINCT queue_name FROM %s_queue;', $self->{prefix} ) );

    if ( !$result->{error} ) {
        %ques = map { $_->{queue_name} => 1 } @{ $result->{rows} };
    }

    $self->_set_queue_list( \%ques );

    return [ keys %ques ];
}

# -----------------------------------------------------------------------------

=head2 stats

obtains stats about the data in the queue, this may be time/processor intensive
so use with care!

provides counts of unprocessed, processed, failures
max process_failure, avg process_failure, earliest_added, latest_added,
min_data_size, max_data_size, avg_data_size, total_records
avg_elapsed, max_elapsed, min_elapsed

    my $stats = $queue->stats( 'queue_name') ;
    say "processed $stats->{processed}, failures $stats->{failure}, unprocessed $stats->{unprocessed}" ;

=cut

sub stats {
    my $self      = shift;
    my $qname     = shift;
    my %all_stats = ();

    # update queue list
    $self->list_queues();

    # queue_size also calls list_queues, so we don't need to do it!
    $all_stats{unprocessed} = $self->queue_size($qname);

    # if the queue does not exist then it must be empty!
    if ( $self->{_queue_list}->{$qname} ) {

        $all_stats{unprocessed} = $self->queue_size($qname);

        my $sql = sprintf(
            "SELECT count(*) as count FROM %s_queue
            WHERE queue_name = ?
            AND processed = 1 ;", $self->{prefix}
        );
        my $resp = $self->_query_db( $sql, [$qname] );
        $all_stats{processed} = $resp->{rows}->[0]->{count};

        $sql = sprintf(
            "SELECT count(*) as count FROM %s_queue
            WHERE queue_name = ?
            AND processed = 0 
            AND process_failure = 1 ;", $self->{prefix}
        );
        $resp = $self->_query_db( $sql, [ $qname] );
        $all_stats{failures} = $resp->{rows}->[0]->{count};

        $sql = sprintf(
            "SELECT 
                min(process_failure) as min_process_failure, 
                max(process_failure) as max_process_failure,
                avg(process_failure) as avg_process_failure,
                min(added) as earliest_added,
                max(added) as latest_added, 
                min( length(data)) as min_data_size,
                max( length(data)) as max_data_size,
                avg( length(data)) as avg_data_size,
                min( processing_time) as min_elapsed,
                max( processing_time) as max_elapsed,      
                avg( processing_time) as avg_elapsed
            FROM %s_queue 
            WHERE queue_name = ?;", $self->{prefix}
        );
        $resp = $self->_query_db( $sql, [$qname] );

        foreach my $k ( keys %{ $resp->{rows}->[0] } ) {
            $all_stats{$k} = $resp->{rows}->[0]->{$k} || "0";
        }

        # number of records in the table
        $all_stats{total_records} = $all_stats{processed} + $all_stats{unprocessed} + $all_stats{failures};
        $all_stats{total_records} ||= '0';

    }

    # make sure hse things have a zero value so calculations don't fail
    foreach my $f (
        qw( unprocessed  processed  failures
        max process_failure avg process_failure  earliest_added latest_added
        min_data_size  max_data_size avg_data_size total_records
        total_records min_proc max_proc avg_proc)
        )
    {
        $all_stats{$f} ||= '0';
    }

    return \%all_stats;
}

# -----------------------------------------------------------------------------

=head2 delete_record

delete a single record from the queue
requires a data record which contains infomation we will use to determine the record

may be used in processing callback functions

    sub processing_callback {
        my ( $queue, $qname, $record ) = @_;

        # lets remove records before 2013
        if( $record->{added) < '2013-01-01') {
            $queue->delete_record( $record) ;
        }
        return 1 ;
    }

=cut

sub delete_record {
    my $self = shift;
    my $data = shift;

    my $sql = sprintf( "WHERE id = ? AND queue_name = ?", $self->{prefix} );
    my $resp = $self->_delete_db_record( $self->{prefix} . "_queue", $sql, [ $data->{id}, $data->{queue_name} ] );

    return $resp->{row_count};
}

# -----------------------------------------------------------------------------

=head2 reset_record

clear failure flag from a failed record
requires a data record which contains infomation we will use to determine the record

may be used in processing callback functions

    sub processing_callback {
        my ( $queue, $qname, $record ) = @_;

        # allow partially failed (and failed) records to be processed
        if( $record->{process_failure) {
            $queue->reset_record( $record) ;
        }
        return 1 ;
    }

=cut

sub reset_record {
    my $self = shift;
    my $data = shift;

    my $sql = "SET process_failure=0 WHERE id = ? AND queue_name = ? AND processed=0 AND process_failure > 0";
    my $resp = $self->_update_db( $self->{prefix} . "_queue", $sql, [ $data->{id}, $data->{queue_name} ] );

    return $resp->{row_count};
}

# -----------------------------------------------------------------------------

=head2 purge_queue

purge will remove all processed items and failures (process_failure >= 5).
These are completely removed from the database

    my $before = $queue->stats( 'queue_name') ;
    $queue->purge_queue( 'queue_name') ;
    my $after = $queue->stats( 'queue_name') ;

    say "removed " .( $before->{total_records} - $after->{total_records}) ;

=cut

sub purge_queue {
    my $self  = shift;
    my $qname = shift;

    # # update queue list
    # $self->list_queues();

    # # if the queue does not exist then we cannot purge it!
    # return 0 if ( !$self->{_queue_list}->{$qname} );

    my $sql = "WHERE processed=1 OR process_failure = 1";
    my $resp = $self->_delete_db_record( $self->{prefix} . "_queue", $sql );

    return $resp->{row_count};
}

# -----------------------------------------------------------------------------

=head2 remove_queue

remove a queue and all of its records

    $queue->remove_queue( 'queue_name') ;
    my $after = $queue->list_queues() ;
    # convert list into a hash for easier checking
    my %a = map { $_ => 1} @after ;
    say "queue removed" if( !$q->{queue_name}) ; 

=cut

sub remove_queue {
    my $self  = shift;
    my $qname = shift;

    # # update queue list
    # $self->list_queues();

    # # if the queue does not exist
    # return 0 if ( !$self->{_queue_list}->{$qname} );

    my $resp = $self->_delete_db_record( $self->{prefix} . "_queue", "WHERE queue_name=?", [$qname] );
    return $resp->{success};
}

# -----------------------------------------------------------------------------

=head2 reset_failures

clear any process_failure values from all unprocessed items

    my $before = $queue->stats( 'queue_name') ;
    $queue->reset_failures( 'queue_name') ;
    my $after = $queue->stats( 'queue_name') ;

    say "reset " .( $after->{unprocessed} - $before->{unprocessed}) ;

=cut

sub reset_failures {
    my $self  = shift;
    my $qname = shift;

    # # update queue list
    # $self->list_queues();

    # # if the queue does not exist
    # return 0 if ( !$self->{_queue_list}->{$qname} );

    my $sql = "SET process_failure=0";
    $sql .= sprintf( " WHERE queue_name = ? AND process_failure = 1", $self->{prefix} );
    my $resp = $self->_update_db( $self->{prefix} . "_queue", $sql, [ $qname ] );

    return $resp->{row_count} ? $resp->{row_count} : 0 ;
}

# -----------------------------------------------------------------------------

=head2 remove_failues

permanently delete failures from the database

    $queue->remove_failues( 'queue_name') ;
    my $stats = $queue->stats( 'queue_name') ;
    say "failues left " .( $stats->{failures}) ;

=cut

sub remove_failues {
    my $self  = shift;
    my $qname = shift;

    my $sql = sprintf( "WHERE process_failure = 1", $self->{prefix} );
    my $resp = $self->_delete_db_record( $self->{prefix} . "_queue", $sql );

    return $resp->{row_count};
}

# -----------------------------------------------------------------------------

=head2 remove_tables

If you never need to use the database again, it can be completely removed

    $queue_>remove_tables() ;

=cut

sub remove_tables {
    my $self = shift;

    my $sql = sprintf( 'DROP TABLE %s_queue;', $self->{prefix} );
    $self->_debug($sql);
    $self->{dbh}->do($sql);
}

# -----------------------------------------------------------------------------

1;
