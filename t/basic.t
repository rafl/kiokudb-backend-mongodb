use strict;
use warnings;
use Test::More 'no_plan';
use KiokuDB;
use KiokuDB::Test;

BEGIN { use_ok('KiokuDB::Backend::MongoDB') }

my $mongo = KiokuDB::Backend::MongoDB->new(
    database_name   => 'kioku_database',
    collection_name => 'kioku_collection',
);

run_all_fixtures( KiokuDB->new(backend => $mongo) );
