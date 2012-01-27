#!/usr/bin/perl
use strict;
use warnings;
use Path::Class;
use lib glob file(__FILE__)->dir->parent->subdir ('modules', '*', 'lib')->stringify;
use Dongry::Database;
use Data::Dumper;
use Getopt::Long;

my $workerdb_dsn;
my $message;
my $CountThreshold = 100;
my $ThresholdByFuncname = {};
GetOptions(
    'count-threshold=s' => \$CountThreshold,
    'count-threshold-by-funcname=s' => sub {
        my ($funcname, $n) = split /=/, $_[1], 2;
        $ThresholdByFuncname->{$funcname} = $n || 0;
    },
    'dsn=s' => \$workerdb_dsn,
    'error-message=s' => \$message,
) or die "Usage: $0 --dsn=dsn --error-message=message\n";

my $db = Dongry::Database->new(
    sources => {
        default => {
            dsn => $workerdb_dsn,
        },
    },
);

my $reported = {};
$db->execute ('SELECT funcid, COUNT(*) AS count FROM job GROUP BY funcid')->each(sub {
    if ($_->{count} >= $CountThreshold) {
        $reported->{$_->{funcid}} = $_;
    }
});

if (keys %$reported) {
    $db->execute (
        'SELECT funcid, funcname FROM funcmap WHERE funcid IN (:funcid)',
        {funcid => [map { $_->{funcid} } values %$reported]},
    )->each(sub {
        $reported->{$_->{funcid}}->{funcname} = $_->{funcname};
    });
}

my @report = values %$reported;
for (@report) {
    my $n = $ThresholdByFuncname->{$_->{funcname}};
    if ($n and $_->{count} < $n) {
        delete $reported->{$_->{funcid}};
    }
}

$workerdb_dsn =~ s/\b(?:user|password)=[^;]*//g;

print Dumper {
    time => time,
    localtime => scalar localtime,
    workerdb => $workerdb_dsn,
    jobs => [sort { $b->{count} <=> $a->{count} } values %$reported],
    message => $message,
} if keys %$reported;

=head1 USAGE

  $ perl path/to/workerwatch.pl \
        --dsn "dsn:..." \
        --error-message "Too many worker jobs > id:wakabatan" \
        --count-threshold 200

=head1 AUTHOR

Wakaba (id:wakabatan) <wakabatan@hatena.ne.jp>.

=head1 LICENSE

Copyright 2012 Hatena <http://www.hatena.com/>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
