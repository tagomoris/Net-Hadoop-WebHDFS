package Net::Hadoop::WebHDFS;

use strict;
use warnings;
use Carp;

use JSON::XS qw//;

use Furl;
use URI;

our $VERSION = "0.5";

our %OPT_TABLE = ();

sub new {
    my ($this, %opts) = @_;
    my $self = +{
        host => $opts{host} || 'localhost',
        port => $opts{port} || 50070,
        standby_host => $opts{standby_host},
        standby_port => ($opts{standby_port} || $opts{port} || 50070),
        httpfs_mode => $opts{httpfs_mode} || 0,
        username => $opts{username},
        doas => $opts{doas},
        useragent => $opts{useragent} || 'Furl Net::Hadoop::WebHDFS (perl)',
        timeout => $opts{timeout} || 10,
        under_failover => 0,
    };
    $self->{furl} = Furl::HTTP->new(agent => $self->{useragent}, timeout => $self->{timeout}, max_redirects => 0);
    return bless $self, $this;
}

# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
#                 [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
#                 [&permission=<OCTAL>][&buffersize=<INT>]"
sub create {
    my ($self, $path, $body, %options) = @_;
    if ($self->{httpfs_mode}) {
        %options = (%options, data => 'true');
    }
    my $err = $self->check_options('CREATE', %options);
    croak $err if $err;

    my $res = $self->operate_requests('PUT', $path, 'CREATE', \%options, $body);
    $res->{code} == 201;
}
$OPT_TABLE{CREATE} = ['overwrite', 'blocksize', 'replication', 'permission', 'buffersize', 'data'];

# curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
sub append {
    my ($self, $path, $body, %options) = @_;
    if ($self->{httpfs_mode}) {
        %options = (%options, data => 'true');
    }
    my $err = $self->check_options('APPEND', %options);
    croak $err if $err;

    my $res = $self->operate_requests('POST', $path, 'APPEND', \%options, $body);
    $res->{code} == 200;
}
$OPT_TABLE{APPEND} = ['buffersize', 'data'];

# curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
#                [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
sub read {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('OPEN', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', $path, 'OPEN', \%options);
    $res->{body};
}
$OPT_TABLE{OPEN} = ['offset', 'length', 'buffersize'];
sub open { (shift)->read(@_); }

# curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
sub mkdir {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('MKDIRS', %options);
    croak $err if $err;

    my $res = $self->operate_requests('PUT', $path, 'MKDIRS', \%options);
    $self->check_success_json($res, 'boolean');
}
$OPT_TABLE{MKDIRS} = ['permission'];
sub mkdirs { (shift)->mkdir(@_); }

# curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
sub rename {
    my ($self, $path, $dest, %options) = @_;
    my $err = $self->check_options('RENAME', %options);
    croak $err if $err;

    unless ($dest =~ m!^/!) {
        $dest = '/' . $dest;
    }
    my $res = $self->operate_requests('PUT', $path, 'RENAME', {%options, destination => $dest});
    $self->check_success_json($res, 'boolean');
}

# curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
#                          [&recursive=<true|false>]"
sub delete {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('DELETE', %options);
    croak $err if $err;

    my $res = $self->operate_requests('DELETE', $path, 'DELETE', \%options);
    $self->check_success_json($res, 'boolean');
}
$OPT_TABLE{DELETE} = ['recursive'];

# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
sub stat {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('GETFILESTATUS', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', $path, 'GETFILESTATUS', \%options);
    $self->check_success_json($res, 'FileStatus');
}
sub getfilestatus { (shift)->stat(@_); }

# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
sub list {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('LISTSTATUS', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', $path, 'LISTSTATUS', \%options);
    $self->check_success_json($res, 'FileStatuses')->{FileStatus};
}
sub liststatus { (shift)->list(@_); }

# curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
sub content_summary {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('GETCONTENTSUMMARY', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', $path, 'GETCONTENTSUMMARY', \%options);
    $self->check_success_json($res, 'ContentSummary');
}
sub getcontentsummary { (shift)->content_summary(@_); }

# curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
sub checksum {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('GETFILECHECKSUM', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', $path, 'GETFILECHECKSUM', \%options);
    $self->check_success_json($res, 'FileChecksum');
}
sub getfilechecksum { (shift)->checksum(@_); }

# curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
sub homedir {
    my ($self, %options) = @_;
    my $err = $self->check_options('GETHOMEDIRECTORY', %options);
    croak $err if $err;

    my $res = $self->operate_requests('GET', '/', 'GETHOMEDIRECTORY', \%options);
    $self->check_success_json($res, 'Path');
}
sub gethomedirectory { (shift)->homedir(@_); }

# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
#                 [&permission=<OCTAL>]"
sub chmod {
    my ($self, $path, $mode, %options) = @_;
    my $err = $self->check_options('SETPERMISSION', %options);
    croak $err if $err;

    my $res = $self->operate_requests('PUT', $path, 'SETPERMISSION', {%options, permission => $mode});
    $res->{code} == 200;
}
sub setpermission { (shift)->chmod(@_); }

# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
#                          [&owner=<USER>][&group=<GROUP>]"
sub chown {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('SETOWNER', %options);
    croak $err if $err;

    unless (defined($options{owner}) or defined($options{group})) {
        croak "'chown' needs at least one of owner or group";
    }

    my $res = $self->operate_requests('PUT', $path, 'SETOWNER', \%options);
    $res->{code} == 200;
}
$OPT_TABLE{SETOWNER} = ['owner', 'group'];
sub setowner { (shift)->chown(@_); }

# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
#                           [&replication=<SHORT>]"
sub replication {
    my ($self, $path, $replnum, %options) = @_;
    my $err = $self->check_options('SETREPLICATION', %options);
    croak $err if $err;

    my $res = $self->operate_requests('PUT', $path, 'SETREPLICATION', {%options, replication => $replnum});
    $self->check_success_json($res, 'boolean');
}
sub setreplication { (shift)->replication(@_); }

# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
#                           [&modificationtime=<TIME>][&accesstime=<TIME>]"
# motificationtime: radix-10 long integer
# accesstime: radix-10 long integer
$OPT_TABLE{SETTIMES} = [ qw( modificationtime accesstime ) ];
sub touch {
    my ($self, $path, %options) = @_;
    my $err = $self->check_options('SETTIMES', %options);
    croak $err if $err;

    unless (defined($options{modificationtime}) or defined($options{accesstime})) {
        croak "'touch' needs at least one of modificationtime or accesstime";
    }

    my $res = $self->operate_requests('PUT', $path, 'SETTIMES', \%options);
    $res->{code} == 200;
}
sub settimes { (shift)->touch(@_); }

# sub delegation_token {}
# sub renew_delegation_token {}
# sub cancel_delegation_token {}

sub check_options {
    my ($self, $op, %opts) = @_;
    my @ex = ();
    my $opts = $OPT_TABLE{$op} || [];
    foreach my $k (keys %opts) {
        push @ex, $k if scalar(grep {$k eq $_} @$opts) < 1;
    }
    return undef unless @ex;
    'no such option: ' . join(' ', @ex);
}

sub check_success_json {
    my ($self, $res, $attr) = @_;
    $res->{code} == 200 and $res->{content_type} =~ m!^application/json! and
        (not defined($attr) or JSON::XS::decode_json($res->{body})->{$attr});
}

sub api_path {
    my ($self, $path) = @_;
    return '/webhdfs/v1' . $path if $path =~ m!^/!;
    '/webhdfs/v1/' . $path;
}

sub build_path {
    my ($self, $path, $op, %params) = @_;
    my %opts = (('op' => $op),
                ($self->{username} ? ('user.name' => $self->{username}) : ()),
                ($self->{doas} ? ('doas' => $self->{doas}) : ()),
                %params);
    my $u = URI->new('', 'http');
    $u->query_form(%opts);
    $self->api_path($path) . $u->path_query; # path_query() #=> '?foo=1&bar=2'
}

sub connect_to {
    my $self = shift;
    if ($self->{under_failover}) {
        return ($self->{standby_host}, $self->{standby_port});
    }
    return ($self->{host}, $self->{port});
}

our %REDIRECTED_OPERATIONS = (APPEND => 1, CREATE => 1, OPEN => 1, GETFILECHECKSUM => 1);
sub operate_requests {
    my ($self, $method, $path, $op, $params, $payload) = @_;

    my ($host, $port) = $self->connect_to();

    my $headers = []; # or undef ?
    if ($self->{httpfs_mode} or not $REDIRECTED_OPERATIONS{$op}) {
        if ($self->{httpfs_mode} and defined($payload) and length($payload) > 0) {
            $headers = ['Content-Type' => 'application/octet-stream'];
        }
        return $self->request($host, $port, $method, $path, $op, $params, $payload, $headers);
    }

    # pattern for not httpfs and redirected by namenode
    my $res = $self->request($host, $port, $method, $path, $op, $params, undef);
    unless ($res->{code} >= 300 and $res->{code} <= 399 and $res->{location}) {
        my $code = $res->{code};
        my $body = $res->{body};
        croak "NameNode returns non-redirection (or without location header), code:$code, body:$body.";
    }
    my $uri = URI->new($res->{location});
    $headers = ['Content-Type' => 'application/octet-stream'];
    return $self->request($uri->host, $uri->port, $method, $uri->path_query, undef, {}, $payload, $headers);
}

# IllegalArgumentException      400 Bad Request
# UnsupportedOperationException 400 Bad Request
# SecurityException             401 Unauthorized
# IOException                   403 Forbidden
# FileNotFoundException         404 Not Found
# RumtimeException              500 Internal Server Error
sub request {
    my ($self, $host, $port, $method, $path, $op, $params, $payload, $header) = @_;

    my $request_path = $op ? $self->build_path($path, $op, %$params) : $path;
    my ($ver, $code, $msg, $headers, $body) = $self->{furl}->request(
        method => $method,
        host => $host,
        port => $port,
        path_query => $request_path,
        headers => $header,
        ($payload ? (content => $payload) : ()),
    );

    my $res = { code => $code, body => $body };

    for (my $i = 0; $i < scalar(@$headers); $i += 2) {
        my $header = $headers->[$i];
        my $value = $headers->[$i + 1];

        if ($header =~ m!^location$!i) { $res->{location} = $value; }
        elsif ($header =~ m!^content-type$!i) { $res->{content_type} = $value; }
    }

    return $res if $code >= 200 and $code <= 299;
    return $res if $code >= 300 and $code <= 399;

    my $errmsg = $res->{body} || 'Response body is empty...';
    $errmsg =~ s/\n//g;

    if ($code == 400) { croak "ClientError: $errmsg"; }
    elsif ($code == 401) { croak "SecurityError: $errmsg"; }
    elsif ($code == 403) {
        if ($errmsg =~ /org\.apache\.hadoop\.ipc\.StandbyException/) {
            if ($self->{httpfs_mode} || not defined($self->{standby_host})) {
                # failover is disabled
            } elsif ($self->{retrying}) {
                # more failover is prohibited
                $self->{retrying} = 0;
            } else {
                $self->{under_failover} = not $self->{under_failover};
                $self->{retrying} = 1;
                my ($next_host, $next_port) = $self->connect_to();
                my $val = $self->request($next_host, $next_port, $method, $path, $op, $params, $payload, $header);
                $self->{retrying} = 0;
                return $val;
            }
        }
        croak "IOError: $errmsg";
    }
    elsif ($code == 404) { croak "FileNotFoundError: $errmsg"; }
    elsif ($code == 500) { croak "ServerError: $errmsg"; }

    croak "RequestFailedError, code:$code, message:$errmsg";
}

1;

__END__

=head1 NAME

Net::Hadoop::WebHDFS - Client library for Hadoop WebHDFS and HttpFs

=head1 SYNOPSIS

  use Net::Hadoop::WebHDFS;
  my $client = Net::Hadoop::WebHDFS->new( host => 'hostname.local', port => 50070 );

  my $statusArrayRef = $client->list('/');

  my $contentData = $client->read('/data.txt');

  $client->create('/foo/bar/data.bin', $bindata);

=head1 DESCRIPTION

This module supports WebHDFS v1 on Hadoop 1.x (and CDH4.0.0 or later), and HttpFs on Hadoop 2.x (and CDH4 or later).
WebHDFS/HttpFs has two authentication methods: pseudo authentication and Kerberos, but this module supports pseudo authentication only.

=head1 METHODS

Net::Hadoop::WebHDFS class method and instance methods.

=head2 CLASS METHODS

=head3 C<< Net::Hadoop::WebHDFS->new( %args ) :Net::Hadoop::WebHDFS >>

Creates and returns a new client instance with I<%args>.
If you are using HttpFs, set I<httpfs_mode => 1> and I<port => 14000>.


I<%args> might be:

=over

=item host :Str = "namenode.local"

=item port :Int = 50070

=item standby_host :Str = "standby.namenode.local"

=item standby_port :Int = 50070

=item username :Str = "hadoop"

=item doas :Str = "hdfs"

=item httpfs_mode :Bool = 0/1

=back

=head2 INSTANCE METHODS

=head3 C<< $client->create($path, $body, %options) :Bool >>

Creates file on HDFS with I<$body> data. If you want to create blank file, pass blank string.

I<%options> might be:

=over

=item overwrite :Str = "true" or "false"

=item blocksize :Int

=item replication :Int

=item permission :Str = "0600"

=item buffersize :Int

=back

=head3 C<< $client->append($path, $body, %options) :Bool >>

Append I<$body> data to I<$path> file.

I<%options> might be:

=over

=item buffersize :Int

=back

=head3 C<< $client->read($path, %options) :Str >>

Open file of I<$path> and returns its content. Alias: B<open>.

I<%options> might be:

=over

=item offset :Int

=item length :Int

=item buffersize :Int

=back

=head3 C<< $client->mkdir($path, [permission => '0644']) :Bool >>

Make directory I<$path> on HDFS. Alias: B<mkdirs>.

=head3 C<< $client->rename($path, $dest) :Bool >>

Rename file or directory as I<$dest>.

=head3 C<< $client->delete($path, [recursive => 0/1]) :Bool >>

Delete file I<$path> from HDFS. With optional I<recursive => 1>, files and directories are removed recursively (default false).

=head3 C<< $client->stat($path) :HashRef >>

Get and returns file status object for I<$path>. Alias: B<getfilestatus>.

=head3 C<< $client->list($path) :ArrayRef >>

Get list of files in directory I<$path>, and returns these status objects arrayref. Alias: B<liststatus>.

=head3 C<< $client->content_summary($path) :HashRef >>

Get 'content summary' object and returns it. Alias: B<getcontentsummary>.

=head3 C<< $client->checksum($path) :HashRef >>

Get checksum information object for I<$path>. Alias: B<getfilechecksum>.

=head3 C<< $client->homedir() :Str >>

Get accessing user's home directory path. Alias: B<gethomedirectory>.

=head3 C<< $client->chmod($path, $mode) :Bool >>

Set permission of I<$path> as octal I<$mode>. Alias: B<setpermission>.

=head3 C<< $client->chown($path, [owner => 'username', group => 'groupname']) :Bool >>

Set owner or group of I<$path>. One of owner/group must be specified. Alias: B<setowner>.

=head3 C<< $client->replication($path, $replnum) :Bool >>

Set replica number for I<$path>. Alias: B<setreplication>.

=head3 C<< $client->touch($path, [modificationtime => $mtime, accesstime => $atime]) :Bool >>

Set mtime/atime of I<$path>. Alias: B<settimes>.

=head1 AUTHOR

TAGOMORI Satoshi E<lt>tagomoris {at} gmail.comE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
