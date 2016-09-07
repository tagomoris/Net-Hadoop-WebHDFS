# NAME

Net::Hadoop::WebHDFS - Client library for Hadoop WebHDFS and HttpFs

# SYNOPSIS

    use Net::Hadoop::WebHDFS;
    my $client = Net::Hadoop::WebHDFS->new( host => 'hostname.local', port => 50070 );

    my $statusArrayRef = $client->list('/');

    my $contentData = $client->read('/data.txt');

    $client->create('/foo/bar/data.bin', $bindata);

# DESCRIPTION

This module supports WebHDFS v1 on Hadoop 1.x (and CDH4.0.0 or later), and HttpFs on Hadoop 2.x (and CDH4 or later).
WebHDFS/HttpFs has two authentication methods: pseudo authentication and Kerberos, but this module supports pseudo authentication only.

# METHODS

Net::Hadoop::WebHDFS class method and instance methods.

## CLASS METHODS

### `Net::Hadoop::WebHDFS->new( %args ) :Net::Hadoop::WebHDFS`

Creates and returns a new client instance with _%args_.
If you are using HttpFs, set _httpfs\_mode => 1_ and _port => 14000_.

_%args_ might be:

- host :Str = "namenode.local"
- port :Int = 50070
- standby\_host :Str = "standby.namenode.local"
- standby\_port :Int = 50070
- username :Str = "hadoop"
- doas :Str = "hdfs"
- httpfs\_mode :Bool = 0/1

## INSTANCE METHODS

### `$client->create($path, $body, %options) :Bool`

Creates file on HDFS with _$body_ data. If you want to create blank file, pass blank string.

_%options_ might be:

- overwrite :Str = "true" or "false"
- blocksize :Int
- replication :Int
- permission :Str = "0600"
- buffersize :Int

### `$client->append($path, $body, %options) :Bool`

Append _$body_ data to _$path_ file.

_%options_ might be:

- buffersize :Int

### `$client->read($path, %options) :Str`

Open file of _$path_ and returns its content. Alias: **open**.

_%options_ might be:

- offset :Int
- length :Int
- buffersize :Int

### `$client->mkdir($path, [permission => '0644']) :Bool`

Make directory _$path_ on HDFS. Alias: **mkdirs**.

### `$client->rename($path, $dest) :Bool`

Rename file or directory as _$dest_.

### `$client->delete($path, [recursive => 0/1]) :Bool`

Delete file _$path_ from HDFS. With optional _recursive => 1_, files and directories are removed recursively (default false).

### `$client->stat($path) :HashRef`

Get and returns file status object for _$path_. Alias: **getfilestatus**.

### `$client->list($path) :ArrayRef`

Get list of files in directory _$path_, and returns these status objects arrayref. Alias: **liststatus**.

### `$client->content_summary($path) :HashRef`

Get 'content summary' object and returns it. Alias: **getcontentsummary**.

### `$client->checksum($path) :HashRef`

Get checksum information object for _$path_. Alias: **getfilechecksum**.

### `$client->homedir() :Str`

Get accessing user's home directory path. Alias: **gethomedirectory**.

### `$client->chmod($path, $mode) :Bool`

Set permission of _$path_ as octal _$mode_. Alias: **setpermission**.

### `$client->chown($path, [owner => 'username', group => 'groupname']) :Bool`

Set owner or group of _$path_. One of owner/group must be specified. Alias: **setowner**.

### `$client->replication($path, $replnum) :Bool`

Set replica number for _$path_. Alias: **setreplication**.

### `$client->touch($path, [modificationtime => $mtime, accesstime => $atime]) :Bool`

Set mtime/atime of _$path_. Alias: **settimes**.

### `$client->touchz($path) :Bool`

Create a zero length file.

## EXTENSIONS

### `$client->exists($path) :Bool`

Returns the `stat()` hash if successful, and false otherwise. Dies on
interface errors.

### `$client->find($path, $callback, $options_hash)`

Loops recursively over the specified path:

    $client->find(
        '/user',
        sub {
            my($cwd, $path) = @_;
            my $date = localtime $path->{modificationTime};
            my $type = $path->{type} eq q{DIRECTORY} ? "[dir ]" : "[file]";
            my $size = sprintf "% 10s",
                                $path->{blockSize}
                                    ? sprintf "%.2f MB", $path->{blockSize} / 1024**2
                                    : 0;
            print "$type $size $path->{permission} $path->{owner}:$path->{group} $cwd/$path->{pathSuffix}\n";
        },
        { # optional
            re_ignore => qr{
                            \A      # Filter some filenames out even before reaching the callback
                                [_] # logs and meta data, java junk, _SUCCESS files, etc.
                        }xms,
        }
    );

# AUTHOR

TAGOMORI Satoshi &lt;tagomoris {at} gmail.com>

# LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.
