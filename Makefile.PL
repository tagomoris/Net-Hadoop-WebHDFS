use inc::Module::Install;
name 'Net-Hadoop-WebHDFS';
all_from 'lib/Net/Hadoop/WebHDFS.pm';

requires 'Try::Tiny';
requires 'JSON';
requires 'JSON::XS';

requires 'URI';
requires 'Furl';

requires 'Test::More';
requires 'Test::Deep';

tests 't/*.t';
test_requires 'Test::More';
test_requires 'Test::Deep';

auto_set_repository;

WriteAll;
