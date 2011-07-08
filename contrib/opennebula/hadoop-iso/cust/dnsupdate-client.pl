#!/usr/bin/perl

use Frontier::Client;
my $argc = @ARGV;
($argc == 2) || die "usage: $0 master_ip ip";
  
my $host = $ARGV[0];
my $self_ip = $ARGV[1];


# Make an object to represent the XML-RPC server.
$server_url = "http://$host:9999/RPC2";
$server = Frontier::Client->new(url => $server_url);

# Call the remote server and get our result.
$result = $server->call('updateDNS', "$self_ip");
if ( $result == "ok" ) {
 exit 0;
}
