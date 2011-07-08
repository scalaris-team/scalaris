#!/usr/bin/perl
use Frontier::Daemon;

#TODO secure this.
sub updateDNS {
    my $ip = @_[0];
    my $host = @_[0];
    $host =~ s/\./-/g;
    open(HOSTS, ">>/etc/hosts") || die("unable to open /etc/hosts: $!");
    print HOSTS $ip, " ", $host, "\n";
    close(HOSTS);
    
    open(PF, "</var/run/dnsmasq.pid") || 
      die("unable to open /var/run/dnsmasq.pid: $!");
    $pid = <PF>;
    close(PF);
    kill 1 => $pid;

    return "ok";
}


# Call me as http://localhost:8080/RPC2
$methods = {'updateDNS' => \&updateDNS};
Frontier::Daemon->new(LocalPort => 9999, methods => $methods)
    or die "Couldn't start HTTP server: $!";
