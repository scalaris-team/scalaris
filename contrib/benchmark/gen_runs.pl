#!/usr/bin/perl
use Template;
my @Servers = (1,2,5,10,15,20);
my $resdir = `pwd | sed 's/NFS3/NFS4/g'`;
chomp($resdir);
print $resdir;
foreach  my $s (@Servers) {
    my $tt = Template->new;
    $tt->process('bench_tt',  { server => $s , resdir => $resdir }, "bench_".$s."_run")
    		|| die $tt->error;
    
}
