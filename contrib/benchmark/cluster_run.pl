#!/usr/bin/perl
if(@ARGV < 3) {
  print "usage: $0 [revision|HEAD] [tcp] Projectname \n";
  exit;
}
$rev = $ARGV[0];
$cl = $ARGV[1];
$name = $ARGV[2]."-".$cl."-".$rev."-".`date +%m%d%y%H%M%S`; 
chomp($name);
use Template;
my @Servers = (1,2,5,10,20);
my $resdir = `pwd`;
chomp($resdir);
$resdir.="/$name";
system "mkdir $resdir";
system "git clone https://github.com/scalaris-team/scalaris.git $resdir/scalaris-read-only && cd $resdir/scalaris-read-only/ && git checkout $rev";
#build scalairs
system "cd  $resdir/scalaris-read-only/ ; ./configure";
system "cd  $resdir/scalaris-read-only/ ; make";
system "cd  $resdir/scalaris-read-only/bin ; chmod u+x bench_master.sh ; chmod u+x bench_slave.sh " ;
my $runfile= $resdir."/qsub.sh";
open(RUNFILE,">$runfile");
$resdir =~ s/NFS3/NFS4/g ;
foreach  my $s (@Servers) {
    my $tt = Template->new;
    $tt->process('bench_tt',  { server => $s , resdir => $resdir, cl => $cl , scalaris => $resdir."/scalaris-read-only/" , name => $ARGV[2]."-".$cl."-".$rev },  $name."/bench_".$s."_run")
    		|| die $tt->error;
   print(RUNFILE "qsub bench_".$s."_run\n");
}

