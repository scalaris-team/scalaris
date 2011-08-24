#!/usr/bin/perl

open(OUT, ">/tmp/hadoop-output");
print OUT "Launching hadoop...";
close(OUT);

#TODO fork
defined (my $pid = fork);
exit if $pid; 

system("hadoop fs -rmr pigout");
unlink ("/tmp/hadoop-output");
$ret = system("JAVA_HOME=/usr/lib/jvm/java-6-sun pig -x mapreduce -f /root/analyze_logs.pig -param LOGS=/access_logs/* > /tmp/hadoop-output 2>&1");
if($ret == 0) {
  unlink("/tmp/result.by_site");
  unlink("/tmp/result.by_date");
  system("hadoop fs -copyToLocal pigout/by_site/part-r-00000 /tmp/result.by_site");
  system("hadoop fs -copyToLocal pigout/by_date/part-r-00000 /tmp/result.by_date");

  $time = localtime;
  open(HTML, ">/var/lib/sc-manager/public/hadoop_result.html") or die "Can't open: $!";
  print HTML <<EOT;
  <html>
  <head>
    <title>Analyzed logfiles</title>
  </head>
  <body>
  <h1>Simple WebStats</h1>
  <h2>Most popular pages</h2>
  <table>
EOT
  open(DAT, "</tmp/result.by_site") or die "Can't open: $!";
  while(<DAT>) {
    ($page, $hits) = split('\t', $_, 2);
    chomp $hits;
    $page =~ s/%20/ /g;
    print HTML "<tr><td>$page</td><td>$hits</td></tr>\n";
  }
  close(DAT);
  print HTML <<EOT;
  </table>
  <h2>Hits per minute</h2>
  <img src="hits_per_minute.png">
  <hr>
  This page was generated at $time
  </body>
  </html>
EOT
  system("gnuplot /root/bydate.gnuplot");
}

