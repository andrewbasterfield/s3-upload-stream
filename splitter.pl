#!/usr/bin/perl

my $max_blocksize = 4096;
my $max_filesize = 4096*3;

use strict;
use warnings;
use File::Temp;

$SIG{__DIE__} = \&Carp::confess;
$SIG{__WARN__} = \&Carp::cluck;

use constant {
  INFO => "INFO",
  WARN => "WARN",
};

my $block;
my $file;
my $this_filesize = 0;
my $this_blocksize = 0;
binmode(STDIN);

my ($tmpfh,$filename) = getfh();

while ($this_blocksize = read(STDIN,$block,$max_blocksize)) {
  logger(INFO, "Read %d",$this_blocksize);
  logger(WARN, "Got error: %s", $!) if $!;
  
  if ($this_blocksize > 0 && ($this_filesize + $this_blocksize <= $max_filesize)) {
    #
    # Append to file
    #
    $tmpfh->write($block);
    $this_filesize += $this_blocksize;
  } else {
    #
    # Flush the file
    #
    process_file($tmpfh,$filename,$this_filesize);
    ($tmpfh,$filename) = getfh();
    $this_filesize = 0;
  }
}
process_file($tmpfh,$filename,$this_filesize) if $this_filesize;

sub getfh {
  my ($fh,$filename) = File::Temp::tempfile();
  binmode($fh);
  return ($fh,$filename);
}

sub process_file {
  my $fh = shift;
  my $filename = shift;
  my $filesize = shift;
  close($fh);
  unlink $filename;
  logger(INFO,"Flushing file %s of %d",$filename,$filesize);
}

sub logger {
  my $level = shift;
  my $format_string = shift;
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
  my $line = sprintf($format_string, @_);
  printf "%4d-%02d-%02d %02d:%02d:%02d (%d) %s: %s\n",$year+1900,$mon+1,$mday,$hour,$min,$sec,$$,$level,$line;
}
