use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.


eval "use Bit::Vector";
if ($@) {
    print q{
The Bit::Vector module is required for installation of this module.
Missing modules are available from any CPAN mirror, for example

  ftp://ftp.funet.fi/pub/languages/perl/CPAN/modules/by-module

};
    exit 1;
}



WriteMakefile(
    'NAME'	=> 'DBIx::TextIndex',
    'VERSION_FROM' => 'TextIndex.pm', # finds $VERSION
);

