require 5.004;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'	=> 'DBIx::TextIndex',
    'VERSION_FROM' => 'TextIndex.pm',
    'PREREQ_PM'    => {
                         'Bit::Vector'    => 5.3,
			 'DBI'            => 1.15,
			 'HTML::Entities' => 1.22,
                      },
    dist           => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
);

