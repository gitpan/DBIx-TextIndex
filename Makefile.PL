require 5.005;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'	=> 'DBIx::TextIndex',
    'VERSION_FROM' => 'lib/DBIx/TextIndex.pm',
    'PREREQ_PM'    => {
                         'Bit::Vector'      => 6.3,
			 'DBI'              => 1.15,
			 'HTML::Entities'   => 1.22,
			 'Exception::Class' => 1.12,
                         'Text::Balanced'   => 1.83,
			 'Text::Unaccent'   => 1.07,
                      },
    dist           => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
);

