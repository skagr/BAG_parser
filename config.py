import locale

version = 23
version_date = '2 augustus 2022'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# Location of BAG zip file downloaded from kadaster. See readme.MD for instructions.
file_bag = 'input/bag.zip'

# Location of gemeenten file downloaded from cbs.nl. See readme.MD for instructions.
file_gemeenten = 'input/gemeenten.csv'

# output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# log file with progress, warnings and error messages. This info is also written to the console
file_log = 'output/bag_importer.log'

# The parser creates an addresses table. After that some BAG tables are no longer needed: nummers, panden,
# verblijfobjecten and ligplaatsen. You can also delete these afterwards using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = False

