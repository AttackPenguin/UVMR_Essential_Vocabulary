import os

# Primary directories
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT_DIR, "Data")
# Temporary Dat is the location large temporary files will be stored.
# Temporary files may take up over 50GB of disk space.
TEMPORARY_DATA = "/home/denis/Data/PyCharm Data"

# Link to compressed Wikipedia XML dump. I cannot find documentation on how
# long older dumps remain accessible, so it is likely that users will need to
# update this link to generate a corpus.
# Links to the latest corpus can be found at
# https://dumps.wikimedia.org/backup-index.html
# Look for the 'enwiki: Dump complete' link on the page, follow it, and then
# the link you will want will be in the same format as below, with only the
# dates changed. This was last confirmed on 2022-06-16
WIKIPEDIA_XML_DUMP_URL = "https://dumps.wikimedia.org/enwiki/20220520/" \
                         "enwiki-20220520-pages-articles-multistream.xml.bz2"

# generate_corpus directories and values
# The directory that the downloaded corpus will be stored in, along with a
# temporary intermediate text file containing the full list of documents.
RAW_DATA_DIR = os.path.join(
    TEMPORARY_DATA,
    "UVMR_Essential_Vocabulary", "Raw Data"
)
# Storage location for final compressed document files. These will consist of
# about 150 38MB bz2 files, each containing 1,000,000 individual documents as
# space-separated strings.
CORPUS_DIR = os.path.join(
    DATA, "Wikipedia Corpus"
)
# Storage location for test data files if generate_corpus is run in test mode.
# This will generate 3 text files, each containing data on 100 pages,
# not including redirects.
TEST_DIR = os.path.join(
    DATA, "Test Data"
)
# A string to insert into documents during creation where there should be a
# symbol, but there is no easy way to determine the correct symbol. Maintains
# appropriate relative positions of other tokens.
PROCESS_GARBAGE_MARKER = 'processgarbagemarker'
