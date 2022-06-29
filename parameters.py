import os

# Get the absolute path of the project directory. Assumes parameters.py is
# located in the project directory. Used to generate other paths used by code.
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# DATA is the location for storage of code output, including test data.
DATA = os.path.join(ROOT_DIR, "Data")
# TEMPORARY_DATA is the location large temporary files will be stored.
# Temporary files may take up over 50GB of disk space.
TEMPORARY_DATA = "/home/denis/Data/PyCharm Data"

"""
WIKIPEDIA_XML_DUMP_URL is a link to compressed Wikipedia XML dump. 

This is a dump of all raw text from all namespaces, as created by contributors, 
with minimal XML tags as wrappers. This text needs to be preprocessed by the 
wikipedia servers before being interpreted by web browsers to convert 
templates and markup into a browser-compatible format.

Dumps are started on the 1st and 20th of each month. They take several days 
to run, and are therefore not a 'snapshot' of the true state of the database 
at the time the trawl is started. The dump on the 1st is a complete trawl, 
and the dump on the 20th is built based on changes that have occurred since 
the 1st.

Links to the latest dumps can be found at:

https://dumps.wikimedia.org/backup-index.html
    
Look for the 'enwiki: Dump complete' link on the page, follow it, and then
the link you will want will be in the same format as below, with only the
dates changed. This was last confirmed on 2022-06-27.

The dump files are approximately 20 GB at this time, compressed in bz2 
format, and expand to about 80 GB if decompressed. The code in this 
repository does not decompress this file, but rather iterates through one 
compressed line at a time. The file is downloaded to TEMPORARY_DATA by default.

All testing of the code in this repository is based on the dump of 
2022-06-01. It is unclear how long wikipedia maintains links to dumps.
"""
WIKIPEDIA_XML_DUMP_URL = "https://dumps.wikimedia.org/enwiki/20220601/" \
                         "enwiki-20220601-pages-articles-multistream.xml.bz2"

# The directory that the downloaded corpus will be stored in.
RAW_DATA_DIR = os.path.join(
    TEMPORARY_DATA,
    "UVMR_Essential_Vocabulary", "Raw Data"
)
# The directory that intermediate files will be stored in. Do to the very
# large size of some data structures created in the process of converting raw
# data to a corpus, these data structures are managed as files on disk rather
# than as objects in memory.
INTERMEDIATE_DATA_DIR = os.path.join(
    TEMPORARY_DATA,
    "UVMR_Essential_Vocabulary", "Intermediate Data"
)

# Storage location for final compressed document files. These will consist of
# more than 100 bz2 compressed text files, each under 100 MB and with one
# document per line.
CORPUS_DIR = os.path.join(
    DATA, "Wikipedia Corpus"
)

# Storage location for test data files if generate_corpus is run in test mode.
# This will generate 3 text files, each containing data on 100 pages by
# default, though more can be specified.
TEST_DIR = os.path.join(
    DATA, "Test Data"
)

"""
DOCUMENT_TYPE must be one of "paragraph" or "sentence". If "paragraph", 
then the lines of text in the wikipedia dump are not chopped up into 
sentences, and each document consists of the original, processed, paragraph, 
and quotation marks are retained.

If "sentence" then the lines are split on periods, question marks, 
and exclamation marks. Quotation marks are not retained, as it is not 
intrinsically apparent which sentence they should be associated with.
"""
DOCUMENT_TYPE = "paragraph"
if DOCUMENT_TYPE not in ['sentence', 'paragraph', 'page']:
    raise ValueError(
        "DOCUMENT_TYPE must be either 'paragraph', 'sentence', or 'page'."
    )

"""
Special tokens to be inserted into generated documents are preceeded by 
'evm'. At this point these consist only of a garbage marker used in processing 
certain templates.
"""
PROCESS_GARBAGE_MARKER = 'evmprocessgarbagemarker'
