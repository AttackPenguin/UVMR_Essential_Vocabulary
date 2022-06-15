import bz2
import os
import pickle
import random
import re
import requests
import typing

import pandas as pd

import parameters as p

DATA_DIR = os.path.join(p.DATA)

# This code was built and tested on the wikipedia dump of 2022-05-20
wikipedia_xml_dump_url = "https://dumps.wikimedia.org/enwiki/20220520/" \
                         "enwiki-20220520-pages-articles-multistream.xml.bz2"
# The input file and un-chunked output files will be stored in the
# raw_data_dir. Note that these files will likely both be greater than 20 gb
# in size.
raw_data_dir = "/home/denis/Data/PyCharm Data/" \
               "UVMR_Essential_Vocabulary/Raw Data"
# The chunked, compressed data files will be stored in output_dir. Note that
# these files are likely to total more than 5 gb.
output_dir = "/home/denis/PycharmProjects/" \
             "UVMR_Essential_Vocabulary/Data/Wikipedia Corpus"

# Define two translation maps for removing punctuation via str.translate,
# which is very efficient. The first will consist of punctuation to remove.
# The second will consist of punctuation around which to insert spaces,
# so that occurrences can be treated as tokens when splitting on white space.
# Periods are handles in the code, and are used to split documents.
punctuation_exclude = "!\"#$&'*+:<=>@[\\]^_`{|}~!?%"
translation_exclude = str.maketrans({
    char: '' for char in punctuation_exclude
})
punctuation_include = "-()/;,"
translation_include = str.maketrans({
    char: f' {char} ' for char in punctuation_include
})


def main():
    generate_corpus()


def generate_corpus(
        xml_url: str = wikipedia_xml_dump_url,
        raw_data_dir: str = raw_data_dir,
        output_dir: str = output_dir
):
    # Get data file if not already acquired.
    get_wikipedia_dump(xml_url, raw_data_dir)

    # Extract documents and export them to a text file, 1 line per document.
    # This approach preserves RAM.
    extract_documents(xml_url, raw_data_dir, output_dir)

    # Randomize documents in document text file and store them in manageable
    # "chunks" of 1,000,000 documents in compressed files. These files amount
    # to about 150 files of 38 gb each.
    # chunk_documents_to_files(
    #     "/media/denis/Data Backup/temp/working.txt",
    #     1_000_000
    # )


def get_wikipedia_dump(
        xml_url: str = wikipedia_xml_dump_url,
        raw_data_dir: str = raw_data_dir
):
    bz2_file_name = xml_url.rsplit('/', 1)[-1]
    bz2_file_path = os.path.join(raw_data_dir, bz2_file_name)

    # Check if xml_file has already been downloaded and extracted
    if os.path.exists(bz2_file_path):
        print("Compressed XML data file found in raw data directory...\n"
              "Download of compressed XML data file canceled.")
    else:
        # Download and extract wikipedia dump.
        print("Downloading compressed XML data file...")
        response = requests.get(xml_url)
        if not response:
            raise FileNotFoundError(
                f"Request for file at {xml_url} returned "
                f"{response.status_code}."
            )
        else:
            with bz2.BZ2File(bz2_file_path, 'wb') as out_file:
                out_file.write(response.content)
            print("Compressed XML data file successfully downloaded.")


def extract_documents(
        xml_url: str = wikipedia_xml_dump_url,
        raw_data_dir: str = raw_data_dir,
        output_dir: str = output_dir
) -> None:
    # Get path to input file.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    # Whenever a line is identified in the input xml file that looks like a
    # paragraph of potential input text, we will call process_paragraph,
    # and give it this file pointer to which to add documents found in the
    # paragraph.
    document_storage_file = open(
        os.path.join(raw_data_dir, "documents.txt"), 'a'
    )
    # Initialize some counters with which to report progress via stdout
    articles_extracted = 0
    documents_generated = 0
    # Used to prevent occasionally printing same line to stdout more than once
    last_article_count_out = 0

    print(f"Beginning extraction of documents from input file at "
          f"{pd.Timestamp.now().strftime('%H:%M:%S')}")

    with bz2.BZ2File(input_file_path, 'r') as file:
        print(f"Successfully accessed input file...\n")
        # We will look at each line of the wikipedia xml dump file, weeding
        # out lines that are not human-readable paragraphs of text.
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            # Strip leading whitespace - xml tags are variably indented for
            # readability
            line = line.lstrip()
            # Check to see if we've completed an article, and update console
            # if appropriate - and skip the line
            if line == "</page>\n":
                articles_extracted += 1
                if (
                        articles_extracted % 10_000 == 0 and
                        articles_extracted < last_article_count_out
                ):
                    print(f"Extracted Article {articles_extracted:,}: "
                          f"{documents_generated:,} Documents Generated.")
                    last_article_count_out = articles_extracted
                continue
            # Check for redirects. Redirects are also marked up with
            # <page></page>, but contain no lines that aren't filtered out.
            # Therefore, they can be used to avoid overcounting the number of
            # articles actually parsed.
            if line.startswith("<redirect title="):
                articles_extracted -= 1
            # Skip all very short lines - This solves an enormous number of
            # miscellaneous problems, and excludes very few text blocks of
            # interest.
            if len(line) < 100:
                continue
            # Skip lines starting and ending with two curly brackets
            if line.startswith('{{') and line.endswith('}}\n'):
                continue
            # Skip lines starting and ending with two (or more) equals signs
            if line.startswith('==') and line.endswith('==\n'):
                continue
            # Skip lines enclosed in carets
            if line.startswith('<') and line.endswith('>\n'):
                continue
            # Skip lines starting with "<text bytes="
            if line.startswith("<text bytes="):
                continue
            # Skip lines starting with "}}</text>"
            if line.startswith("}}</text>"):
                continue
            # Eliminate picture links and descriptions
            if line.startswith("[[File:"):
                continue
            # Remove hidden editor comments
            if line.startswith("&lt;!--"):
                continue
            # Remove tables
            if (
                    line.startswith("{|class=") or
                    line.startswith("{| class=") or
                    line.startswith("|") or
                    line.startswith("!")
            ):
                continue
            # Remove references
            if line.startswith('*'):
                continue
            # Remove categories
            if line.endswith("]]\n") or line.endswith("]]</text>\n"):
                continue
            # Miscellaneous other debris to remove
            if (
                    line.startswith("{{Redirect category shell|") or
                    line.startswith("{{Infobox") or
                    line == "}}\n" or
                    line.startswith("{{Rcat shell") or
                    line.startswith("{{rcat shell") or
                    line.startswith("{{multiple image") or
                    line.startswith("# ")
            ):
                continue
            # We're now likely looking at a line that is a paragraph of
            # human-readable data. We pass that to process_paragraph,
            # which returns the number of documents, along with the file
            # pointer, and get back the number of documents added to the file.
            documents_generated += process_paragraph(document_storage_file,
                                                     line)

    document_storage_file.close()

    print(f"\nFinished extracting documents from articles at "
          f"{pd.Timestamp.now().strftime('%H:%M:%S')}.\n"
          f"Extracted {articles_extracted:,} articles.\n"
          f"Extracted {documents_generated:,} documents.\n")


def process_paragraph(
        document_storage_file: typing.TextIO,
        paragraph: str
) -> int:
    # First apply all processes that will significantly shorten the block of
    # text, and then check to see if length has become insignificant before
    # doing further cleanup.

    # Remove end of sentence citations - multiple ways to do these...
    paragraph = re.sub(r"}}.*?}}", '}}', paragraph)
    paragraph = re.sub(r"\{\{.*?}}", '', paragraph)
    paragraph = re.sub(r"&lt;ref&gt.*?&lt;/ref&gt;", '', paragraph)
    # Eliminate editor comments, as well as ways of creating special links
    paragraph = re.sub(r"&lt.*?&gt;", '', paragraph)
    # This will rarely create a scenario where we have the string "()"
    paragraph = re.sub(r"\(\)", '', paragraph)
    # Replace hyperlinks with hyperlink text:
    links = re.findall("\[\[.*?]]", paragraph)
    for link in links:
        if '|' not in link:
            paragraph = paragraph.replace(link, link[2:-2], 1)
        else:
            bar_index = len(link) - link[::-1].index("|")
            paragraph = paragraph.replace(link, link[bar_index:-2], 1)

    # Again kill blocks of text of less than 100 characters (We did this
    # before process_paragraph, but have now cleaned out a lot more junk)
    if len(paragraph) < 100:
        return 0

    # Removing other special XML characters and odds and ends
    paragraph = re.sub(
        r"&amp;|nbsp;|&quot;",
        '', paragraph
    )

    # Remove punctuation that we want to ignore
    paragraph = paragraph.translate(translation_exclude)

    # Put spaces on either side of punctuation we want to include
    paragraph = paragraph.translate(translation_include)

    # We're going to split on periods, but need to deal with special cases
    # where periods are used for non-sentence ending purpose.

    # Need to replace e.g., i.e., etc., etc...
    paragraph = re.sub(r"e\.g\.", "for example", paragraph)
    paragraph = re.sub(r"i\.e\.", "that is", paragraph)
    paragraph = re.sub(r"etc\.", "etcetera", paragraph)
    paragraph = re.sub(r"et al\.", "and others", paragraph)
    paragraph = re.sub(r"mr\.", "mr", paragraph)
    paragraph = re.sub(r"mrs\.", "mrs", paragraph)
    paragraph = re.sub(r"ms\.", "ms", paragraph)
    paragraph = re.sub(r"miss\.", "miss", paragraph)
    # Remove "..."
    paragraph = re.sub(r"\.\.\.", '', paragraph)
    # Eliminate initials where indicated with periods
    paragraph = re.sub(r"[A-Z]\.[A-Z]\.", '', paragraph)
    paragraph = re.sub(r" [A-HJ-Z]\.", ' ', paragraph)
    # Convert decimal numbers to just number before decimal (round down)
    paragraph = re.sub(r"([0-9]+)\.[0-9]+", r"\1", paragraph)

    # Some last cleanup...

    # Set all characters to lower case
    paragraph = paragraph.lower()
    # Eliminate multiple spaces
    paragraph = re.sub(r" +", " ", paragraph)

    # Split into documents on periods - also removes periods
    documents = paragraph.split('.')
    # We want to convert our documents to lists of tokens and add them to our
    # data. We'll create a 'documents_generated' variable to track how many
    # we've created.
    documents_generated = 0
    for document in documents:
        document = document.strip()
        # Some documents were just the new lines that were at the ends of the
        # paragraphs, and are now empty strings after document.strip()
        if document == '':
            continue
        # Split document on spaces
        document = document.split()
        # Ditch documents consisting of single token
        if len(document) == 1:
            continue
        # For documents of three or less tokens, if any token includes
        # anything other than letters, drop them.
        if len(document) <= 3:
            drop = False
            for token in document:
                if not token.isalpha():
                    drop = True
            if drop:
                continue
        documents_generated += 1
        document_storage_file.write(' '.join(document) + '\n')
    # If we don't call flush on the storage buffer, it will accumulate data
    # until we close the file, and overflow memory.
    document_storage_file.flush()
    # Return the number of documents we've added to the storage file.
    return documents_generated


def chunk_documents_to_files(
        document_storage_filepath: str,
        documents_per_file: int = 1_000_000,
        output_filepath: str = os.path.join(DATA_DIR, "Wikipedia Corpus")
):
    with open(document_storage_filepath, 'r') as file:
        documents = list(file)
    random.shuffle(documents)

    # Figure out how many files we're going to create
    num_files = len(documents) // documents_per_file + 1

    for file_num in range(num_files - 1):
        file_documents = documents[
                         file_num * documents_per_file:(
                                                                   file_num + 1) * documents_per_file
                         ]
        dump_file(
            file_documents,
            f"corpus_file_{str(file_num + 1).rjust(3, '0')}_of_{num_files}",
            output_filepath
        )
    file_documents = documents[(num_files - 1) * documents_per_file:-1]
    dump_file(
        file_documents,
        f"corpus_file_{str(num_files).rjust(3, '0')}_of_{num_files}",
        output_filepath
    )


def dump_file(
        document_list: list[str],
        title: str,
        dir_path: str = os.path.join(DATA_DIR, "Wikipedia Corpus")
):
    print(document_list[0])
    with bz2.BZ2File(os.path.join(dir_path, title + '.bz2'), 'wb') as file:
        pickle.dump(document_list, file)


if __name__ == "__main__":
    main()
