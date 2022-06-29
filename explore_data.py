from __future__ import annotations

import bz2
import os
import pickle
import random
import re
from collections import Counter

import requests
import typing

import pandas as pd

import parameters as p
from article import Article

DATA_DIR = os.path.join(p.DATA)

# This code was built and tested on the wikipedia dump of 2022-05-20
WIKIPEDIA_XML_DUMP_URL = p.WIKIPEDIA_XML_DUMP_URL
# The input file and un-chunked output files will be stored in the
# raw_data_dir. Note that these files will likely both be greater than 20 gb
# in size.
RAW_DATA_DIR = p.RAW_DATA_DIR
# The chunked, compressed data files will be stored in output_dir. Note that
# these files are likely to total more than 5 gb.
CORPUS_DIR = p.CORPUS_DIR
# Directory to which to output test data files if option is selected.
TEST_DIR = p.TEST_DIR
PROCESS_GARBAGE_MARKER = p.PROCESS_GARBAGE_MARKER

# Define two translation maps for removing punctuation via str.translate,
# which is very efficient. The first will consist of punctuation to remove.
# The second will consist of punctuation around which to insert spaces,
# so that occurrences can be treated as tokens when splitting on white space.
# Periods, exclamation points, and question marks are handles in the code,
# and are used to split documents.
punctuation_exclude = "#$&*+<=>@[\\]^_`{|}~%"
translation_exclude = str.maketrans({
    char: '' for char in punctuation_exclude
})
punctuation_include = "-()/;,:!.?'\""
translation_include = str.maketrans({
    char: f' {char} ' for char in punctuation_include
})


def main():
    # get_page_tag_stats()
    # check_for_nested_pages()
    # data = get_pages(1)[0].data
    # print(data)
    # get_high_freq_lines()
    # pages = get_pages(list(range(100, 999, 100_000)))
    # get_redirect_page_count()
    # get_num_pages_with_title_as_first_line()
    # get_ns_distribution()
    # get_num_pages_with_text_tag()
    pass


def get_page_tag_stats(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> dict[str, int]:
    """
    Gets the lines containing "<page" or "</page" and counts occurrences of
    specific instances in the document.
    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    # Create dictionary to identify lines containing "<page" and count
    # occurrences.
    line_counts = dict()

    # Iterate through file, counting number of occurrences of opening <page>
    # xml tag in lines
    with bz2.BZ2File(input_file_path, 'r') as file:
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            if "<page" in line or "</page" in line:
                if line in line_counts:
                    line_counts[line] += 1
                else:
                    line_counts[line] = 1
                    print(f"New line discovered: \n\t{line}")

    print("\nResults:")
    for line, count in line_counts.items():
        print(f"\t{count}: {line}")

    return line_counts


def check_for_nested_pages(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> bool:
    """
    Checks to see if <page> tags are ever nested in the document.
    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    depth = 0

    # Iterate through file, counting number of occurrences of opening <page>
    # xml tag in lines
    with bz2.BZ2File(input_file_path, 'r') as file:
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            if "<page" in line:
                depth += 1
                if depth > 1:
                    print("Nesting of page tag found.")
                    return True
            if "</page" in line:
                depth -= 1
                if depth < 0:
                    raise Exception(
                        "Error, close tag count exceeds open tag count."
                    )

    print("No nesting of page tag found.")
    return False


def get_pages(
        page_numbers: int | list[int],
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> list[Article]:
    """
    Returns the numbered pages specified from the document. Does so by
    iterating through the lines of the file from the beginning. Exits as soon
    as the page with the largest number in page_numbers is processed.
    :param page_numbers:
    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    # if a single page number is provided, convert it to a list with that
    # page number as the only item in the list
    if type(page_numbers) is int:
        page_numbers = [page_numbers]
    # Get the largest page number for efficient early exit from iteration
    # through file.
    max_page_number = max(page_numbers)
    # Create a list to store pages in.
    pages = list()  # type: list[Article]
    # Page numbers start at 1, and are incremented every time <\page> is
    # encountered. We initialize it to zero.
    current_page = 0

    # Iterate through file, generating Page objects and appending them to
    # pages until all page_numbers in list have been iterated through.
    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            if "<page>" in line:
                current_page += 1
                if current_page > max_page_number:
                    break
                lines = list()
            elif "</page>" in line:
                if current_page in page_numbers:
                    pages.append(Article(lines))
            else:
                lines.append(line)

    return pages


def get_high_freq_lines(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR,
        max_lines: int = 1000,
        trim_count: int = 1_000_000,
        max_pages: int | None = None
) -> Counter[str | int]:

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    line_counts = Counter()
    current_page = 0

    # Iterate through file, generating Page objects and appending them to
    # pages until all page_numbers in list have been iterated through.
    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            if "<page>" in line:
                current_page += 1
                if max_pages is not None and current_page > max_pages:
                    break
                if current_page % 10_000 == 0:
                    print(f"Processed {current_page:,} pages...")
                lines = list()
            elif "</page>" in line:
                line_counts.update(lines)
            else:
                lines.append(line)
                if len(line_counts) > trim_count:
                    line_counts = line_counts.most_common(max_lines)
                    line_counts = Counter(dict(line_counts))

    line_counts = line_counts.most_common(max_lines)
    line_counts = Counter(dict(line_counts))

    print((f"Processed {current_page:,} pages...\nResults:\n"))
    for line, count in line_counts.items():
        print(f"{line[:-1]},{count}")

    return line_counts


def get_redirect_page_count(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> int:
    """
    Checks to see if <page> tags are ever nested in the document.
    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    redirect_pages = 0

    # Iterate through file, counting number of occurrences of opening <page>
    # xml tag in lines
    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8').strip()
            if "<page>" in line:
                lines = list()
            elif "</page>" in line:
                redirect_line_count = 0
                for page_line in lines:
                    if page_line.startswith("<redirect title="):
                        redirect_line_count += 1
                if redirect_line_count == 1:
                    redirect_pages += 1
                    if redirect_pages % 10_000 == 0:
                        print(f"Found {redirect_pages:,} pages so far...")
                elif redirect_line_count > 1:
                    raise Exception("More than one redirect line found in a "
                                    "single page...")
            else:
                lines.append(line)

    print(f"Found a total of {redirect_pages:,}")
    return redirect_pages


def get_num_pages_with_title_as_first_line(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> int:
    """

    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    pages_processed = 0
    pages_starting_with_title = 0

    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8').strip()
            if "<page>" in line:
                lines = list()
            elif "</page>" in line:
                pages_processed += 1
                if lines[0].strip().startswith("<title"):
                    pages_starting_with_title += 1
                if pages_processed % 10_000 == 0:
                    print(f"Processed {pages_processed:,} pages... "
                          f"{pages_starting_with_title:,} have started with "
                          f"'<title'")
            else:
                lines.append(line)

    print(f"A total of {pages_starting_with_title:,} pages had a first line "
          f"starting with '<title'")
    return pages_starting_with_title


def get_ns_distribution(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> Counter[str, int]:
    """

    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    ns_dist = Counter()

    pages_processed = 0

    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8').strip()
            if "<page>" in line:
                lines = list()
            elif "</page>" in line:
                pages_processed += 1
                ns = re.findall(r'\d+', lines[1])[0]
                if ns == '0':
                    redirect = False
                    for page_line in lines:
                        if page_line.startswith("<redirect title="):
                            redirect = True
                            break
                    if redirect:
                        ns = ns + ' redirect'
                ns_dist.update([ns])
                if pages_processed % 10_000 == 0:
                    print(f"Processed {pages_processed:,} pages... ")
            else:
                lines.append(line)

    for key, value in ns_dist.items():
        print(f"{key}, {value}")
    return ns_dist


def get_num_pages_with_text_tag(
        xml_url: str = WIKIPEDIA_XML_DUMP_URL,
        raw_data_dir: str = RAW_DATA_DIR
) -> int:
    """

    :param xml_url:
    :param raw_data_dir:
    :return:
    """

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    input_file_name = xml_url.rsplit('/', 1)[-1]
    input_file_path = os.path.join(raw_data_dir, input_file_name)

    pages_processed = 0
    pages_w_independent_text_tag = 0

    with bz2.BZ2File(input_file_path, 'r') as file:
        lines = list()  # type: list[str]
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8').strip()
            if "<page>" in line:
                lines = list()
            elif "</page>" in line:
                pages_processed += 1
                for page_line in lines:
                    if page_line.strip().startswith("<text"):
                        pages_w_independent_text_tag += 1
                        continue
                if pages_processed % 10_000 == 0:
                    print(f"Processed {pages_processed:,} pages... "
                          f"{pages_w_independent_text_tag:,} have had the "
                          f"'text' tag start on its own line.")
            else:
                lines.append(line)

    print(f"A total of {pages_w_independent_text_tag:,} pages have had the "
          f"'text' tag start on its own line.")
    return pages_w_independent_text_tag


if __name__ == "__main__":
    main()
