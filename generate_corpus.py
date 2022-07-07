from __future__ import annotations

import bz2
import os
import pickle
import random
import re
import shutil
import warnings

import pandas as pd
import requests
import wget
import yaml

from article import Article


def main():
    generate_corpus('my_generate_corpus_config.yaml')


def generate_corpus(
        config_file_path: str = './default_generate_corpus_config.yaml'
):
    # Validate yaml configuration file and build dictionary of arguments.
    args = validate_configuration(config_file_path)

    # Get data file if not already acquired.
    get_wikipedia_dump(
        args['mode'],
        args['num_test_articles'],
        args['xml_file_url'],
        args['raw_data_dir'],
        args['test_data_dir']
    )
    print("\n" + "-------------------------------" + "\n")

    # Extract documents and export them to a text file.
    extract_documents(
        args['document_type'],
        args['mode'],
        args['num_test_articles'],
        args['xml_file_url'],
        args['raw_data_dir'],
        args['intermediate_data_dir'],
        args['test_data_dir'],
        args['translation_include'],
        args['translation_exclude'],
        args['marker_garbage_processing'],
        args['marker_paragraph_break']
    )
    print("\n" + "-------------------------------" + "\n")

    # Randomize documents in document text file and store them in manageable
    # "chunks" in numbered compressed files.
    if args['mode'] == 'production':
        chunk_documents_to_files(
            args['document_type'],
            args['xml_file_url'],
            args['intermediate_data_dir'],
            args['corpus_dir'],
            args['documents_per_corpus_file']
        )


def validate_configuration(
        config_file_path: str
) -> dict[str: str | int | None]:
    with open(config_file_path, 'r') as file:
        data = yaml.safe_load(file)

    return_values = dict()

    # Get type of document to be generated.
    document_type = data['document_type']
    if (
            type(document_type) is not str or
            document_type not in ['sentence', 'paragraph', 'article']
    ):
        raise ValueError(
            "'document_type' must be one of 'sentence', 'paragraph', "
            "or 'article'."
        )
    return_values['document_type'] = document_type

    # Get whether test data or production data is to be generated. If test
    # data is to be generated, get number of test articles to be generated.
    mode = data['mode']  # type: str
    if (
            type(mode) is not str or
            mode not in ['testing', 'production']
    ):
        raise ValueError(
            "'mode' must be one of 'testing' or 'production'."
        )
    if mode == 'testing':
        num_test_articles = data['num_test_articles']  # type: int | None
        if type(num_test_articles) is not int or num_test_articles < 1:
            raise ValueError(
                "'num_test_articles' must be a positive integer."
            )
    else:
        num_test_articles = None
    return_values['mode'] = mode
    return_values['num_test_articles'] = num_test_articles

    # If no value is specified for the xml file url, specify the url for a
    # recent dump.
    xml_file_url = data['xml_file_url']  # type: str | None
    if xml_file_url is None:
        dttm_now = pd.Timestamp.now()
        if dttm_now.day > 14:
            dttm_dump = dttm_now - pd.offsets.MonthBegin()
        else:
            dttm_dump = \
                dttm_now - pd.offsets.MonthEnd() - pd.offsets.MonthBegin()
        date_string = dttm_dump.strftime("%Y%m%d")
        xml_file_url = (
                "https://dumps.wikimedia.org/enwiki/" +
                date_string + "/enwiki-" +
                date_string + "-pages-articles-multistream.xml.bz2"
        )
    # If a value is specified, verify that the format is valid.
    elif (
            type(xml_file_url) is not str or
            not xml_file_url.startswith(
                "https://dumps.wikimedia.org/enwiki/") or
            not xml_file_url.endswith("-pages-articles-multistream.xml.bz2") or
            xml_file_url[41:43] != '01'
    ):
        raise ValueError(
            "This does not appear to be a viable URL. Visit\n"
            "https://dumps.wikimedia.org/enwiki/ for viable links\n"
            "to recent wikipedia data dumps. Dumps generated on the 1st of\n"
            "the month should be used, not dumps generated on the 20th. File\n"
            "links on the dump-specific reference page should be of the form: "
            "\n\n"
            "\tenwiki-[year][month]01-pages-articles-multistream.txt"
            ".bz2\n\n"
            "Where year is the four digit year and month is the zero-padded\n"
            "month."
        )
    check_url = data['check_url']  # type: bool
    if type(check_url) is not bool:
        raise ValueError(
            "'check_url' must be of type bool."
        )
    # Finally, verify that the URL is reachable, if check_url is true
    if check_url:
        try:
            response = requests.get(xml_file_url, stream=True)
            if response.status_code != 200:
                raise ValueError(
                    "Was unable to make connection to wikipedia dump URL."
                    "Are you trying to access a dump older than three months "
                    "that is no longer hosted?"
                )
        except requests.exceptions.RequestException as e:
            raise SystemExit(
                f"The specified url is not reachable.\n"
                f"Err: {e}"
            )
    return_values['xml_file_url'] = xml_file_url

    raw_data_dir = data['raw_data_dir']  # type: str
    if type(raw_data_dir) is not str:
        raise ValueError(
            "'raw_data_dir' must be of type str and a viable file path."
        )
    if not os.path.isdir(raw_data_dir):
        try:
            os.makedirs(raw_data_dir)
        except(
                f"Was unable to create directory at {raw_data_dir}"
        ):
            exit(1)
    return_values['raw_data_dir'] = raw_data_dir

    intermediate_data_dir = data['intermediate_data_dir']  # type: str
    if type(intermediate_data_dir) is not str:
        raise ValueError(
            "'intermediate_data_dir' must be of type str "
            "and a viable file path."
        )
    if not os.path.isdir(intermediate_data_dir):
        try:
            os.makedirs(intermediate_data_dir)
        except(
                f"Was unable to create directory at {intermediate_data_dir}"
        ):
            exit(1)
    return_values['intermediate_data_dir'] = intermediate_data_dir

    corpus_dir = data['corpus_dir']  # type: str
    if type(corpus_dir) is not str:
        raise ValueError(
            "'corpus_dir' must be of type str and a viable file path."
        )
    if not os.path.isdir(corpus_dir):
        try:
            os.makedirs(corpus_dir)
        except(
                f"Was unable to create directory at {corpus_dir}"
        ):
            exit(1)
    return_values['corpus_dir'] = corpus_dir

    test_data_dir = data['test_data_dir']  # type: str
    if type(test_data_dir) is not str:
        raise ValueError(
            "'test_data_dir' must be of type str and a viable file path."
        )
    if not os.path.isdir(test_data_dir):
        try:
            os.makedirs(test_data_dir)
        except(
                f"Was unable to create directory at {test_data_dir}"
        ):
            exit(1)
    return_values['test_data_dir'] = test_data_dir

    documents_per_corpus_file = \
        data[f'documents_per_corpus_file_{document_type}']  # type: int | None
    if not (
            (type(documents_per_corpus_file) is int and
             documents_per_corpus_file > 0) or
            type(documents_per_corpus_file) is None
    ):
        raise ValueError(
            f"'documents_per_corpus_file_{document_type} must be an integer "
            f"greater than 0 or None."
        )
    return_values['documents_per_corpus_file'] = documents_per_corpus_file

    marker_garbage_processing = data['marker_garbage_processing']  # type: str
    if type(marker_garbage_processing) is not str:
        raise ValueError(
            "'marker_garbage_processing' must be of type str."
        )
    return_values['marker_garbage_processing'] = marker_garbage_processing

    marker_paragraph_break = data['marker_paragraph_break']  # type: str
    if type(marker_paragraph_break) is not str:
        raise ValueError(
            "'marker_paragraph_break' must be of type str."
        )
    if marker_paragraph_break != "uvmr_ev_paragraph_break":
        warnings.warn(
            "Failure to use the default value of 'uvmr_ev_paragraph_break'"
            "for 'marker_paragraph_break' will result in a corpus that may "
            "not be compatible with other products intended to use the "
            "corpuses generated by this code."
        )
    return_values['marker_paragraph_break'] = marker_paragraph_break

    # Define two translation maps for removing punctuation via str.translate,
    # which is very efficient. The first will consist of punctuation to remove.
    # The second will consist of punctuation around which to insert spaces,
    # so that occurrences can be treated as tokens when splitting on white space.
    # Periods, exclamation points, and question marks are handles in the code,
    # and are used to split documents. Quotation marks are included if
    # DOCUMENT_TYPE is 'paragraph', and excluded if DOCUMENT_TYPE is 'sentence'.
    if document_type == "sentence":
        punctuation_exclude = "#$&*+<=>@[\\]^_`{|}~%\""
        punctuation_include = "-()/;,:!.?'"
    else:
        punctuation_exclude = "#$&*+<=>@[\\]^_`{|}~%"
        punctuation_include = "-()/;,:!.?'\""

    translation_exclude = str.maketrans({
        char: '' for char in punctuation_exclude
    })
    return_values['translation_exclude'] = translation_exclude

    translation_include = str.maketrans({
        char: f' {char} ' for char in punctuation_include
    })
    return_values['translation_include'] = translation_include

    return return_values


def get_wikipedia_dump(
        mode: str,
        num_test_articles: int,
        xml_file_url: str,
        raw_data_dir: str,
        test_data_dir: None | str = None
) -> None:
    """
    Obtains a wikipedia text dump encoded as XML and compressed with bz2.

    Will first check raw_data_dir to see if the file already exists there. If
    the file already exists, it will not be downloaded again.

    Note that this file will likely exceed 20 GB in size. Uncompressed size
    will be greater than 80GB, but the corpus extraction code in this module
    will not decompress it, nor will it attempt to load the entire file into
    memory. Instead, it will process one line at a time to preserve resources.

    If a directory path is passed via test_data_dir, will generate a file
    named "Raw Data.txt" in that directory containing the raw file text from
    the first n wikepedia articles that are not redirects, where n =
    num_test_articles.

    :param xml_file_url: The url for the compressed wikipedia dump.
    :param raw_data_dir: The directory to store the compressed data file in.
    :param mode: Used to determine whether test data should be generated.
    :param test_data_dir: The directory in which to store a sample of the raw
    text of the file.
    :param num_test_articles: The number of articles to export as test data if
    test_data_dir is specified.
    :return: None.
    """

    dttm_start = pd.Timestamp.now()
    print(f"Beginning download of wikipedia xml dump at "
          f"{dttm_start.strftime('%H:%M:%S')}")

    # Pull the file name off the end of the URL, and define the path to where
    # the file will be stored.
    bz2_file_name = xml_file_url.rsplit('/', 1)[-1]
    bz2_file_path = os.path.join(raw_data_dir, bz2_file_name)

    # Check if xml_file has already been downloaded
    if os.path.exists(bz2_file_path):
        print("Compressed XML data file found in raw data directory...\n"
              "Download of compressed XML data file canceled.")
        downloaded = False
    else:
        wget.download(xml_file_url, bz2_file_path)
        downloaded = True

    # If testing mode is specified, output raw text of
    # specified number of articles to "Raw Data.txt", leaving a blank line in
    # between each line of the dump for readability.
    if mode == 'testing':
        date_of_dump = (bz2_file_name[7:15])
        print(f"Generating raw data file in test directory. "
              f"Including first {num_test_articles} articles...")
        test_data_dir = os.path.join(
            test_data_dir,
            "XML File Creation Date " + date_of_dump +
            " - Raw Data.txt"
        )
        with (
            bz2.BZ2File(bz2_file_path, 'rb') as in_file,
            open(test_data_dir, 'w') as test_data_file
        ):
            test_data_file.write(
                f"This file contains the raw article text for the first "
                f"{num_test_articles} articles in the wikipedia data file. "
                f"Data file ines are "
                f"separated by blank lines for purposes of readability. Only "
                f"articles from namespace 0 (subject-specific articles) that "
                f"are not redirects are included.\n"
            )
            current_article = 1
            article_lines = list()

            for line in in_file:
                # Convert from binary string
                line = line.decode('utf-8')
                if "<page>" in line:
                    article_lines = list()
                elif "</page>" in line:
                    article = Article(article_lines)
                    # Wikipedia articles are in namespace 0, and we are not
                    # interested in redirect pages, which contain no useful
                    # text.
                    if article.namespace == 0 and not article.is_redirect_page:
                        test_data_file.write(
                            f"\n\n\n\n"
                            f"*********** Article {current_article}: "
                            f"{article.title} ***********"
                            f"\n\n\n"
                        )
                        for article_line in article.lines:
                            if article_line != '':
                                test_data_file.write(article_line + '\n\n')
                        current_article += 1
                else:
                    article_lines.append(line)
                if current_article > num_test_articles:
                    break
        print(f"'Raw Data.txt' generated for first {num_test_articles} "
              f"articles.")

    dttm_finish = pd.Timestamp.now()
    dttm_delta = (dttm_finish - dttm_start).total_seconds() / 60
    file_size = os.path.getsize(bz2_file_path)
    print(f"Finished at "
          f"{dttm_finish.strftime('%H:%M:%S')}.")
    if downloaded:
        print(f"Elapsed Time: {dttm_delta:,.2f} minutes.")
    print(f"File Size: {file_size / (1024 ** 3):,.4} GB")


def extract_documents(
        document_type: str,
        mode: str,
        num_test_articles: int,
        xml_file_url: str,
        raw_data_dir: str,
        intermediate_data_dir: str,
        test_data_dir: str,
        translation_include: dict,
        translation_exclude: dict,
        marker_garbage_processing: str,
        marker_paragraph_break: str
) -> None:
    dttm_start = pd.Timestamp.now()
    print(f"Beginning extraction of documents from input file at "
          f"{dttm_start.strftime('%H:%M:%S')}")

    # Pull the file name off the end of the URL, and define the path to where
    # the input file is stored.
    bz2_file_name = xml_file_url.rsplit('/', 1)[-1]
    bz2_file_path = os.path.join(raw_data_dir, bz2_file_name)

    date_of_dump = (bz2_file_name[7:15])

    # If in testing mode, we point document_storage_file at our test directory.
    # We also point filtered_data_file at the same directory
    if mode == 'testing':
        document_storage_file = open(
            os.path.join(
                test_data_dir,
                "XML File Creation Date " + date_of_dump +
                f" - Documents - {document_type.capitalize()} Level.txt"
            ), 'w'
        )
        document_storage_file.write(
            f"This file contains the documents generated for the first "
            f"{num_test_articles} articles in the wikipedia file. "
            f"Documents are "
            f"separated by blank lines for purposes of readability. Only "
            f"articles from namespace 0 (subject-specific articles) that "
            f"are not redirects are included. Documents in this file were "
            f"generated at the {document_type} level."
        )
        filtered_data_file = open(
            os.path.join(
                test_data_dir,
                "XML File Creation Date " + date_of_dump +
                f" - Filtered Data.txt"
            ), 'w'
        )
        filtered_data_file.write(
            f"This file contains the raw paragraphs identified for document "
            f"extraction for the first "
            f"{num_test_articles} articles in the wikipedia file. Paragraphs "
            f"are "
            f"separated by blank lines for purposes of readability. Only "
            f"articles from namespace 0 (subject-specific articles) that "
            f"are not redirects are included."
        )
    # Otherwise we will point document_storage_file at our intermediate data
    # directory, and we will assign None to test_data_file.
    else:
        file_path = os.path.join(
            intermediate_data_dir,
            "XML File Creation Date " + date_of_dump +
            f" - Documents - {document_type.capitalize()} Level.txt"
        )
        # If the file has already been generated, exit. We do not test for
        # this if generating test data.
        if os.path.exists(file_path):
            print(
                "Documents appear to have already been extracted to file.\n"
                f"{file_path} exists..."
            )
            return None
        document_storage_file = open(
            os.path.join(file_path), 'w'
        )
        filtered_data_file = None

    # Initialize some counters with which to report progress via stdout
    articles_extracted = 0
    documents_generated = 0

    with bz2.BZ2File(bz2_file_path, 'r') as file:
        print(f"Successfully accessed bz2 input file...\n")
        lines = list()
        for line in file:
            # Convert from binary string
            line = line.decode('utf-8')
            # Strip whitespace - xml tags are variably indented for
            # readability and the newline is inconvenient
            line = line.strip()
            # As we iterate through the lines of an article, we will determine
            # whether it is a wikipedia article (namespace 0) and
            # whether it is a redirect. This will prevent us from
            # unnecessarily making Article objects.
            if line == '<page>':
                lines = list()
                correct_name_space = False
                is_a_redirect = False
            elif (
                    line == '</page>' and
                    correct_name_space and
                    not is_a_redirect
            ):
                article = Article(lines)
                paragraphs = article.get_paragraphs()
                if mode == 'testing':
                    filtered_data_file.write(
                        f"\n\n\n\n"
                        f"*********** Article {articles_extracted + 1}: "
                        f"{article.title} ***********"
                        f"\n\n"
                    )
                    document_storage_file.write(
                        f"\n\n\n\n"
                        f"*********** Article {articles_extracted + 1}: "
                        f"{article.title} ***********"
                        f"\n\n"
                    )
                article_level_document = str()
                for paragraph in paragraphs:
                    if mode == 'testing':
                        filtered_data_file.write(paragraph + '\n\n')
                    documents = process_paragraph(
                        paragraph, document_type,
                        translation_include, translation_exclude,
                        marker_garbage_processing
                    )
                    if document_type in ['sentence', 'paragraph']:
                        for document in documents:
                            document_storage_file.write(document + '\n')
                            if filtered_data_file:
                                document_storage_file.write('\n')
                        documents_generated += len(documents)
                    else:
                        article_level_document += documents[0]
                        article_level_document += f" {marker_paragraph_break} "
                if document_type == 'article':
                    document_storage_file.write(article_level_document + '\n')
                    documents_generated += 1
                # If we don't call flush on the storage buffer, it will
                # accumulate data until we close the file, and overflow
                # memory.
                document_storage_file.flush()

                articles_extracted += 1
                if articles_extracted % 10_000 == 0:
                    print(f"Extracted Article {articles_extracted:,}: "
                          f"{documents_generated:,} Documents Generated.")
                if filtered_data_file and \
                        articles_extracted >= num_test_articles:
                    break
            else:
                lines.append(line)
                if (
                        line.startswith('<ns>') and
                        line == '<ns>0</ns>'
                ):
                    correct_name_space = True
                if line.startswith('<redirect title='):
                    is_a_redirect = True

    # Clean-up
    if filtered_data_file:
        filtered_data_file.close()
    document_storage_file.close()

    dttm_finish = pd.Timestamp.now()
    dttm_delta = (dttm_finish - dttm_start).total_seconds() / 60
    print(f"\nFinished extracting documents from input file at "
          f"{dttm_finish.strftime('%H:%M:%S')}.\n"
          f"Elapsed Time: {dttm_delta:,.2f} minutes."
          f"Extracted {articles_extracted:,} articles.\n"
          f"Extracted {documents_generated:,} documents.\n")


def process_paragraph(
        paragraph: str,
        document_type: str,
        translation_include: dict,
        translation_exclude: dict,
        marker_garbage_processing: str
) -> list[str]:
    """

    :param translation_exclude:
    :param translation_include:
    :param paragraph:
    :param document_type:
    :return:
    """

    # Deal with language templates
    langs = re.findall("\{\{lang\|.*?}}", paragraph)
    for lang in langs:
        fragments = lang[2:-2].split('|')
        if '=' not in fragments[-1]:
            substitution = re.sub(r"'", '', fragments[-1])
        else:
            substitution = fragments[-2]
        paragraph = paragraph.replace(lang, substitution, 1)

    # Deal with conversion templates
    # Only the most common cases are handled here - there are complicated
    # options that would take many, many lines of code.
    conversions = re.findall("\{\{convert\|.*?}}", paragraph)
    for conversion in conversions:
        fragments = conversion[2:-2].split('|')
        substitution = fragments[1] + ' '
        if len(fragments) >= 3 and fragments[2] not in [
            '-', '–', 'and', 'and(-)', 'or', 'to', 'to(-)',
            'to about', '+/-', '±', '+', ',', ', and', ', or',
            'by', 'x', '×', 'xx', '*']:
            substitution += fragments[2]
        elif len(fragments) >= 5:
            substitution += fragments[4]
        else:
            substitution = marker_garbage_processing
        substitution = re.sub(r"[\[\]]", '', substitution)
        paragraph = paragraph.replace(conversion, substitution, 1)

    # Deal with non-english translation templates:
    translations = re.findall("\{\{transl\|.*?}}", paragraph)
    for translation in translations:
        fragments = translation[2:-2].split('|')
        substitution = re.sub(r"[\[\]]", '', fragments[-1])
        paragraph = paragraph.replace(translation, substitution, 1)

    # Eliminate a specific way to create end of sentence links
    paragraph = re.sub(r"&lt;ref.*?/ref&gt;", '', paragraph)
    # Eliminate editor comments, and a number of special features
    paragraph = re.sub(r"&lt.*?&gt;", '', paragraph)
    # Remove end of sentence citations created via templates
    paragraph = re.sub(r"}}[^{][^{]*?}}", '}}', paragraph)
    paragraph = re.sub(r"\{\{.*?}}", '', paragraph)
    # The above will rarely create a scenario where we have the string "()"
    paragraph = re.sub(r"\(\)", '', paragraph)

    # Replace hyperlink code within wikipedia with hyperlink text:
    links = re.findall("\[\[.*?]]", paragraph)
    for link in links:
        fragments = link[2:-2].split('|')
        paragraph = paragraph.replace(link, fragments[-1], 1)
    # deal with external hyperlinks and scenarios where letters or words are
    # inserted into incomplete quotes
    elinks = re.findall("\[.*?]", paragraph)
    for link in elinks:
        fragments = link[1:-1].split()
        if len(fragments) == 1:
            substitution = fragments[0]
        else:
            substitution = str(fragments[1:])
        paragraph = paragraph.replace(link, substitution, 1)

    # Removing other special XML characters and odds and ends
    paragraph = re.sub(
        r"&amp;|nbsp;",
        '', paragraph
    )

    # Replace &quot; with double quotes, which are not otherwise used.
    paragraph = re.sub(r"&quot;", '"', paragraph)

    # Deal with "''" and "'''" used for italics and bold notation - do not
    # remove single "'"
    paragraph = re.sub(r"'''", '', paragraph)
    paragraph = re.sub(r"''", '', paragraph)

    # Deal with contractions by removing apostrophe and splicing words
    paragraph = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1\2", paragraph)
    # At this point, we're down to only quotes used to indicate possession at
    # the end of a word ending in s, e.g. Zeus'.

    # Comma-separated numbers need their commas pulled
    paragraph = re.sub(r"([0-9]),([0-9])", r"\1\2", paragraph)

    # Remove punctuation that we want to ignore - note that if DOCUMENT_TYPE
    # is "sentence" then this will remove double quotes, which will desirably
    # affect downstream processing.
    paragraph = paragraph.translate(translation_exclude)

    # Splitting will involve periods, but we need to deal with special cases
    # where periods are used for non-sentence ending purpose. For
    # consistency, we will perform this processing even if we are generating
    # paragraph level documents.

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
    # Exclude the possible scenario of a sentence ending with the word "I".
    paragraph = re.sub(r" [A-HJ-Z]\.", ' ', paragraph)
    # Convert decimal numbers to just number before decimal (round down)
    # This may do funky things with the occasional decimal separated date,
    # but this has only a very occasional impact.
    paragraph = re.sub(r"([0-9]+)\.[0-9]+", r"\1", paragraph)

    # Set all characters to lower case
    paragraph = paragraph.lower()

    # Split into sentence level documents - strings of tokens separated by
    # spaces
    sentences = split_paragraph(paragraph, translation_include)

    documents = list()

    # We'll split our documents into lists to do some final cleanup -
    # basically just eliminating some very short documents that contain
    # non-alphabetic characters.
    for sentence in sentences:
        # Split document on spaces
        sentence = sentence.split()
        # For documents of four or fewer tokens, if any but the last token
        # consists of anything but letters, ditch the document. This
        # accommodates very short phrases, but eliminates a fair amount of
        # noise. More relevant to eventual incorporation of data sources
        # involving character speech: "Yes.", "No.", "That way.",
        # "I guess so."
        if len(sentence) <= 4:
            drop = False
            for token in sentence[0:-1]:
                if not token.isalpha():
                    drop = True
            if drop:
                continue
        sentence = ' '.join(sentence)
        documents.append(sentence)

    if document_type == 'sentence':
        return documents
    else:
        documents = [' '.join(documents)]
        return documents


def split_paragraph(
        paragraph: str,
        translation_include: dict
) -> list[str]:
    """
    Takes a paragraph and splits it into sentences, using the characters in
    splitters, and retaining those characters as part of the document.

    :param paragraph: A cleaned up, usually multi-sentence block of
    human-readable text.
    :param translation_include:
    :return: A list of strings, each stream consisting of a document,
    with tokens separated by whitespace.
    """
    splitters = ['."', '!"', '?"', '.', '!', '?', ]

    lines = list()
    current_line = str()
    for i in range(len(paragraph)):
        current_line += paragraph[i]
        # Two characters splitters - with double quotes.
        if current_line[-2:] in splitters:
            lines.append(current_line.strip())
            current_line = str()
            continue
        # One character splitters - no double quotes.
        if current_line[-2:-1] in splitters:
            lines.append(current_line[:-1].strip())
            current_line = str()
            continue
        # This case deals with a paragraph not ending with a splitter.
        if i == len(paragraph) - 1:
            if current_line != '\n':
                lines.append(current_line)

    # Take the punctuation we want to include and slap a space on either side
    # of it, allowing the line to be split into documents using spaces.
    for i in range(len(lines)):
        lines[i] = lines[i].translate(translation_include)
        # Eliminate multiple spaces we have introduced
        lines[i] = re.sub(r" +", " ", lines[i])

    return lines


def chunk_documents_to_files(
        document_type: str,
        xml_file_url: str,
        intermediate_data_dir: str,
        corpus_dir: str,
        documents_per_corpus_file: int
) -> None:
    """
    Takes as input a text file with one document per line. Randomizes the
    order of documents, then breaks them into chunks of n documents and
    stores those documents as binary files compressed via bz2, again with one
    document per line.

    n is specified by documents_per_files, with a default
    value of 1,000,000. For a recent wikipedia corpus processed by methods in
    this module, this results in about 150 files about 38 mb in size.

    This process DOES load the 20GB output file into memory to shuffle it.
    I've experimented quite a bit with memory-efficient ways to approach that
    task, and they are all extremely slow. Something to work on.

    :param document_storage_filepath: Directory in which to write output files.
    :param documents_per_file: Number of documents to include in each file.
    :param output_filepath: Directory to store documents in.
    :return: None.
    """

    dttm_start = pd.Timestamp.now()
    print(f"Beginning document chunking at "
          f"{dttm_start.strftime('%H:%M:%S')}")

    # Get input file path
    bz2_file_name = xml_file_url.rsplit('/', 1)[-1]
    date_of_dump = (bz2_file_name[7:15])
    input_file_path = os.path.join(
        intermediate_data_dir,
        "XML File Creation Date " + date_of_dump +
        f" - Documents - {document_type.capitalize()} Level.txt"
    )

    # Modify corpus directory path to point to subdirectory labeled with date
    # of XML File Creation and Document Type
    corpus_dir = os.path.join(
        corpus_dir,
        "XML File Creation Date " + date_of_dump +
        f" - {document_type.capitalize()} Level"
    )
    # If there is an existing directory, wipe it to clear old corpus,
    # then create empty directory.
    if os.path.isdir(corpus_dir):
        shutil.rmtree(corpus_dir)
    try:
        os.makedirs(corpus_dir)
    except(
        f"Was unable to create directory at {corpus_dir}"
    ):
        exit(1)

    # Load corpus into memory and shuffle it. This requires considerable memory,
    # but on-disk methods are fantastically slow. Note that calling list on the
    # file is much more memory efficient than other, similar methods,
    # for reasons that are unclear to me. This approach uses about the same
    # amount of memory as the file occupies on disk.
    print("Loading documents into memory. This may take a while...")
    with open(input_file_path, 'r') as file:
        documents = list(file)
    print("Shuffling documents. This will take even longer...")
    random.shuffle(documents)

    # Figure out how many files we're going to create
    num_files = len(documents) // documents_per_corpus_file + 1
    print(f"Generating {num_files} files...")

    # Create all of our files except for the last one
    for file_num in range(num_files - 1):
        print(f"Generating file number {file_num+1}...")
        start = file_num * documents_per_corpus_file
        stop = (file_num + 1) * documents_per_corpus_file
        file_documents = documents[start:stop]
        # Store as binary bz2 file.
        dump_file(
            file_documents,
            f"corpus_file_{str(file_num + 1).rjust(3, '0')}_of_{num_files}",
            corpus_dir
        )
    # Create our last file, consisting of less than documents_per_file
    # documents.
    print(f"Generating file number {num_files}...")
    file_documents = documents[(num_files - 1) * documents_per_corpus_file:-1]
    dump_file(
        file_documents,
        f"corpus_file_"
        f"{str(num_files).rjust(2, '0')}"
        f"_of_"
        f"{str(num_files).rjust(2, '0')}",
        corpus_dir
    )

    dttm_finish = pd.Timestamp.now()
    print(f"Finished at "
          f"{dttm_finish.strftime('%H:%M:%S')}.")
    print(f"Generated {num_files} files.")


def dump_file(
        document_list: list[str],
        file_name: str,
        dir_path: str
) -> None:
    """
    A very simple method that stores a list of strings as a compressed binary
    file. Implemented separately to facilitate future development of added
    features.

    :param document_list: A list of document strings.
    :param file_name: The title of the file. '.bz2' will be automatically appended.
    :param dir_path: The output directory in which to store the file.
    :return: None.
    """
    with bz2.BZ2File(os.path.join(dir_path, file_name + '.bz2'), 'wb') as file:
        pickle.dump(document_list, file)


if __name__ == "__main__":
    main()
