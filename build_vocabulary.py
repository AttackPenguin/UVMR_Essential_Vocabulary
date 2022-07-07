from __future__ import annotations

import bz2
import csv
import os.path
from collections import Counter

import yaml


def main():
    build_vocabulary('my_build_vocabulary_config.yaml')


def build_vocabulary(
    config_file_path: str = './default_build_vocabulary_config.yaml'
):
    args = validate_configuration(config_file_path)

    corpus = Counter()
    file_names = os.listdir(args['corpus_dir_path'])
    num_corpus_files = len(file_names)

    print("Building vocabulary...")
    print(f"{num_corpus_files} corpus files found...\n")

    for file_num, file_name in enumerate(file_names):
        print(f"Processing File {file_num+1} of {num_corpus_files}...")
        file_path = os.path.join(args['corpus_dir_path'], file_name)
        with bz2.BZ2File(file_path, 'r') as file:
            for line in file:
                line = line.decode('utf-8')[:-1]
                corpus.update(line)

    print("\nCorpus construction complete.")
    print(f"{len(corpus)} tokens found.")
    if args['num_tokens'] is not None:
        corpus = corpus.most_common(args['num_tokens'])
        print(f"Corpus trimmed to most common {args['num_tokens']}")
    with open(args['vocab_file_path'], 'w') as file:
        field_names = ['token', 'count']
        writer = csv.DictWriter(file, field_names=field_names)
        writer.writeheader()
        for key, value in corpus.items():
            writer.writerow({'token': key, 'count': value})
    print("Corpus saved as csv file to:")
    print(f"\t{args['vocab_file_path']}")


def validate_configuration(
        config_file_path: str
) -> dict[str: str | int | None]:
    with open(config_file_path, 'r') as file:
        data = yaml.safe_load(file)

    return_values = dict()

    # Get directory of corpus files
    corpus_dir_path = data['corpus_dir_path']
    if type(corpus_dir_path) is not str:
        raise ValueError(
            "'corpus_dir_path' must be of type str."
        )
    if not os.path.isdir(corpus_dir_path):
        raise ValueError(
            "Directory specified by 'corpus_dir_path' does not exist."
        )
    if len(os.listdir(corpus_dir_path)) == 0:
        raise ValueError(
            "Directory specified by 'corpus_dir_path' is empty."
        )
    for file_name in os.listdir(corpus_dir_path):
        if file_name[-3:] != 'bz2':
            raise ValueError(
                "Directory specified by 'corpus_dir_path contains files that "
                "do not appear to be compressed corpus chunks.\n"
                "All files should be text files compressed via bzip2.\n"
                f"{file_name} does not appear to be a bzip2 compressed file."
            )
    return_values['corpus_dir_path'] = corpus_dir_path

    # Get number of tokens to include in vocabulary.
    # if an integer n is provided, then the most frequent n tokens will be
    # returned.
    # if None is provided, then all tokens will be returned.
    num_tokens = data['num_tokens']
    if type(num_tokens) is None:
        return_values['num_tokens'] = None
    elif type(num_tokens) is int:
        if num_tokens <= 0:
            raise ValueError(
                "'num_tokens' must be either None or a positive integer."
            )
    else:
        raise ValueError(
            "'num_tokens' must be either None or a positive integer."
        )

    # Get vocab_file_path
    vocab_file_path = data['vocab_file_path']
    if type(vocab_file_path) is not str:
        raise ValueError(
            "'vocab_file_path' should be of type str and specify a path to a "
            "file in an existing directory."
        )
    elif not os.path.isdir(os.path.dirname(vocab_file_path)):
        raise ValueError(
            "'vocab_file_path' specifies a location in a directory that does "
            "not exist."
        )
    else:
        return_values['vocab_file_path'] = vocab_file_path

    return return_values


if __name__ == '__main__':
    main()