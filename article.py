from __future__ import annotations

import re


class Article:

    def __init__(
        self,
        data: list[str]
    ):
        """
        Accepts as an argument a list of strings. Strings should be the
        unprocessed lines from a wikipedia text dump occurring within page
        tags. They should be in utf-8 format, not binary format. If
        whitespace was not stripped prior to passing them, it will be
        stripped during Page initialization.
        """
        self.lines = list()  # type: list[str]
        self.is_redirect_page = False  # type: bool
        self.namespace = int(re.findall(r'\d+', data[1])[0])
        self.title = None  # type: str | None
        for line in data:
            line = line.strip()
            self.lines.append(line)
            if line.startswith("<redirect title="):
                self.is_redirect_page = True
            if line.startswith('<title>'):
                match = re.search(r"<title>(.+?)</title>", line)
                self.title = match.group(1)
        self._paragraphs = None  # type: list[str] | None

    def get_paragraphs(
            self,
            namespaces: list[int] | None = None
    ) -> list[str] | None:
        # If this data has already been generated, don't waste resources
        # generating it again.
        if self._paragraphs is not None:
            return self._paragraphs

        # There are no useful paragraphs on redirect pages.
        if self.is_redirect_page:
            return None

        if namespaces is None:
            namespaces = [0]
        if self.namespace not in namespaces:
            return None

        paragraphs = list()

        for line in self.lines:
            """
            The exclusion criteria below are listed with the highest yielding 
            criteria first, to increase efficiency. For instance, there were 
            many hundreds of millions of lines in the test file starting with a
            vertical bar, 102 million blank lines, and 13.5 million lines 
            that were double end brackets.
            """
            # Ignore lines starting with vertical bars, used in ...
            if line.startswith('|'):
                continue
            # Ignore blank lines, of which there are many, and simultaneously
            # nix lines with a single character, of which there are quite a few
            if len(line) <= 1:
                continue
            # Ignore lines starting with the end marker for a template
            if line.startswith('}}'):
                continue
            # Ignore xml tags
            if line.startswith('<') or line.endswith('>'):
                continue
            # Ignore headers
            if line.startswith('==') and line.endswith('=='):
                continue
            # Ignore unordered and ordered lists
            if line.startswith('*') or line.startswith('#'):
                continue
            # Ignore description lists
            if line.startswith(';') or line.startswith(':'):
                continue
            # Ignore single line templates
            if (
                line.startswith('{{') and
                line.endswith('}}') and
                "{{" not in line[2:-2] and
                "}}" not in line[2: -2]
            ):
                continue
            # Ignore tables
            if line.startswith('!'):
                continue
            # Ignore categories
            if (
                    line.startswith('[[') and
                    (line.endswith(']]') or line.endswith(']]</text>'))
            ):
                continue
            # Ignore file links and descriptions
            if line.startswith('[[File:'):
                continue
            # Remove some equations
            if (line.startswith("&lt;math") and
                line.endswith("/math&gt;")):
                continue
            # Remove a particular style of making references
            if (line.startswith("&lt;ref name=") and
                line.endswith("/ref&gt;")):
                continue
            # Ignore lines starting with begin markers for templates,
            # but containing no end markers
            if line.startswith('{{') and '}}' not in line:
                continue
            # Eliminate inline documentation
            if line.startswith('&lt;!--') and line.endswith('--&gt;'):
                continue
            # ?
            if (
                len(line) < 100 and
                line.startswith('&lt;') and
                line.endswith('&gt;')
            ):
                continue
            # Ignore lines defining classes
            if line.startswith('{|'):
                continue
            # Ignore captions of some photos:
            if line.startswith('File:'):
                continue

            paragraphs.append(line)

        self._paragraphs = paragraphs
        return paragraphs

