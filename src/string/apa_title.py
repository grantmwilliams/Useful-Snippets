import re
import itertools


def apa_title_case(text: str, minor_words: set[str] | None = None) -> str:
    """Convert a string to APA title case.
    Rules:
    - Capitalize the first word in the title.
    - Capitalize the first word after a colon, em dash, or end punctuation.
    - Capitalize both words in hyphenated compounds.
    - Capitalize words with four or more letters.
    - Capitalize words with less than four letters if they are not in our list of minor words.
    - Lowercase minor words.
    Args:
        text (str): The input text to convert to APA title case.
        minor_words (Optional[set[str]]): A set of minor words to lowercase. Defaults to None.
    Returns:
        str: The input text converted to APA title case.
    """

    # fmt: off
    minor_words =  minor_words or {
        "a", "an", "the", "and", "as", "but", "for", "if", "nor", "or", "so", "yet",
        "at", "by", "for", "in", "of", "off", "on", "per", "to", "up", "via"
    }
    punctuation = {
        "–", ",", "-", ":", "‑", ";", "–", "!", "—", "?"
    }
    # fmt: on

    split_pattern = r"(\s+|[\–\,\-\:\‑\;\–\!\—\?\(\)])"
    splits = re.split(split_pattern, text)

    if len(splits) == 1:
        return splits[
            0
        ].capitalize()  # if there's only one word, capitalize it and return

    capitalize_next = False
    title_parts = []
    for idx, (word, split) in enumerate(
        itertools.zip_longest(splits[0::2], splits[1::2], fillvalue="")
    ):
        if idx == 0:
            title_parts.append(word.capitalize())
            title_parts.append(split)
        elif capitalize_next and word != "":
            title_parts.append(word.capitalize())
            title_parts.append(split)
            capitalize_next = False
        elif len(word) > 3 or word.lower() not in minor_words:
            title_parts.append(word.capitalize())
            title_parts.append(split)
        else:
            title_parts.append(word.lower())
            title_parts.append(split)

        if split in punctuation:
            capitalize_next = True

    result = "".join(title_parts).strip()
    return result
