from src.string.apa_title import apa_title_case


def test_apa_title_case() -> None:
    # fmt: off
    assert apa_title_case("trust") == "Trust"
    assert apa_title_case("The quick brown fox jumps over the lazy dog") == "The Quick Brown Fox Jumps Over the Lazy Dog"
    assert apa_title_case("a study on the effects of caffeine: a meta-analysis") == "A Study on the Effects of Caffeine: A Meta-Analysis"
    assert apa_title_case("turning frowns (and smiles) upside down: a multilevel examination of surface acting positive and negative emotions on well-being") == "Turning Frowns (and Smiles) Upside Down: A Multilevel Examination of Surface Acting Positive and Negative Emotions on Well-Being"
    assert apa_title_case("Chicago tribune") == "Chicago Tribune"
    assert apa_title_case("a self-report study") == "A Self-Report Study" # ascii hyphen
    assert apa_title_case("a self–report study") == "A Self–Report Study" # unicode en dash
    assert apa_title_case("on the effectiveness of the methods") == "On the Effectiveness of the Methods"
    assert apa_title_case("a tale of two cities") == "A Tale of Two Cities"
    assert apa_title_case("to kill a mockingbird") == "To Kill a Mockingbird"
    assert apa_title_case("the catcher in the rye") == "The Catcher in the Rye"
    assert apa_title_case("lord of the flies") == "Lord of the Flies"
    assert apa_title_case("of mice and men") == "Of Mice and Men"
    assert apa_title_case("the grapes of wrath") == "The Grapes of Wrath"
    assert apa_title_case("the old man and the sea") == "The Old Man and the Sea"
    assert apa_title_case("train your mind for peak performance: a science-based approach for achieving your goals") == "Train Your Mind for Peak Performance: A Science-Based Approach for Achieving Your Goals"
    assert apa_title_case("a cross-sectional analysis of socio-economic factors") == "A Cross-Sectional Analysis of Socio-Economic Factors"
    # fmt: on
