from kaiano_common_utils import helpers

from live_music_history import placeholder


def test_import_common_utils():
    # Sanity check that the shared utils package is available
    assert hasattr(helpers, "try_lock_folder")


def test_placeholder():
    assert placeholder() is True
