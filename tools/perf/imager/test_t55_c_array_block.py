# SPDX-License-Identifier: LGPL-3.0-or-later
"""Selection boundaries for the opt-in C-array cube diagnostic."""

from types import SimpleNamespace

import pytest

from t55_c_array_block import selection


def test_middle_block_keeps_one_input_halo_channel():
    assert selection(SimpleNamespace(first_channel=240, channels=32)) == (
        240, 32, 272, 640_000)


def test_full_cube_uses_every_stored_channel_without_past_end_halo():
    assert selection(SimpleNamespace(first_channel=0, channels=512)) == (
        0, 512, 511, 10_240_000)


@pytest.mark.parametrize("first,count", [(-1, 1), (0, 0), (1, 512)])
def test_invalid_channel_range_is_rejected(first, count):
    with pytest.raises(AssertionError):
        selection(SimpleNamespace(first_channel=first, channels=count))
