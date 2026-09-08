from __future__ import annotations

from pathlib import Path
import json

import pytest

from casars import parameters
from casars.parameters import ParameterOrigin, SessionParameters, TaskParameters


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_catalog_and_definition_are_projected_from_rust() -> None:
    catalog = parameters.catalog()
    assert len(catalog["surfaces"]) == 41
    assert (
        len([surface for surface in catalog["surfaces"] if surface["kind"] == "task"])
        == 39
    )
    assert (
        len(
            [surface for surface in catalog["surfaces"] if surface["kind"] == "session"]
        )
        == 2
    )

    imager = parameters.definition("imager")
    assert imager["kind"] == "task"
    assert {binding["name"] for binding in imager["bindings"]} >= {
        "vis",
        "imagename",
        "imsize",
        "cell",
    }
    embedded = parameters.contract_bundle("imager")
    referenced = {
        binding["concept"]["id"] for binding in embedded["surface"]["bindings"]
    }
    assert {concept["id"] for concept in embedded["catalog"]["concepts"]} == referenced


def test_task_parameters_mutate_reset_save_reload_and_track_origins(
    tmp_path: Path,
) -> None:
    values = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    assert values.origins["mode"] is ParameterOrigin.DEFAULT
    assert [(item.code, item.parameter) for item in values.diagnostics] == [
        ("missing_required", "vis")
    ]

    values["vis"] = Path("example.ms")
    values["comment"] = "before editing"
    assert values["vis"] == ["example.ms"]
    assert values.origins["vis"] is ParameterOrigin.OVERRIDE
    assert values.is_dirty
    assert values.diagnostics == ()

    with pytest.raises(ValueError):
        values["vis"] = 12
    assert values["vis"] == ["example.ms"]

    profile = tmp_path / "flagmanager.toml"
    assert values.save(profile) == profile
    text = profile.read_text(encoding="utf-8")
    assert 'vis = ["example.ms"]' in text
    assert 'comment = "before editing"' in text
    assert "mode =" not in text

    loaded = TaskParameters.load("flagmanager", profile, workspace=tmp_path)
    assert loaded["comment"] == "before editing"
    assert loaded.origins["comment"] is ParameterOrigin.BASE_PROFILE
    loaded.reset("comment")
    assert loaded["comment"] == "none"
    assert loaded.origins["comment"] is ParameterOrigin.DEFAULT
    loaded.revert()
    assert loaded["comment"] == "before editing"

    profile.write_text(text.replace("before editing", "after reload"), encoding="utf-8")
    loaded.reload()
    assert loaded["comment"] == "after reload"

    inferred = parameters.load(profile, workspace=tmp_path)
    assert isinstance(inferred, TaskParameters)
    assert inferred.surface == "flagmanager"
    assert inferred["comment"] == "after reload"


def test_load_infers_surface_with_the_authoritative_toml_parser(tmp_path: Path) -> None:
    profile = tmp_path / "single-quoted.toml"
    profile.write_text(
        """[casars]
format = 1
surface = 'flagmanager'
kind = 'task'
contract = 1

[parameters]
vis = ['target.ms']
comment = 'parsed by Rust'
""",
        encoding="utf-8",
    )

    loaded = parameters.load(profile, workspace=tmp_path)
    assert isinstance(loaded, TaskParameters)
    assert loaded.surface == "flagmanager"
    assert loaded["vis"] == ["target.ms"]
    assert loaded["comment"] == "parsed by Rust"

    profile.write_text(
        """[casars]
format = 1
kind = 'task'
contract = 1

[parameters]
vis = ['target.ms']
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="parse parameter profile"):
        parameters.load(profile, workspace=tmp_path)


def test_last_and_last_successful_use_managed_workspace_slots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CASA_RS_STATE_DIR", raising=False)
    values = TaskParameters.defaults("flagmanager", workspace=tmp_path)
    values.set_many({"vis": "target.ms", "comment": "remember me"})
    assert values.write_last().name == "last.toml"
    assert values.write_last(successful=True).name == "last-successful.toml"

    attempted = TaskParameters.last("flagmanager", workspace=tmp_path)
    successful = TaskParameters.last_successful("flagmanager", workspace=tmp_path)
    assert attempted["comment"] == "remember me"
    assert successful["vis"] == ["target.ms"]

    with pytest.raises(TypeError):
        TaskParameters.defaults("imexplore")
    with pytest.raises(ValueError):
        SessionParameters.last_successful("imexplore", workspace=tmp_path)


def test_documented_template_is_non_activating_toml_reference() -> None:
    template = parameters.documented_template("flagmanager")
    assert "[parameters]" in template
    assert "# mode" in template


@pytest.mark.parametrize("surface", ["imager", "imexplore", "tablebrowser"])
def test_shared_cross_surface_profile_matches_canonical_expected_values(
    surface: str,
) -> None:
    profile = REPO_ROOT / f"resources/test-profiles/{surface}-cross-surface.toml"
    expected_path = profile.with_suffix(".expected.json")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    loaded = parameters.load(profile, workspace=REPO_ROOT)

    assert loaded.surface == expected["surface"]
    assert {name: loaded[name] for name in expected["values"]} == expected["values"]
    if surface == "imager":
        invocation = loaded.provider_invocation()
        assert invocation.protocol_name == "casa_imager_task"
        assert invocation.protocol_version == 7
        assert json.loads(invocation.stdin or "null") == expected["request"]
        unsupported = {reason.id for reason in invocation.unsupported_reasons}
        assert "task.aw_projection" not in unsupported
        assert unsupported >= {
            "task.grid_threads",
            "task.memory_target",
            "task.memory_pressure_policy",
        }


@pytest.mark.parametrize(
    ("name", "overrides", "expected_reason"),
    [
        ("continuum", {"write_preview_pngs": False}, None),
        (
            "cube",
            {
                "specmode": "cube",
                "channel_count": 4,
                "perchanweightdensity": False,
                "write_preview_pngs": False,
            },
            None,
        ),
            (
                "mosaic",
                {"gridder": "mosaic", "usepointing": True, "write_preview_pngs": False},
                None,
            ),
    ],
)
def test_imager_python_profiles_round_trip_exact_provider_requests(
    tmp_path: Path,
    name: str,
    overrides: dict[str, object],
    expected_reason: str | None,
) -> None:
    values = TaskParameters.defaults("imager", workspace=tmp_path)
    values.set_many(
        {"vis": f"{name}.ms", "imagename": f"products/{name}", **overrides}
    )
    before = values.provider_invocation()
    profile = values.save(tmp_path / f"{name}.toml")
    after = TaskParameters.load("imager", profile, workspace=tmp_path).provider_invocation()

    assert before == after
    assert before.protocol_name == "casa_imager_task"
    assert before.protocol_version == 7
    assert json.loads(before.stdin or "null")["request"]["measurement_set"] == f"{name}.ms"
    assert [reason.id for reason in before.unsupported_reasons] == (
        [] if expected_reason is None else [expected_reason]
    )
