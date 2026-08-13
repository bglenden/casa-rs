# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the checked-in CASA tclean JSON protocol."""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from perf_harness import casa_tclean


CASA_VERSION = "6.7.5.9"


class CasaTcleanRecipeTests(unittest.TestCase):
    def test_literal_recipe_parser_ignores_comments_and_rejects_code(self) -> None:
        parsed = casa_tclean.parse_literal_assignment_recipe(
            "taskname = 'tclean'\nfield = '1525'\n#tclean(field='evil')\n"
        )
        self.assertEqual({"taskname": "tclean", "field": "1525"}, parsed)

        for unsafe in (
            "taskname = 'tclean'\nvalue = make_value()\n",
            "taskname = 'tclean'\nprint('side effect')\n",
            "taskname = 'tclean'\ntaskname = 'again'\n",
        ):
            with self.subTest(unsafe=unsafe):
                with self.assertRaises(casa_tclean.ProtocolError):
                    casa_tclean.parse_literal_assignment_recipe(unsafe)

    def test_recipe_requires_exact_hash_task_and_parameter_names(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root)
            recipe = casa_tclean.load_validated_recipe(request["recipe"])
            self.assertEqual("tclean", recipe["task"])
            self.assertEqual(
                sorted(recipe["archived_parameters"]),
                request["recipe"]["parameter_names"],
            )

            wrong_hash = copy.deepcopy(request["recipe"])
            wrong_hash["sha256"] = "0" * 64
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "sha256 mismatch"):
                casa_tclean.load_validated_recipe(wrong_hash)

            wrong_names = copy.deepcopy(request["recipe"])
            wrong_names["parameter_names"] = sorted(
                request["recipe"]["parameter_names"] + ["not_archived"]
            )
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "parameter-name mismatch"
            ):
                casa_tclean.load_validated_recipe(wrong_names)

            wrong_task_path = root / "wrong-task.last"
            wrong_task_path.write_text(recipe_text().replace("'tclean'", "'clean'", 1))
            wrong_task = copy.deepcopy(request["recipe"])
            wrong_task["path"] = str(wrong_task_path)
            wrong_task["sha256"] = sha256_file(wrong_task_path)
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "taskname mismatch"):
                casa_tclean.load_validated_recipe(wrong_task)

    def test_plan_records_compatibility_defaults_overrides_and_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root)
            plan = casa_tclean.build_invocation_plan(request)

            self.assertEqual("planned", plan["status"])
            self.assertNotIn("chanchunks", plan["effective_kwargs"])
            self.assertEqual([0.0], plan["effective_kwargs"]["pointingoffsetsigdev"])
            self.assertEqual("data", plan["effective_kwargs"]["datacolumn"])
            self.assertFalse(plan["effective_kwargs"]["interactive"])
            self.assertEqual(
                casa_tclean.CASA_6_7_5_9_NEW_DEFAULTS, plan["version_defaults"]
            )
            self.assertEqual(
                casa_tclean.canonical_sha256(plan["effective_kwargs"]),
                plan["effective_kwargs_sha256"],
            )
            normalization_names = {
                item["parameter"] for item in plan["compatibility_normalizations"]
            }
            self.assertEqual(
                {"chanchunks", "pointingoffsetsigdev"}, normalization_names
            )

            reordered = copy.deepcopy(request)
            reordered["overrides"] = dict(reversed(list(request["overrides"].items())))
            self.assertEqual(
                plan["effective_kwargs_sha256"],
                casa_tclean.build_invocation_plan(reordered)["effective_kwargs_sha256"],
            )

    def test_only_lossless_archived_compatibility_normalizations_are_accepted(
        self,
    ) -> None:
        archived = {
            "chanchunks": 2,
            "pointingoffsetsigdev": 0.0,
        }
        with self.assertRaisesRegex(casa_tclean.ProtocolError, "chanchunks"):
            casa_tclean.normalize_archived_parameters(archived, {})

        archived["chanchunks"] = 1
        archived["pointingoffsetsigdev"] = 1.0
        with self.assertRaisesRegex(casa_tclean.ProtocolError, "pointingoffsetsigdev"):
            casa_tclean.normalize_archived_parameters(archived, {})

        archived["pointingoffsetsigdev"] = [0.0, 0.0]
        effective, normalizations, _ = casa_tclean.normalize_archived_parameters(
            archived, {}
        )
        self.assertEqual([0.0, 0.0], effective["pointingoffsetsigdev"])
        self.assertEqual(["chanchunks"], [item["parameter"] for item in normalizations])

        archived["fullsummary"] = False
        with self.assertRaisesRegex(casa_tclean.ProtocolError, "unexpectedly contains"):
            casa_tclean.normalize_archived_parameters(archived, {})

    def test_override_allowlist_and_value_constraints_are_strict(self) -> None:
        with self.assertRaisesRegex(casa_tclean.ProtocolError, "non-approved"):
            casa_tclean.validate_reproducibility_overrides({"aterm": False})
        for overrides, pattern in (
            ({"vis": "relative.ms"}, "absolute"),
            ({"datacolumn": "corrected"}, "exactly 'data'"),
            ({"parallel": True}, "must be false"),
            ({"niter": -1}, "non-negative"),
            ({"imsize": [1024, 0]}, "positive"),
            ({"mask": "relative.mask"}, "absolute"),
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(casa_tclean.ProtocolError, pattern):
                    casa_tclean.validate_reproducibility_overrides(overrides)

    def test_request_rejects_unknown_fields_and_bad_cache_plan_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root)
            request["private_fallback"] = True
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "field mismatch"):
                casa_tclean.build_invocation_plan(request)

    def test_cache_plan_is_rederived_from_the_exact_effective_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root)
            changed_call = copy.deepcopy(request)
            changed_call["overrides"]["field"] = "changed-field"
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "CF parameters"):
                casa_tclean.build_invocation_plan(changed_call)

            for field in casa_tclean.CF_CACHE_PARAMETER_FIELDS:
                with self.subTest(field=field):
                    changed = copy.deepcopy(request)
                    value = changed["cache"]["plan"]["cf_parameters"][field]
                    if isinstance(value, bool):
                        replacement = not value
                    elif isinstance(value, int):
                        replacement = value + 1
                    elif isinstance(value, float):
                        replacement = value + 1.0
                    elif isinstance(value, str):
                        replacement = value + "-changed"
                    elif isinstance(value, list):
                        replacement = [*value, "changed"]
                    else:
                        self.fail(f"unsupported fixture value for {field}: {value!r}")
                    changed["cache"]["plan"]["cf_parameters"][field] = replacement
                    changed["cache"]["plan_sha256"] = casa_tclean.canonical_sha256(
                        changed["cache"]["plan"]
                    )
                    with self.assertRaises(casa_tclean.ProtocolError):
                        casa_tclean.build_invocation_plan(changed)

            mutations = {
                "schema_version": lambda plan: plan.__setitem__("schema_version", 2),
                "kind": lambda plan: plan.__setitem__("kind", "other"),
                "casa_version": lambda plan: plan.__setitem__(
                    "casa_version", "6.8.0.0"
                ),
                "recipe_sha256": lambda plan: plan.__setitem__(
                    "recipe_sha256", "0" * 64
                ),
                "dataset": lambda plan: plan.__setitem__(
                    "dataset", {"key": "fixture", "path": str(root / "other.ms")}
                ),
            }
            for name, mutate in mutations.items():
                with self.subTest(envelope=name):
                    changed = copy.deepcopy(request)
                    mutate(changed["cache"]["plan"])
                    changed["cache"]["plan_sha256"] = casa_tclean.canonical_sha256(
                        changed["cache"]["plan"]
                    )
                    with self.assertRaises(casa_tclean.ProtocolError):
                        casa_tclean.build_invocation_plan(changed)

            request = make_request(root)
            request["cache"]["plan_sha256"] = "f" * 64
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "plan sha256 mismatch"
            ):
                casa_tclean.build_invocation_plan(request)


class CasaTcleanCacheAndInventoryTests(unittest.TestCase):
    def test_cold_cache_requires_absent_cache_and_receipt_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            cache = casa_tclean.build_invocation_plan(make_request(root))["cache"]
            before = casa_tclean.validate_cache_precondition(cache)
            self.assertFalse(before["inventory"]["exists"])
            self.assertFalse(before["working_inventory"]["exists"])
            self.assertNotEqual(cache["path"], cache["working_path"])
            self.assertTrue(cache["working_path"].endswith(".partial"))

            pathlib.Path(cache["path"]).mkdir()
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "must be absent"):
                casa_tclean.validate_cache_precondition(cache)

            pathlib.Path(cache["path"]).rmdir()
            pathlib.Path(cache["receipt_path"]).write_text("{}")
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "receipt must be absent"
            ):
                casa_tclean.validate_cache_precondition(cache)

            pathlib.Path(cache["receipt_path"]).unlink()
            pathlib.Path(cache["working_path"]).mkdir()
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "working path must be absent"
            ):
                casa_tclean.validate_cache_precondition(cache)

    def test_tree_digest_excludes_only_table_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary) / "tree"
            nested = root / "CFS0"
            nested.mkdir(parents=True)
            (nested / "table.dat").write_bytes(b"science")
            (nested / "table.lock").write_bytes(b"first lock")
            (root / ".DS_Store").write_bytes(b"included")

            first = casa_tclean.tree_inventory(root)
            self.assertEqual(
                ["CFS0/table.lock"],
                [item["relative_path"] for item in first["excluded_volatile"]],
            )
            self.assertIn(
                ".DS_Store", [item["relative_path"] for item in first["entries"]]
            )

            (nested / "table.lock").write_bytes(b"different volatile lock")
            second = casa_tclean.tree_inventory(root)
            self.assertEqual(first["stable_tree_sha256"], second["stable_tree_sha256"])

            (root / ".DS_Store").write_bytes(b"changed stable content")
            third = casa_tclean.tree_inventory(root)
            self.assertNotEqual(
                second["stable_tree_sha256"], third["stable_tree_sha256"]
            )

    def test_product_inventory_discovers_every_exact_prefix_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            prefix = root / "casa"
            for suffix in (".image.tt0", ".psf.tt0", ".sumwt.tt2"):
                product = pathlib.Path(str(prefix) + suffix)
                product.mkdir()
                (product / "table.dat").write_text(suffix)
                (product / "table.lock").write_text("volatile")
            unrelated = root / "casa-other"
            unrelated.mkdir()

            inventory = casa_tclean.inventory_product_siblings(prefix)
            self.assertEqual(
                [".image.tt0", ".psf.tt0", ".sumwt.tt2"],
                [item["suffix"] for item in inventory],
            )
            self.assertTrue(
                all(item["inventory"]["stable_tree_sha256"] for item in inventory)
            )

    def test_cold_then_warm_fake_execution_links_exact_cache_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            cold_request = make_request(root, action="run")

            def cold_tclean(**kwargs):
                cache = pathlib.Path(kwargs["cfcache"])
                self.assertTrue(cache.name.endswith(".partial"))
                self.assertNotEqual(pathlib.Path(cold_request["cache"]["path"]), cache)
                self.assertFalse(pathlib.Path(cold_request["cache"]["path"]).exists())
                cfs = cache / "CFS0"
                cfs.mkdir(parents=True)
                (cfs / "table.dat").write_bytes(b"frozen-cf")
                (cfs / "table.lock").write_bytes(b"cold-lock")
                product = pathlib.Path(kwargs["imagename"] + ".psf.tt0")
                product.mkdir(parents=True)
                (product / "table.dat").write_bytes(b"cold-product")
                return {"ok": True}

            cold = casa_tclean.process_request(
                cold_request, tclean_task=cold_tclean, casa_version=CASA_VERSION
            )
            self.assertEqual("completed", cold["status"])
            self.assertGreaterEqual(cold["wall_seconds"], 0.0)
            self.assertGreater(cold["resources"]["after"]["peak_rss_bytes"], 0)
            self.assertEqual(
                [".psf.tt0"], [p["suffix"] for p in cold["products"]["after"]]
            )
            receipt_path = pathlib.Path(cold_request["cache"]["receipt_path"])
            self.assertTrue(receipt_path.is_file())
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            expected_producer = casa_tclean.cold_publication_identity(
                request_id=cold["request_id"],
                effective_kwargs_sha256=cold["effective_kwargs_sha256"],
                products=cold["products"]["after"],
            )
            self.assertEqual(casa_tclean.CACHE_RECEIPT_SCHEMA_VERSION, 2)
            self.assertEqual(expected_producer, receipt["producer"])
            self.assertEqual(
                casa_tclean.canonical_sha256(receipt["producer"]["product_inventory"]),
                receipt["producer"]["product_inventory_sha256"],
            )
            final_cache = pathlib.Path(cold_request["cache"]["path"])
            self.assertTrue(final_cache.is_dir())
            self.assertFalse(pathlib.Path(cold["cache"]["working_path"]).exists())
            self.assertEqual(
                str(final_cache), cold["cache"]["after"]["inventory"]["root"]
            )
            cold_digest = cold["cache"]["after"]["inventory"]["stable_tree_sha256"]

            warm_request = make_request(root, action="run", write_recipe=False)
            warm_request["overrides"]["imagename"] = str(root / "products" / "warm")
            warm_request["cache"] = {
                **cold_request["cache"],
                "role": "warm",
                "expected_stable_tree_sha256": cold_digest,
            }

            def warm_tclean(**kwargs):
                cache = pathlib.Path(kwargs["cfcache"])
                (cache / "CFS0" / "table.lock").write_bytes(b"warm-lock-changed")
                product = pathlib.Path(kwargs["imagename"] + ".image.tt0")
                product.mkdir(parents=True)
                (product / "table.dat").write_bytes(b"warm-product")

            warm = casa_tclean.process_request(
                warm_request, tclean_task=warm_tclean, casa_version=CASA_VERSION
            )
            self.assertEqual("completed", warm["status"])
            self.assertEqual(
                cold_digest,
                warm["cache"]["before"]["inventory"]["stable_tree_sha256"],
            )
            self.assertEqual(
                cold_digest,
                warm["cache"]["after"]["inventory"]["stable_tree_sha256"],
            )

    def test_cold_cache_receipt_publication_failure_is_idempotently_recoverable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            cache, before, publication = self._staged_cold_cache(root)
            working_cache = pathlib.Path(cache["working_path"])
            final_cache = pathlib.Path(cache["path"])
            working_receipt = pathlib.Path(cache["working_receipt_path"])
            final_receipt = pathlib.Path(cache["receipt_path"])
            real_replace = casa_tclean.os.replace

            def fail_receipt_publication(source, destination):
                if (
                    pathlib.Path(source) == working_receipt
                    and pathlib.Path(destination) == final_receipt
                ):
                    raise OSError("injected receipt publication failure")
                return real_replace(source, destination)

            with mock.patch.object(
                casa_tclean.os, "replace", side_effect=fail_receipt_publication
            ):
                with self.assertRaisesRegex(
                    casa_tclean.ProtocolError, "receipt remain retryable"
                ):
                    casa_tclean.validate_cache_postcondition(
                        cache, before, **publication
                    )

            self.assertTrue(working_cache.is_dir())
            self.assertTrue(working_receipt.is_file())
            self.assertFalse(final_cache.exists())
            self.assertFalse(final_receipt.exists())

            recovered = casa_tclean.validate_cache_postcondition(
                cache, before, **publication
            )
            self.assertEqual("cold", recovered["role"])
            self.assertTrue(final_cache.is_dir())
            self.assertTrue(final_receipt.is_file())
            self.assertFalse(working_cache.exists())
            self.assertFalse(working_receipt.exists())

    def test_cold_cache_publication_failure_rolls_back_and_retries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            cache, before, publication = self._staged_cold_cache(root)
            working_cache = pathlib.Path(cache["working_path"])
            final_cache = pathlib.Path(cache["path"])
            working_receipt = pathlib.Path(cache["working_receipt_path"])
            final_receipt = pathlib.Path(cache["receipt_path"])
            real_replace = casa_tclean.os.replace

            def fail_cache_publication(source, destination):
                if (
                    pathlib.Path(source) == working_cache
                    and pathlib.Path(destination) == final_cache
                ):
                    raise OSError("injected cache publication failure")
                return real_replace(source, destination)

            with mock.patch.object(
                casa_tclean.os, "replace", side_effect=fail_cache_publication
            ):
                with self.assertRaisesRegex(
                    casa_tclean.ProtocolError, "receipt was rolled back"
                ):
                    casa_tclean.validate_cache_postcondition(
                        cache, before, **publication
                    )

            self.assertTrue(working_cache.is_dir())
            self.assertTrue(working_receipt.is_file())
            self.assertFalse(final_cache.exists())
            self.assertFalse(final_receipt.exists())

            recovered = casa_tclean.validate_cache_postcondition(
                cache, before, **publication
            )
            self.assertEqual("cold", recovered["role"])
            self.assertTrue(final_cache.is_dir())
            self.assertTrue(final_receipt.is_file())

    def test_cold_cache_failed_rollback_retains_resumable_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            cache, before, publication = self._staged_cold_cache(root)
            working_cache = pathlib.Path(cache["working_path"])
            final_cache = pathlib.Path(cache["path"])
            working_receipt = pathlib.Path(cache["working_receipt_path"])
            final_receipt = pathlib.Path(cache["receipt_path"])
            real_replace = casa_tclean.os.replace

            def fail_publication_and_rollback(source, destination):
                pair = (pathlib.Path(source), pathlib.Path(destination))
                if pair == (working_cache, final_cache):
                    raise OSError("injected cache publication failure")
                if pair == (final_receipt, working_receipt):
                    raise OSError("injected receipt rollback failure")
                return real_replace(source, destination)

            with mock.patch.object(
                casa_tclean.os, "replace", side_effect=fail_publication_and_rollback
            ):
                with self.assertRaisesRegex(
                    casa_tclean.ProtocolError, "rollback also failed"
                ):
                    casa_tclean.validate_cache_postcondition(
                        cache, before, **publication
                    )

            self.assertTrue(working_cache.is_dir())
            self.assertTrue(final_receipt.is_file())
            self.assertFalse(final_cache.exists())
            self.assertFalse(working_receipt.exists())

            (working_cache / "CFS0" / "table.lock").write_bytes(
                b"volatile lock after interrupted publication"
            )
            recovered = casa_tclean.validate_cache_postcondition(
                cache, before, **publication
            )
            self.assertEqual("cold", recovered["role"])
            self.assertTrue(final_cache.is_dir())
            self.assertTrue(final_receipt.is_file())
            self.assertFalse(working_cache.exists())

    def test_exact_run_request_replay_recovers_every_publication_failure(
        self,
    ) -> None:
        for failure_mode in ("receipt", "cache", "cache_and_rollback"):
            with self.subTest(failure_mode=failure_mode):
                with tempfile.TemporaryDirectory() as temporary:
                    root = pathlib.Path(temporary)
                    request = make_request(root, action="run")
                    plan = casa_tclean.build_invocation_plan(request)
                    cache = plan["cache"]
                    working_cache = pathlib.Path(cache["working_path"])
                    final_cache = pathlib.Path(cache["path"])
                    working_receipt = pathlib.Path(cache["working_receipt_path"])
                    final_receipt = pathlib.Path(cache["receipt_path"])
                    real_replace = casa_tclean.os.replace
                    tclean_calls = 0

                    def fake_tclean(**kwargs):
                        nonlocal tclean_calls
                        tclean_calls += 1
                        stable = pathlib.Path(kwargs["cfcache"]) / "CFS0" / "table.dat"
                        stable.parent.mkdir(parents=True)
                        stable.write_bytes(b"frozen-cf")
                        product = pathlib.Path(kwargs["imagename"] + ".image.tt0")
                        product.mkdir(parents=True)
                        (product / "table.dat").write_bytes(b"completed-image")

                    def inject_publication_failure(source, destination):
                        pair = (pathlib.Path(source), pathlib.Path(destination))
                        if failure_mode == "receipt" and pair == (
                            working_receipt,
                            final_receipt,
                        ):
                            raise OSError("injected receipt publication failure")
                        if failure_mode in {"cache", "cache_and_rollback"} and pair == (
                            working_cache,
                            final_cache,
                        ):
                            raise OSError("injected cache publication failure")
                        if failure_mode == "cache_and_rollback" and pair == (
                            final_receipt,
                            working_receipt,
                        ):
                            raise OSError("injected receipt rollback failure")
                        return real_replace(source, destination)

                    with mock.patch.object(
                        casa_tclean.os,
                        "replace",
                        side_effect=inject_publication_failure,
                    ):
                        failed = casa_tclean.process_request(
                            request,
                            tclean_task=fake_tclean,
                            casa_version=CASA_VERSION,
                        )

                    self.assertEqual("failed_postcondition", failed["status"])
                    self.assertEqual("cache", failed["failure"]["kind"])
                    self.assertEqual(1, tclean_calls)
                    self.assertTrue(working_cache.is_dir())
                    self.assertFalse(final_cache.exists())
                    self.assertTrue(
                        working_receipt.is_file() or final_receipt.is_file()
                    )

                    recovered = casa_tclean.process_request(
                        request,
                        tclean_task=fake_tclean,
                        casa_version=CASA_VERSION,
                    )

                    self.assertEqual("recovered_publication", recovered["status"])
                    casa_tclean.validate_result_for_request(recovered, request)
                    self.assertEqual(1, tclean_calls)
                    self.assertFalse(
                        recovered["casa"]["publication_recovery"]["tclean_reinvoked"]
                    )
                    self.assertEqual(0.0, recovered["wall_seconds"])
                    self.assertTrue(final_cache.is_dir())
                    self.assertTrue(final_receipt.is_file())
                    self.assertFalse(working_cache.exists())
                    self.assertFalse(working_receipt.exists())

                    reinvoked = copy.deepcopy(recovered)
                    reinvoked["casa"]["publication_recovery"]["tclean_reinvoked"] = True
                    with self.assertRaisesRegex(
                        casa_tclean.ProtocolError, "non-reinvocation"
                    ):
                        casa_tclean.validate_result_for_request(reinvoked, request)

    def test_publication_recovery_binds_stable_products_but_ignores_locks(
        self,
    ) -> None:
        for mutation in ("stable_product", "volatile_lock"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = pathlib.Path(temporary)
                request = make_request(root, action="run")
                plan = casa_tclean.build_invocation_plan(request)
                working_receipt = pathlib.Path(plan["cache"]["working_receipt_path"])
                final_receipt = pathlib.Path(plan["cache"]["receipt_path"])
                real_replace = casa_tclean.os.replace
                tclean_calls = 0

                def fake_tclean(**kwargs):
                    nonlocal tclean_calls
                    tclean_calls += 1
                    cache_data = pathlib.Path(kwargs["cfcache"]) / "CFS0" / "table.dat"
                    cache_data.parent.mkdir(parents=True)
                    cache_data.write_bytes(b"frozen-cf")
                    product = pathlib.Path(kwargs["imagename"] + ".image.tt0")
                    product.mkdir(parents=True)
                    (product / "table.dat").write_bytes(b"frozen-image")

                def fail_receipt_publication(source, destination):
                    if (
                        pathlib.Path(source) == working_receipt
                        and pathlib.Path(destination) == final_receipt
                    ):
                        raise OSError("injected receipt publication failure")
                    return real_replace(source, destination)

                with mock.patch.object(
                    casa_tclean.os,
                    "replace",
                    side_effect=fail_receipt_publication,
                ):
                    failed = casa_tclean.process_request(
                        request,
                        tclean_task=fake_tclean,
                        casa_version=CASA_VERSION,
                    )
                self.assertEqual("failed_postcondition", failed["status"])
                product = pathlib.Path(
                    plan["effective_kwargs"]["imagename"] + ".image.tt0"
                )
                if mutation == "stable_product":
                    (product / "table.dat").write_bytes(b"changed-image")
                    with self.assertRaisesRegex(
                        casa_tclean.ProtocolError, "producer request, products"
                    ):
                        casa_tclean.process_request(
                            request,
                            tclean_task=fake_tclean,
                            casa_version=CASA_VERSION,
                        )
                    self.assertFalse(pathlib.Path(plan["cache"]["path"]).exists())
                else:
                    (product / "table.lock").write_bytes(b"volatile")
                    recovered = casa_tclean.process_request(
                        request,
                        tclean_task=fake_tclean,
                        casa_version=CASA_VERSION,
                    )
                    self.assertEqual("recovered_publication", recovered["status"])
                self.assertEqual(1, tclean_calls)

    @staticmethod
    def _staged_cold_cache(
        root: pathlib.Path,
    ) -> tuple[dict, dict, dict]:
        plan = casa_tclean.build_invocation_plan(make_request(root))
        cache = plan["cache"]
        before = casa_tclean.validate_cache_precondition(cache)
        stable = pathlib.Path(cache["working_path"]) / "CFS0" / "table.dat"
        stable.parent.mkdir(parents=True)
        stable.write_bytes(b"frozen-cf")
        product = pathlib.Path(plan["effective_kwargs"]["imagename"] + ".image.tt0")
        product.mkdir(parents=True)
        (product / "table.dat").write_bytes(b"frozen-image")
        return (
            cache,
            before,
            {
                "products": casa_tclean.inventory_product_siblings(
                    plan["effective_kwargs"]["imagename"]
                ),
                "request_id": plan["request_id"],
                "effective_kwargs_sha256": plan["effective_kwargs_sha256"],
            },
        )

    def test_warm_cache_rejects_content_or_receipt_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root)
            plan = casa_tclean.build_invocation_plan(request)
            cache = pathlib.Path(plan["cache"]["path"])
            cache.mkdir()
            (cache / "CFS").write_bytes(b"cache")
            inventory = casa_tclean.tree_inventory(cache)
            receipt = {
                "schema_version": 1,
                "kind": casa_tclean.CACHE_RECEIPT_KIND,
                "cache_path": str(cache),
                "plan_sha256": plan["cache"]["plan_sha256"],
                "stable_tree_sha256": inventory["stable_tree_sha256"],
                "inventory": inventory,
            }
            pathlib.Path(plan["cache"]["receipt_path"]).write_text(json.dumps(receipt))
            warm = {
                **plan["cache"],
                "role": "warm",
                "expected_stable_tree_sha256": inventory["stable_tree_sha256"],
            }
            self.assertEqual(
                "warm", casa_tclean.validate_cache_precondition(warm)["role"]
            )

            (cache / "CFS").write_bytes(b"changed")
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "contents do not match"
            ):
                casa_tclean.validate_cache_precondition(warm)

            warm["expected_stable_tree_sha256"] = "0" * 64
            with self.assertRaisesRegex(casa_tclean.ProtocolError, "request digest"):
                casa_tclean.validate_cache_precondition(warm)


class CasaTcleanExecutionTests(unittest.TestCase):
    def test_completed_result_is_strictly_bound_to_its_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root, action="run")

            def fake_tclean(**kwargs):
                cache = pathlib.Path(kwargs["cfcache"])
                cache.mkdir(parents=True)
                (cache / "CFS").write_bytes(b"cache")
                product = pathlib.Path(kwargs["imagename"] + ".image.tt0")
                product.mkdir(parents=True)
                (product / "table.dat").write_bytes(b"image")

            result = casa_tclean.process_request(
                request, tclean_task=fake_tclean, casa_version=CASA_VERSION
            )
            casa_tclean.validate_result_for_request(result, request)

            mutations = {
                "request_id": lambda value: value.__setitem__(
                    "request_id", "different-request"
                ),
                "effective_call": lambda value: value["effective_kwargs"].__setitem__(
                    "field", "different-field"
                ),
                "cache": lambda value: value["cache"].__setitem__(
                    "plan_sha256", "0" * 64
                ),
                "product": lambda value: value["products"]["after"][0].__setitem__(
                    "path", "/unbound/product.image.tt0"
                ),
            }
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    changed = copy.deepcopy(result)
                    mutate(changed)
                    with self.assertRaises(casa_tclean.ProtocolError):
                        casa_tclean.validate_result_for_request(changed, request)

            omitted = copy.deepcopy(result)
            del omitted["products"]
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "missing=.*products"
            ):
                casa_tclean.validate_result_for_request(omitted, request)

    def test_completed_result_stage_and_resource_evidence_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root, action="run")

            def fake_tclean(**kwargs):
                cache = pathlib.Path(kwargs["cfcache"])
                cache.mkdir(parents=True)
                (cache / "CFS").write_bytes(b"cache")
                product = pathlib.Path(kwargs["imagename"] + ".image.tt0")
                product.mkdir(parents=True)
                (product / "table.dat").write_bytes(b"image")

            result = casa_tclean.process_request(
                request, tclean_task=fake_tclean, casa_version=CASA_VERSION
            )
            self.assertEqual(
                set(casa_tclean.STAGE_TIMING_FIELDS),
                set(result["stage_timings_seconds"]),
            )
            self.assertIn(
                result["resources"]["disk_io_source"],
                {"darwin_proc_pid_rusage_v2", "linux_proc_self_io"},
            )
            self.assertGreater(result["resources"]["after"]["peak_rss_bytes"], 0)

            def missing_stage(value):
                del value["stage_timings_seconds"]["product_inventory"]

            def extra_resource(value):
                value["resources"]["delta"]["unbound"] = 1

            def nan_stage(value):
                value["stage_timings_seconds"]["protocol_preflight"] = float("nan")

            def negative_stage(value):
                value["stage_timings_seconds"]["cache_postcondition"] = -1.0

            def boolean_counter(value):
                value["resources"]["after"]["disk_read_bytes"] = True

            def wrong_delta(value):
                value["resources"]["delta"]["disk_write_bytes"] += 1

            def regressing_counter(value):
                value["resources"]["before"]["minor_page_faults"] = (
                    value["resources"]["after"]["minor_page_faults"] + 1
                )

            def wall_mismatch(value):
                value["wall_seconds"] += 1.0

            def short_total(value):
                value["stage_timings_seconds"]["protocol_total"] = 0.0

            mutations = {
                "missing_stage": missing_stage,
                "extra_resource": extra_resource,
                "nan_stage": nan_stage,
                "negative_stage": negative_stage,
                "boolean_counter": boolean_counter,
                "wrong_delta": wrong_delta,
                "regressing_counter": regressing_counter,
                "wall_mismatch": wall_mismatch,
                "short_total": short_total,
            }
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    changed = copy.deepcopy(result)
                    mutate(changed)
                    with self.assertRaises(casa_tclean.ProtocolError):
                        casa_tclean.validate_result_envelope(changed)

    def test_runtime_identity_is_verified_before_tclean_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root, action="run")
            frozen_identity = runtime_identity_fixture()
            stable_identity = casa_tclean.stable_identity_projection(frozen_identity)
            request["cache"]["plan"]["runtime_identity"] = {
                "identity": stable_identity,
                "identity_sha256": casa_tclean.stable_identity_sha256(stable_identity),
            }
            request["cache"]["plan_sha256"] = casa_tclean.canonical_sha256(
                request["cache"]["plan"]
            )
            called = False

            def fake_tclean(**kwargs):
                nonlocal called
                called = True
                cache = pathlib.Path(kwargs["cfcache"])
                cache.mkdir(parents=True)
                (cache / "CFS").write_bytes(b"cache")
                product = pathlib.Path(kwargs["imagename"] + ".psf.tt0")
                product.mkdir(parents=True)
                (product / "table.dat").write_bytes(b"product")

            completed = casa_tclean.process_request(
                request,
                tclean_task=fake_tclean,
                casa_version=CASA_VERSION,
                runtime_identity=copy.deepcopy(frozen_identity),
            )
            self.assertEqual("matched", completed["casa"]["runtime_identity"]["status"])
            self.assertTrue(called)

            mismatch_root = root / "mismatch"
            mismatch_root.mkdir()
            mismatch = make_request(mismatch_root, action="run")
            mismatch["cache"]["plan"]["runtime_identity"] = request["cache"]["plan"][
                "runtime_identity"
            ]
            mismatch["cache"]["plan_sha256"] = casa_tclean.canonical_sha256(
                mismatch["cache"]["plan"]
            )
            called = False
            changed_identity = copy.deepcopy(frozen_identity)
            changed_identity["modules"]["casatasks"]["code_tree"]["tree_sha256"] = (
                "f" * 64
            )
            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "runtime/data identity mismatch"
            ):
                casa_tclean.process_request(
                    mismatch,
                    tclean_task=fake_tclean,
                    casa_version=CASA_VERSION,
                    runtime_identity=changed_identity,
                )
            self.assertFalse(called)

    def test_file_mask_is_rehashed_immediately_before_tclean(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            mask = root / "clean.mask"
            mask.write_bytes(b"frozen mask")
            request = make_request(root, action="run")
            request["overrides"]["mask"] = str(mask)
            request["mask_identity"] = {
                "kind": "file",
                "sha256": sha256_file(mask),
                "identity": {"size_bytes": mask.stat().st_size},
            }
            mask.write_bytes(b"changed mask")
            called = False

            def fake_tclean(**_kwargs):
                nonlocal called
                called = True

            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "mask identity mismatch"
            ):
                casa_tclean.process_request(
                    request, tclean_task=fake_tclean, casa_version=CASA_VERSION
                )
            self.assertFalse(called)

    def test_casa_image_mask_tree_is_rehashed_before_tclean(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            mask = root / "clean.mask"
            mask.mkdir()
            (mask / "table.dat").write_bytes(b"frozen mask")
            (mask / "table.lock").write_bytes(b"volatile lock")
            frozen = casa_tclean.tree_identity(mask, excluded_names={"table.lock"})
            request = make_request(root, action="run")
            request["overrides"]["mask"] = str(mask)
            request["mask_identity"] = {
                "kind": "casa_image_tree",
                "sha256": frozen["tree_sha256"],
                "identity": frozen,
            }
            (mask / "table.lock").write_bytes(b"changed volatile lock")
            self.assertEqual(
                "matched",
                casa_tclean._validate_mask_identity(
                    casa_tclean.build_invocation_plan(request)["mask_identity"],
                    effective_kwargs=request["overrides"],
                )["status"],
            )
            (mask / "table.dat").write_bytes(b"changed mask")
            called = False

            def fake_tclean(**_kwargs):
                nonlocal called
                called = True

            with self.assertRaisesRegex(
                casa_tclean.ProtocolError, "mask identity mismatch"
            ):
                casa_tclean.process_request(
                    request, tclean_task=fake_tclean, casa_version=CASA_VERSION
                )
            self.assertFalse(called)

    def test_relocated_identical_masks_share_cf_plan_and_revalidate_per_call(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            first_mask = root / "first.mask"
            second_mask = root / "second.mask"
            first_mask.write_bytes(b"same deterministic mask")
            second_mask.write_bytes(first_mask.read_bytes())
            request = make_request(root, action="run")
            frozen = {
                "kind": "file",
                "sha256": sha256_file(first_mask),
                "identity": {"size_bytes": first_mask.stat().st_size},
            }
            request["overrides"]["mask"] = str(first_mask)
            request["mask_identity"] = copy.deepcopy(frozen)
            relocated = copy.deepcopy(request)
            relocated["overrides"]["mask"] = str(second_mask)

            first_plan = casa_tclean.build_invocation_plan(request)
            second_plan = casa_tclean.build_invocation_plan(relocated)

            self.assertEqual(
                first_plan["cache"]["plan_sha256"],
                second_plan["cache"]["plan_sha256"],
            )
            self.assertNotEqual(
                first_plan["effective_kwargs_sha256"],
                second_plan["effective_kwargs_sha256"],
            )
            for plan in (first_plan, second_plan):
                self.assertEqual(
                    "matched",
                    casa_tclean._validate_mask_identity(
                        plan["mask_identity"],
                        effective_kwargs=plan["effective_kwargs"],
                    )["status"],
                )

    def test_execution_failure_is_typed_and_preserves_partial_facts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root, action="run")

            def failing_tclean(**kwargs):
                partial = pathlib.Path(kwargs["cfcache"])
                partial.mkdir()
                (partial / "partial.cf").write_bytes(b"incomplete")
                raise RuntimeError("synthetic CASA failure")

            result = casa_tclean.process_request(
                request, tclean_task=failing_tclean, casa_version=CASA_VERSION
            )
            self.assertEqual("failed_execution", result["status"])
            self.assertEqual("tclean", result["failure"]["kind"])
            self.assertIn("synthetic CASA failure", result["failure"]["reason"])
            self.assertEqual([], result["products"]["before"])
            self.assertFalse(pathlib.Path(request["cache"]["path"]).exists())
            self.assertTrue(pathlib.Path(result["cache"]["working_path"]).is_dir())
            self.assertTrue(result["cache"]["after"]["inventory"]["exists"])

    def test_runtime_version_must_match_before_tclean_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = make_request(root, action="run")
            called = False

            def fake_tclean(**_kwargs):
                nonlocal called
                called = True

            with self.assertRaisesRegex(casa_tclean.ProtocolError, "version mismatch"):
                casa_tclean.process_request(
                    request, tclean_task=fake_tclean, casa_version="6.8.0.0"
                )
            self.assertFalse(called)

    def test_disk_io_capture_is_typed_and_fail_closed(self) -> None:
        with (
            mock.patch.object(casa_tclean.platform, "system", return_value="Linux"),
            mock.patch.object(
                pathlib.Path,
                "read_text",
                return_value="read_bytes: 123\nwrite_bytes: 456\n",
            ),
        ):
            self.assertEqual(
                (123, 456, "linux_proc_self_io"), casa_tclean._disk_io_bytes()
            )
        with (
            mock.patch.object(casa_tclean.platform, "system", return_value="Linux"),
            mock.patch.object(
                pathlib.Path, "read_text", return_value="read_bytes: 123\n"
            ),
            self.assertRaisesRegex(casa_tclean.ProtocolError, "disk-I/O bytes"),
        ):
            casa_tclean._disk_io_bytes()
        with (
            mock.patch.object(casa_tclean.platform, "system", return_value="Plan9"),
            self.assertRaisesRegex(casa_tclean.ProtocolError, "unsupported"),
        ):
            casa_tclean._disk_io_bytes()

    def test_plan_entrypoint_never_requires_casa_imports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request_path = root / "request.json"
            result_path = root / "result.json"
            request_path.write_text(json.dumps(make_request(root)))

            completed = subprocess.run(
                [
                    sys.executable,
                    str(pathlib.Path(casa_tclean.__file__)),
                    str(request_path),
                    str(result_path),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            result = json.loads(result_path.read_text())
            self.assertEqual("planned", result["status"])
            self.assertEqual(CASA_VERSION, result["casa"]["expected_version"])

    def test_entrypoint_writes_typed_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request_path = root / "request.json"
            result_path = root / "result.json"
            request = make_request(root)
            request["unknown"] = True
            request_path.write_text(json.dumps(request))

            completed = subprocess.run(
                [
                    sys.executable,
                    str(pathlib.Path(casa_tclean.__file__)),
                    str(request_path),
                    str(result_path),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            result = json.loads(result_path.read_text())
            self.assertEqual("failed_validation", result["status"])
            self.assertEqual("protocol", result["failure"]["kind"])


def runtime_identity_fixture() -> dict:
    modules = {}
    for index, name in enumerate(("casatasks", "casatools", "casaconfig", "casadata")):
        modules[name] = {
            "distribution_version": "6.7.5.9",
            "reported_version": CASA_VERSION if name == "casatasks" else None,
            "module_file": f"/runtime/{name}/__init__.py",
            "module_file_sha256": f"{index + 1:x}" * 64,
            "code_tree": {
                "tree_sha256": f"{index + 5:x}" * 64,
                "file_count": index + 1,
                "size_bytes": index + 10,
                "policy": "package_files_without_bytecode_v1",
            },
        }
    tree = {
        "tree_sha256": "b" * 64,
        "file_count": 1,
        "size_bytes": 10,
        "excluded_names": ["data_update.lock", "table.lock"],
        "excluded_count": 0,
    }
    return {
        "schema_version": 1,
        "python": {
            "version": "3.14.6",
            "implementation": "CPython",
            "executable": "/runtime/python",
            "executable_sha256": "a" * 64,
        },
        "modules": modules,
        "configuration": {
            "measurespath": "/runtime/data",
            "datapath": ["/runtime/data"],
        },
        "data_versions": {"casarundata": None, "measures": None},
        "data_trees": {
            "geodetic": {"path": "/runtime/data/geodetic", **tree},
            "vla": {"path": "/runtime/data/nrao/VLA", **tree},
        },
    }


def recipe_text() -> str:
    return """taskname = 'tclean'
vis = 'archived.ms'
field = '1525'
spw = '2~17'
imagename = 'archived-image'
datacolumn = ''
imsize = [12150, 12150]
cell = '0.6arcsec'
phasecenter = ''
stokes = 'I'
projection = 'SIN'
specmode = 'mfs'
reffreq = ''
nchan = -1
start = ''
width = ''
outframe = 'LSRK'
veltype = 'radio'
restfreq = []
interpolation = 'linear'
gridder = 'awproject'
facets = 1
psfphasecenter = ''
wprojplanes = 32
vptable = ''
aterm = True
psterm = False
wbawp = True
conjbeams = True
usepointing = True
computepastep = 360.0
rotatepastep = 360.0
pblimit = 0.0001
cfcache = '/obsolete/cache'
interactive = True
parallel = True
restart = True
chanchunks = 1
pointingoffsetsigdev = 0.0
#tclean(vis='must-not-execute.ms')
"""


def make_request(
    root: pathlib.Path, *, action: str = "plan", write_recipe: bool = True
) -> dict:
    recipe_path = root / "tclean.last"
    if write_recipe:
        recipe_path.write_text(recipe_text())
    parsed = casa_tclean.parse_literal_assignment_recipe(recipe_path.read_text())
    parameter_names = sorted(name for name in parsed if name != "taskname")
    cache_path = root / "cf-cache"
    overrides = {
        "vis": str(root / "input.ms"),
        "imagename": str(root / "products" / "casa"),
        "datacolumn": "data",
        "interactive": False,
        "parallel": False,
        "restart": False,
        "cfcache": str(cache_path),
    }
    effective, _, _ = casa_tclean.normalize_archived_parameters(
        {name: value for name, value in parsed.items() if name != "taskname"},
        overrides,
    )
    cache_plan = {
        "schema_version": 1,
        "kind": "casa_tclean_cf_plan",
        "casa_version": CASA_VERSION,
        "dataset": {"key": "fixture", "path": str(root / "input.ms")},
        "recipe_sha256": sha256_file(recipe_path),
        "cf_parameters": casa_tclean.cf_cache_parameter_identity(effective),
    }
    return {
        "schema_version": casa_tclean.REQUEST_SCHEMA_VERSION,
        "kind": casa_tclean.REQUEST_KIND,
        "request_id": "test-vlass",
        "action": action,
        "expected_casa_version": CASA_VERSION,
        "mask_identity": None,
        "recipe": {
            "path": str(recipe_path),
            "sha256": sha256_file(recipe_path),
            "task": "tclean",
            "parameter_names": parameter_names,
        },
        "overrides": overrides,
        "cache": {
            "role": "cold",
            "path": str(cache_path),
            "plan": cache_plan,
            "plan_sha256": casa_tclean.canonical_sha256(cache_plan),
            "receipt_path": str(root / "cf-cache-receipt.json"),
        },
    }


def sha256_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
