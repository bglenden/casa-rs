"""The benchmark command must reach the normal application dataset boundary."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[3]


@unittest.skipUnless((REPO / "target/release/casars-imager").is_file(),
                     "build the release casars-imager binary first")
class SpectralBenchmarkCliTests(unittest.TestCase):
    def test_t53_cube_families_reach_measurement_set_open(self):
        for gridder in ("standard", "wproject", "mosaic"):
            with self.subTest(gridder=gridder), tempfile.TemporaryDirectory() as root:
                environment = {key: value for key, value in os.environ.items()
                               if not key.startswith(("IMAGER_BENCH_", "BENCH_"))}
                environment.update({
                    "IMAGER_BENCH_VALIDATE_RUST_CLI_ONLY": "1",
                    "IMAGER_BENCH_SKIP_RUST": "1",
                    "IMAGER_BENCH_TMP_ROOT": root,
                    "IMAGER_BENCH_GRIDDER": gridder,
                    "IMAGER_BENCH_SPECMODE": "cube",
                    "IMAGER_BENCH_CHANNEL_COUNT": "16",
                    "IMAGER_BENCH_CUBE_START": "0",
                    "IMAGER_BENCH_CUBE_WIDTH": "1",
                    "IMAGER_BENCH_STANDARD_MFS_ACCELERATION": "cpu",
                    "IMAGER_BENCH_IMAGING_FFT_BACKEND": "rustfft",
                })
                result = subprocess.run(
                    [str(REPO / "scripts/bench-imager-vs-casa.sh"),
                     str(Path(root) / "missing.ms")],
                    env=environment, text=True, capture_output=True, timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("rust_cli_preflight=validated-before-measurement-set-open",
                              result.stdout)


if __name__ == "__main__":
    unittest.main()
