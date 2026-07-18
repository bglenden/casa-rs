"""Surface adapter registry."""

# `unittest discover -s scripts/tutorial_parity` imports child packages as
# top-level discovery nodes. Keep that required verification mode inert while
# preserving normal imports through `tutorial_parity.adapters`.
if __package__ == "adapters":
    __all__: list[str] = []
else:
    from .base import AdapterPlan, SurfaceAdapter
    from .casa import CasaAdapter
    from .cli import CliAdapter
    from .gui import GuiAdapter
    from .python import PythonAdapter
    from .tui import TuiAdapter

    def adapters() -> dict[str, SurfaceAdapter]:
        return {
            "casa": CasaAdapter(),
            "cli": CliAdapter(),
            "python": PythonAdapter(),
            "tui": TuiAdapter(),
            "gui": GuiAdapter(),
        }

    __all__ = ["AdapterPlan", "SurfaceAdapter", "adapters"]
