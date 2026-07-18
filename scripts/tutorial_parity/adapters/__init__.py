"""Surface adapter registry."""

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
