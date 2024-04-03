"""CLI for cell location calculations."""

from pycytominer.cyto_utils.cell_locations import CellLocation
import fire

if __name__ == "__main__":
    fire.Fire(CellLocation)
