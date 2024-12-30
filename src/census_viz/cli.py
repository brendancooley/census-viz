import asyncio
import typer
from pathlib import Path
from .collector import CensusCollector

app = typer.Typer()


@app.command()
def collect(
    state: str = typer.Argument(
        ..., help="State FIPS code (e.g., '06' for California)"
    ),
    year: int = typer.Option(2021, help="Year of ACS data"),
    table_path: Path = typer.Option(
        Path("./data/pop_bg"),
        help="Path to Delta table",
        dir_okay=True,
        file_okay=False,
    ),
) -> None:
    """Collect Census data for a state and store it in a Delta table"""
    table_path.parent.mkdir(parents=True, exist_ok=True)

    async def run():
        collector = CensusCollector(table_path)
        try:
            await collector.update_state(state, year)
        finally:
            await collector.close()

    asyncio.run(run())


if __name__ == "__main__":
    app()
