import asyncio
import typer
from pathlib import Path
from .collector import DataCollector
from .client import CensusClient
from .viz.maps import CensusMapper
import rich
from typing import Optional
from .constants import STATE_CENTERS, DEFAULT_CENTER, DEMOGRAPHIC_VARS

app = typer.Typer()


@app.command()
def map(
    state: str = typer.Argument(
        ..., help="State FIPS code (e.g., '06' for California)"
    ),
    variable: str = typer.Option(
        "total", "--var", help="Demographic variable to map", case_sensitive=False
    ),
    output: Path = typer.Option(
        Path("./maps"),
        "--output",
        "-o",
        help="Output directory for maps",
        dir_okay=True,
        file_okay=False,
    ),
    data_dir: Path = typer.Option(
        Path("./data/pop_bg"),
        help="Directory containing Delta tables",
        dir_okay=True,
        file_okay=False,
    ),
    center_lat: Optional[float] = typer.Option(
        None,
        "--lat",
        help="Center latitude (defaults to state center)",
    ),
    center_lon: Optional[float] = typer.Option(
        None,
        "--lon",
        help="Center longitude (defaults to state center)",
    ),
    zoom: int = typer.Option(
        7,
        "--zoom",
        "-z",
        help="Initial zoom level",
    ),
) -> None:
    """Generate a choropleth map for census data"""

    if variable not in DEMOGRAPHIC_VARS:
        valid_vars = ", ".join(DEMOGRAPHIC_VARS.keys())
        rich.print(f"[red]Error: Invalid variable. Choose from: {valid_vars}[/red]")
        raise typer.Exit(1)

    # Ensure output directory exists
    output.mkdir(parents=True, exist_ok=True)

    center = (
        center_lat or STATE_CENTERS.get(state, DEFAULT_CENTER)[0],
        center_lon or STATE_CENTERS.get(state, DEFAULT_CENTER)[1],
    )

    # Create map
    mapper = CensusMapper(
        demo_table_path=data_dir / "demographics",
        geo_table_path=data_dir / "geometries",
    )

    var_name = DEMOGRAPHIC_VARS[variable]
    m = mapper.create_map(
        value_column=var_name,
        title=f"{var_name.replace('_', ' ').title()} by Block Group",
        center=center,
        zoom_start=zoom,
    )

    # Save map
    output_file = output / f"{state}_{variable}.html"
    mapper.save_map(m, output_file)
    rich.print(f"[green]Map saved to: {output_file}[/green]")


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
        collector = DataCollector(table_path)
        try:
            await collector.update_state(state, year)
        finally:
            await collector.close()

    asyncio.run(run())


@app.command()
def lookup_state(
    name: str = typer.Argument(..., help="Full or partial state name"),
) -> None:
    """Look up a state's FIPS code by name"""

    async def run():
        client = CensusClient()
        try:
            states = await client.get_states()
            # Case-insensitive partial matching
            matches = {
                state: code
                for state, code in states.items()
                if name.lower() in state.lower()
            }

            if not matches:
                rich.print("[red]No matching states found[/red]")
                return

            # Print matches in a table
            table = rich.table.Table("State", "FIPS Code")
            for state, code in sorted(matches.items()):
                table.add_row(state, code)

            rich.print(table)

        finally:
            await client.close()

    asyncio.run(run())


if __name__ == "__main__":
    app()
