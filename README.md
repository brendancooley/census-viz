# Census Viz

A Python tool for collecting and visualizing US Census data at the block group level.

## Configuration

Create a `.env` file in the project root with your Census API key:

```bash
CENSUS_API_KEY=your-api-key-here
```

## CLI Usage

The package provides a command-line interface with the following commands:

### Looking up State FIPS Codes

```bash
# Look up a state by name (case-insensitive, partial matches supported)
census-collect lookup-state mary # Finds Maryland
census-collect lookup-state new # Finds New Hampshire, New Jersey, New Mexico, New York
```

### Collecting Census Data

```bash
# collect data for a state using its FIPS code
census-collect collect 06 # Collect California data (FIPS code 06)
# specify a different year (defaults to 2021)
census-collect collect 24 --year 2019  # Collect Maryland data for 2019
# store data in a custom location
census-collect collect 06 --output-dir /path/to/data/census
```
