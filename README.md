# Template Project

A simple Python project template, it's really only made to save me time when starting a new project, and remind me of my own current best practice. Rather than as an exemplar of actual best practice.

## Dependencies

- [uv](https://docs.astral.sh/uv/getting-started/)

## Setup


### Install in development mode

```bash
uv sync
```

## Project Structure

```
TemplateProject/
├── src/                  # Source code
├── tests/                # Unit tests
├── requirements.txt      # Project dependencies
├── pyproject.toml        # Project metadata and build configuration
└── README.md             # This file
```

## Usage

```python
from template_project import main

main.run()
```

## Development

### Running tests

```bash
pytest
```


## License

MIT

