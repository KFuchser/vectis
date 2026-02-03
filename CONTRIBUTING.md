# Contributing to the Vectis Data Pipeline

First off, thank you for considering contributing! Your help is appreciated.

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please open an issue and provide the following information:
- A clear and descriptive title.
- A detailed description of the problem.
- Steps to reproduce the bug.
- Any relevant logs or screenshots.

### Suggesting Enhancements

If you have an idea for an enhancement, please open an issue and provide the following information:
- A clear and descriptive title.
- A detailed description of the enhancement.
- The rationale for the enhancement.

### Pull Requests

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes.
4.  Follow the [style guidelines](#style-guidelines).
5.  Commit your changes (`git commit -m 'Add some feature'`).
6.  Push to the branch (`git push origin feature/your-feature-name`).
7.  Open a pull request.

## Style Guidelines

-   **Python:** Follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide.
-   **Docstrings:** Use [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#3.8-comments-and-docstrings).
-   **Comments:** Use comments to explain complex logic and "magic numbers."

## Adding a New City

To add a new city to the pipeline, you will need to:

1.  Create a new `ingest_<city_name>.py` script.
2.  This script must have a function `get_<city_name>_data(lookback_date)` that returns a list of `PermitRecord` objects.
3.  Add the new script to `ingest_velocity_50.py` and call the new function in the `main` function.
4.  Update the `Verified Cities` list in `ARCHITECTURE.md`.
