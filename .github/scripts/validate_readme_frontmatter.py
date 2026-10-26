"""Validate the frontmatter of every connector README.md.

The Fivetran docs community connectors catalog is built from this frontmatter, so every
connector directory (a directory containing connector.py) must have a README.md starting with
the block below. A README.md in any other directory, such as a parent directory grouping several
implementations, is listed in the catalog when it has this block, and is validated the same way.

---
name: Apache HBase
description: Connect and sync data from Apache HBase using happybase and thrift libraries
category: Databases
---

Run from the repository root: python .github/scripts/validate_readme_frontmatter.py
"""

import pathlib
import re
import sys

import yaml

CATEGORIES = {
    "Databases",
    "Cloud Data Warehouses",
    "Message Queues & Streaming",
    "AI and Connector SDK",
    "SaaS & APIs",
}
REQUIRED_KEYS = ("name", "description", "category")

# Connector directories intentionally not listed in the community connectors catalog.
# The odata_api implementations are listed through the odata_api/README.md parent entry.
EXCLUDED_DIRS = {
    "_template_connector",
    "empty-project",
    "odata_api/odata_version_2_using_pyodata",
    "odata_api/odata_version_4",
    "odata_api/odata_version_4_using_python_odata",
}

# Parent directories without connector.py that are listed in the catalog. Their README.md is
# required even without frontmatter, so removing it cannot drop the entry unnoticed.
PARENT_ENTRY_DIRS = {
    "odata_api",
}

FRONTMATTER_RE = re.compile(r"\A---\r?\n(.*?)\r?\n---\r?\n", re.DOTALL)


def validate_readme(readme: pathlib.Path) -> list:
    if not readme.is_file():
        return ["missing README.md"]

    match = FRONTMATTER_RE.match(readme.read_text(encoding="utf-8"))
    if not match:
        return ["README.md must start with a frontmatter block delimited by '---' lines"]

    try:
        frontmatter = yaml.safe_load(match.group(1))
    except yaml.YAMLError as error:
        # GitHub annotations must fit on a single line.
        return [f"frontmatter is not valid YAML: {' '.join(str(error).split())}"]

    if not isinstance(frontmatter, dict):
        return ["frontmatter must be a YAML mapping"]

    errors = []
    for key in REQUIRED_KEYS:
        value = frontmatter.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"'{key}' must be a non-empty string")

    unknown_keys = sorted(set(frontmatter) - set(REQUIRED_KEYS))
    if unknown_keys:
        errors.append(f"unknown keys: {', '.join(unknown_keys)}")

    category = frontmatter.get("category")
    if isinstance(category, str) and category.strip() and category not in CATEGORIES:
        errors.append(f"'category' must be one of: {', '.join(sorted(CATEGORIES))}")

    return errors


def main() -> int:
    root = pathlib.Path(".")
    connector_readmes = {
        path.parent / "README.md"
        for path in root.rglob("connector.py")
        if ".git" not in path.parts and path.parent.as_posix() not in EXCLUDED_DIRS
    }
    parent_readmes = {root / parent_dir / "README.md" for parent_dir in PARENT_ENTRY_DIRS}
    readmes_with_frontmatter = {
        path
        for path in root.rglob("README.md")
        if ".git" not in path.parts and path.read_text(encoding="utf-8").startswith("---")
    }
    readmes = sorted(connector_readmes | parent_readmes | readmes_with_frontmatter)

    failed = False
    for readme in readmes:
        for error in validate_readme(readme):
            failed = True
            print(f"::error file={readme.as_posix()}::{readme.as_posix()}: {error}")

    if failed:
        print(f"See {pathlib.Path(__file__).as_posix()} for the expected format.")
        return 1

    print(f"Frontmatter is valid in {len(readmes)} connector READMEs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
