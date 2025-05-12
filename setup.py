"""Setup python package."""

from setuptools import setup, find_packages

setup(
    name='ml',
    version='0.1',
    packages=find_packages(where='.'),
    package_dir={'': '.'},
    exclude=[
        "data",
        "notebooks",
        "scripts",
        "models",
        "data/*",
        "notebooks/*",
        "models/*",
        "scripts/*",
        "archive",
        "archive/*",
    ],
)