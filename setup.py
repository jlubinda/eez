from distutils.core import setup
from setuptools import find_packages
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eez",
    version="1",
    author="Joseph S. Lubinda",
    author_email="joseph@obtir.com",
    description="An awesome tool for rapidly creating performant GraphQL-like APIs that use SQL-like JSON formatted queries.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jlubinda/jsql",
    project_urls={
        "Bug Tracker": "https://github.com/jlubinda/jsql/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPL 3.0 License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)