import pathlib

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as readme_file:
    LONG_DESCRIPTION = readme_file.read()

ABOUT = {}
with open(pathlib.Path("pycytominer/__about__.py")) as fp:
    exec(fp.read(), ABOUT)

# pull requirements for install_requires
with open("requirements.txt") as f:
    REQUIRED_PKGS = f.read().splitlines()

setup(
    name="pycytominer",
    version=ABOUT["__version__"],
    description="Processing perturbation profiling readouts.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/cytomining/pycytominer",
    packages=find_packages(),
    license=ABOUT["__license__"],
    install_requires=REQUIRED_PKGS,
    extras_require={"collate": ["cytominer-database==0.3.4"]},
    python_requires=">=3.4",
    include_package_data=True,
)
