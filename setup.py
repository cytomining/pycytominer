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

with open("requirements-dev.txt") as f:
    DEV_PKGS = f.read().splitlines()

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
    extras_require={
        "collate": ["cytominer-database==0.3.4"],
        "cell_locations": [
            "fsspec>=2023.1.0",
            "s3fs>=0.4.2",
            "boto3>=1.26.79",
            "fire>=0.5.0",
        ],
        "dev": DEV_PKGS,
    },
    python_requires=">=3.4",
    include_package_data=True,
)
