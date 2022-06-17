import pathlib
from setuptools import setup
from setuptools import find_packages

with open("README.md", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

about = {}
with open(pathlib.Path("pycytominer/__about__.py")) as fp:
    exec(fp.read(), about)
print(about)
setup(
    name="pycytominer",
    version=about["__version__"],
    description="Processing perturbation profiling readouts.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cytomining/pycytominer",
    packages=find_packages(),
    license=about["__license__"],
    install_requires=["numpy", "pandas", "scikit-learn", "sqlalchemy"],
    extras_require={"collate": ["cytominer-database==0.3.4"]},
    python_requires=">=3.4",
    include_package_data=True,
)
