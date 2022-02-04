import pathlib
import os
from setuptools import setup
from setuptools import find_packages

with open("README.md", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

# The directory containing this file
my_current_dir = pathlib.Path(__file__).parent

# Pull requirements from the text file
requirement_path = os.path.join(my_current_dir, "requirements.txt")

install_requires = []
if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        install_requires = f.read().splitlines()

testing_requires = []
install_requires = install_requires + testing_requires

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
    author=about["__author__"],
    author_email="gregory.way@gmail.com",
    url="https://github.com/cytomining/pycytominer",
    packages=find_packages(),
    license=about["__license__"],
    install_requires=install_requires,
    python_requires=">=3.4",
    include_package_data=True,
)
