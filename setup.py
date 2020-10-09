import pathlib
from setuptools import setup
from setuptools import find_packages

with open('README.md', encoding="utf-8") as readme_file:
    long_description = readme_file.read()

about = {}
with open(pathlib.Path("pycytominer/__about__.py")) as fp:
    exec(fp.read(), about)

setup(
    name="pycytominer",
    version=about["version"],
    description="Processing perturbation profiling readouts.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=about["author"],
    author_email="gregory.way@gmail.com",
    url="https://github.com/cytomining/pycytominer",
    packages=find_packages(),
    license=about["license"],
    install_requires=["numpy", "pandas", "scikit-learn", "sqlalchemy"],
    python_requires=">=3.4",
    include_package_data=True,
)
