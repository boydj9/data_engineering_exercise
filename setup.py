#template from : https://github.com/pypa/sampleproject/blob/main/setup.py

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")


setup(
    name="jacobdataeexercise",
    version="1.0.0",
    description="A sample feed", 
    url="https://github.com/boydj9/data_engineering_exercise",
    author="Jacob Boyd",
    packages=find_packages(where="src"),
    python_requires=">=3.7, <4",
    install_requires=["postgres"], 
)