from setuptools import setup, find_packages

setup(
    name="pyflowetl",
    version="1.0.0",
    packages=find_packages(include=["pyflowetl", "pyflowetl.*"]),
    include_package_data=True,
    install_requires=[],
)
