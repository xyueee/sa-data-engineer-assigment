from setuptools import find_packages, setup

setup(
    name="data_engineer_assignment",
    packages=find_packages(exclude=["data_engineer_assignment_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "pandas",
        "duckcli"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
