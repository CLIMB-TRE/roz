import os
from setuptools import setup

version_py = os.path.join(os.path.dirname(__file__), "version.py")
version = open(version_py).read().strip().split(
    '=')[-1].replace('"', '').strip()
long_description = """
``roz`` is a set of microservices used to ingest data and pass messages around in climb-mpx
"""

HERE = os.path.dirname(__file__)

with open(os.path.join(HERE, "requirements.txt"), "r") as f:
    install_requires = [x.strip() for x in f.readlines()]

setup(
    name="roz",
    version=version,
    install_requires=install_requires,
    requires=["python (>=3.8)"],
    packages=["roz", "snoop_db", "varys"],
    author="Sam AJ Wilkinson",
    description="A file ingest and message queue solution",
    long_description=long_description,
    package_dir={"roz": "roz", "snoop_db": "snoop_db", "varys": "roz"},
    package_data={"artic": [], "snoop_db": []},
    zip_safe=False,
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "roz=roz.client:main",
            "snoop_db=snoop_db.client:main",
        ],
    },
    author_email="s.a.j.wilkinson@bham.ac.uk",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT",
        "Topic :: Scientific/Engineering :: Bio-Informatics"
    ]
)