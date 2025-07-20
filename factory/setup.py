# -*- coding: utf-8 -*-
"""Setup script for the factory package."""
from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text() if (this_directory / "README.md").exists() else ""


# Read requirements from requirements.txt
def get_requirements():
    """Get requirements from requirements.txt"""
    requirements_path = this_directory / "requirements.txt"
    if requirements_path.exists():
        with open(requirements_path, 'r', encoding='utf-8') as f:
            requirements = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '==' in line:
                        requirements.append(line.replace('==', '>='))
                    else:
                        requirements.append(line)
            return requirements
    return []


setup(
    name="factory",
    version="0.1.1",
    author="Diogo Leme Jacomini",
    author_email="diogojacomini@outlook.com",
    description="Intelligent Scoring and Market Behavior (ISMB) Factory Pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/diogojacomini/ismb-app",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Office/Business :: Financial",
    ],
    python_requires=">=3.9",
    install_requires=get_requirements(),
    extras_require={
        "dev": [
            "pytest-cov~=3.0",
            "pytest-mock>=1.7.1, <2.0",
            "pytest~=7.2",
            "ruff~=0.1.8",
        ],
        "docs": [
            "docutils<0.21",
            "sphinx>=5.3,<7.3",
            "sphinx_rtd_theme==2.0.0",
            "nbsphinx==0.8.1",
            "sphinx-autodoc-typehints==1.20.2",
            "sphinx_copybutton==0.5.2",
            "ipykernel>=5.3, <7.0",
            "Jinja2<3.2.0",
            "myst-parser>=1.0,<2.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "factory=factory.__main__:main",
        ],
        "kedro.hooks": [
            # Kedro hooks
        ],
    },
    include_package_data=True,
    package_data={
        "factory": [
            "conf/**/*",
            "data/**/*",
            "docs/**/*",
        ],
    },
    zip_safe=False,
    keywords=[
        "kedro",
        "pipeline",
        "data-engineering",
        "machine-learning",
        "finance",
        "market-analysis",
        "airflow",
        "etl",
    ],
    project_urls={
        "Bug Reports": "https://github.com/diogojacomini/ismb-app/issues",
        "Source": "https://github.com/diogojacomini/ismb-app",
        "Documentation": "https://github.com/diogojacomini/ismb-app/wiki",
    },
)
