import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='databathing',
    version='0.9.1',
    description="Advanced SQL-to-code generator with intelligent auto-selection, multi-engine support, validation, and performance optimization",
    author="Jiazhen Zhu, Sanhe Hu",
    author_email="jason.jz.zhu@gmail.com, husanhe@email.com",
    maintainer="Jiazhen Zhu",
    maintainer_email="jason.jz.zhu@gmail.com",
    url="https://github.com/jason-jz-zhu/databathing",
    project_urls={
        "Homepage": "https://github.com/jason-jz-zhu/databathing",
        "Bug Tracker": "https://github.com/jason-jz-zhu/databathing/issues",
        "Source Code": "https://github.com/jason-jz-zhu/databathing",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        'License :: OSI Approved :: MIT License'],
    license="MIT",
    packages=setuptools.find_packages(),
    install_requires=[
          'mo-sql-parsing',
      ],
    extras_require={
        'dev': ['pytest', 'pytest-cov'],
        'test': ['pytest', 'pytest-cov'],
        'validation': ['sqlparse>=0.4.0'],
        'duckdb': ['duckdb'],
        'all': ['sqlparse>=0.4.0', 'duckdb'],
    },
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['sql', 'spark', 'pyspark', 'duckdb', 'etl', 'data', 'parser', 'converter', 'validation', 'quality', 'auto-selection', 'intelligent', 'optimization'],
    python_requires=">=3.7",
)
