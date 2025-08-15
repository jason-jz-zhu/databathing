import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='databathing',
    version='0.4.0',
    description="Convert SQL queries to PySpark DataFrame operations",
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
    packages=['databathing'],
    install_requires=[
          'mo-sql-parsing',
      ],
    extras_require={
        'dev': ['pytest', 'pytest-cov'],
        'test': ['pytest', 'pytest-cov'],
    },
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['sql', 'spark', 'pyspark', 'etl', 'data', 'parser', 'converter'],
    python_requires=">=3.7",
)
